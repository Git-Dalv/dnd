"""
VTT backend — минимальный каркас.
Принцип: сервер — источник истины. Клиент шлёт намерения, сервер рассылает факты.
Броски считает сервер. Приватные сообщения фильтруются ДО отправки.

Это старт под «вертикальный срез»: dice.roll -> сервер кидает -> dice.rolled всем.
Дальше навешиваются token.move, scene.switch, combat.*, whisper.* по тому же паттерну.
"""
import json
import random
import re
import secrets
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

try:
    from . import db                 # запуск как пакет (uvicorn backend.main:app)
    from .catalog import Catalog, resolve_character
except ImportError:                  # noqa: запуск из каталога backend/ (uvicorn main:app)
    import db
    from catalog import Catalog, resolve_character

# Справочник контента в памяти. Загружается при старте; используется, чтобы
# собрать действия персонажа { ref, overrides } перед отправкой на стол.
content_catalog = Catalog()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # БД поднимаем при старте — это «полка», с которой комнаты встают после рестарта.
    db.init_db()
    content_catalog.load()
    yield


app = FastAPI(lifespan=lifespan)

# Прототип фронта лежит в docs/. Отдаём его как статику.
DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"

DICE_RE = re.compile(r"^\s*(\d+)\s*d\s*(\d+)\s*$", re.I)


def roll_dice(notation: str, modifier: int = 0):
    """Сервер генерит результат. notation вида '2d6'. Возвращает (список_бросков, сумма)."""
    m = DICE_RE.match(notation)
    if not m:
        raise ValueError(f"bad notation: {notation}")
    count, sides = int(m.group(1)), int(m.group(2))
    if not (1 <= count <= 100 and 1 <= sides <= 1000):
        raise ValueError("out of range")
    rolls = [random.randint(1, sides) for _ in range(count)]
    return rolls, sum(rolls) + modifier


@dataclass
class Member:
    member_id: str
    name: str
    role: str  # "gm" | "player"
    ws: WebSocket
    char_id: str | None = None
    char_name: str | None = None   # имя персонажа для журнала (а не member_id)


@dataclass
class Room:
    room_id: str
    name: str = ""
    gm_id: str | None = None
    members: dict[str, Member] = field(default_factory=dict)
    log: list = field(default_factory=list)

    async def broadcast(self, message: dict):
        """Всем в комнате."""
        dead = []
        for m in self.members.values():
            try:
                await m.ws.send_text(json.dumps(message, ensure_ascii=False))
            except Exception:
                dead.append(m.member_id)
        for mid in dead:
            self.members.pop(mid, None)

    async def send_to(self, member_ids: list[str], message: dict):
        """Приватно — только перечисленным. Фильтрация ДО отправки."""
        for mid in member_ids:
            m = self.members.get(mid)
            if m:
                await m.ws.send_text(json.dumps(message, ensure_ascii=False))

    def gm_ids(self):
        return [m.member_id for m in self.members.values() if m.role == "gm"]


rooms: dict[str, Room] = {}


def get_room(room_id: str) -> Room:
    room = rooms.get(room_id)
    if room is None:
        room = Room(room_id=room_id)
        # если игра уже есть в БД — поднимаем её метаданные (места берём из БД по запросу)
        for g in db.list_games():
            if g["roomId"] == room_id:
                room.name = g["name"]
                room.gm_id = g["gmId"]
                break
        rooms[room_id] = room
    return room


@app.websocket("/ws/{room_id}/{member_id}")
async def ws_endpoint(ws: WebSocket, room_id: str, member_id: str):
    await ws.accept()
    role = ws.query_params.get("role", "player")
    name = ws.query_params.get("name", member_id)
    room = get_room(room_id)
    member = Member(member_id, name, role, ws)
    room.members[member_id] = member

    # персонаж участника: char_id из query (его передаёт экран мест при входе),
    # иначе — из занятого места в БД. Действия { ref, overrides } собираем
    # каталогом, чтобы стол получил готовые rolls с modifier.
    char_id = ws.query_params.get("char")
    if not char_id:
        for s in db.list_seats(room_id):
            if s.get("memberId") == member_id:
                char_id = s.get("charId")
                break
    character = None
    if char_id:
        raw = db.get_character(char_id)
        if raw:
            character = resolve_character(content_catalog, raw)
    member.char_id = char_id
    member.char_name = character.get("name") if character else None

    await room.broadcast({"type": "member.connection", "memberId": member_id, "connected": True})
    # новому участнику — хвост журнала, чтобы догнал состояние
    await ws.send_text(json.dumps({"type": "log.snapshot", "log": room.log[-50:]}, ensure_ascii=False))
    # и текущий состав мест из БД (если игра существует)
    await ws.send_text(json.dumps({"type": "seat.updated", "seats": db.list_seats(room_id)}, ensure_ascii=False))
    # и его собственный персонаж (личное сообщение, по образцу log/catalog.snapshot)
    if character is not None:
        await ws.send_text(json.dumps({"type": "character.snapshot", "character": character}, ensure_ascii=False))

    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            await handle(room, member_id, role, msg)
    except WebSocketDisconnect:
        # если этот member_id уже ПЕРЕПОДКЛЮЧИЛСЯ (новый ws заменил наш в members),
        # старый дисконнект не должен трогать чужое подключение/его место.
        if room.members.get(member_id) is member:
            room.members.pop(member_id, None)
            db.free_seat(room_id, member_id)   # освобождаем место (событие уровня сессии)
            await room.broadcast({"type": "member.connection", "memberId": member_id, "connected": False})
            await room.broadcast({"type": "seat.updated", "seats": db.list_seats(room_id)})


async def handle(room: Room, member_id: str, role: str, msg: dict):
    mtype = msg.get("type")

    # --- бросок кубика: результат генерит сервер ---
    if mtype == "dice.roll":
        try:
            rolls, total = roll_dice(msg.get("notation", ""), int(msg.get("modifier", 0)))
        except ValueError:
            return
        member = room.members.get(member_id)
        by = (member.char_name or member.name) if member else member_id  # имя персонажа в журнале
        event = {
            "type": "dice.rolled",
            "by": by,
            "label": msg.get("label", ""),
            "rolled": rolls,
            "total": total,
        }
        room.log.append(event)
        await room.broadcast(event)
        return

    # --- приватный запрос игрока к мастеру ---
    if mtype == "request.toGm":
        await room.send_to(room.gm_ids(), {
            "type": "request.received",
            "from": member_id,
            "kind": msg.get("kind"),
            "label": msg.get("label"),
        })
        return

    # --- мастер открывает приватный канал на выбранную аудиторию ---
    if mtype == "whisper.open":
        if role != "gm":
            return  # проверка прав на сервере
        audience = msg.get("audience", [])
        # инициатор/мастер тоже должны видеть — добавляем GM
        recipients = list(set(audience + room.gm_ids()))
        await room.send_to(recipients, {
            "type": "whisper.opened",
            "from": member_id,
            "audience": audience,
            "label": msg.get("label", ""),
        })
        return

    # --- сохранить раскладку бара: пишем в блоб персонажа (событие сессии) ---
    if mtype == "hotbar.set":
        char_id = msg.get("charId")
        member = room.members.get(member_id)
        # проверка прав на сервере: менять можно ТОЛЬКО раскладку своего персонажа
        if not member or not char_id or member.char_id != char_id:
            return
        hotbar = msg.get("hotbar")
        if not isinstance(hotbar, list):
            return
        db.update_character_hotbar(char_id, hotbar)
        # подтверждение лично инициатору; другим не транслируем (раскладка приватна)
        await room.send_to([member_id], {"type": "hotbar.saved", "charId": char_id})
        return

    # --- занять место: источник истины — БД (лимит 4+1, гонку решает атомарный UPDATE) ---
    if mtype == "seat.take":
        seat_no = msg.get("seatNo")
        char_id = msg.get("charId")
        if not isinstance(seat_no, int):
            await room.send_to([member_id], {"type": "seat.denied", "reason": "bad_seat"})
            return
        # роль и номер места должны соответствовать (проверка на сервере):
        # gm — только seat 0; player — только 1..MAX_PLAYER_SEATS
        if role == "gm" and seat_no != 0:
            await room.send_to([member_id], {"type": "seat.denied", "reason": "gm_seat_only"})
            return
        if role != "gm" and seat_no == 0:
            await room.send_to([member_id], {"type": "seat.denied", "reason": "gm_only"})
            return
        if role != "gm" and not (1 <= seat_no <= db.MAX_PLAYER_SEATS):
            await room.send_to([member_id], {"type": "seat.denied", "reason": "bad_seat"})
            return
        if db.take_seat(room.room_id, seat_no, member_id, char_id):
            await room.broadcast({"type": "seat.updated", "seats": db.list_seats(room.room_id)})
        else:
            await room.send_to([member_id], {"type": "seat.denied", "reason": "taken"})
        return

    # TODO: token.move (проверять ownerId), scene.switch (gm only),
    #       combat.start/next (gm only), audio.play (gm only), fog.reveal (gm only)


@app.get("/health")
def health():
    return {"ok": True, "rooms": len(rooms)}


# ---- Лобби (HTTP) — экраны ДО входа в WS-комнату, удобнее обычным fetch ----

@app.get("/api/games")
def api_list_games():
    return db.list_games()


@app.post("/api/games")
def api_create_game(payload: dict):
    name = payload.get("name") or "Новая игра"
    gm_id = payload.get("gmId")
    room_id = "room_" + secrets.token_hex(4)   # код комнаты = код входа по ссылке
    return db.create_game(room_id, name, gm_id)


@app.get("/api/games/{room_id}/characters")
def api_list_characters(room_id: str):
    return db.list_characters(room_id)


@app.post("/api/games/{room_id}/characters")
def api_create_character(room_id: str, payload: dict):
    # data — объект по character-schema.json; и форма, и импорт JSON дают один и тот же data
    data = payload.get("data") or {}
    owner_id = payload.get("ownerId")
    char_id = "char_" + secrets.token_hex(4)
    data.setdefault("id", char_id)
    db.create_character(char_id, room_id, data, owner_id)
    return {"charId": char_id, "character": data}


@app.get("/api/games/{room_id}/seats")
def api_list_seats(room_id: str):
    return db.list_seats(room_id)


# Корень отдаёт прототип. Регистрируем ПОСЛЕ /ws и /health, чтобы их не перехватить.
@app.get("/")
def index():
    return FileResponse(DOCS_DIR / "prototype.html")


# Остальные файлы docs/ (схемы, эскизы) — статикой. Mount на "/" ловит всё,
# что не совпало с маршрутами выше (включая /ws — он остаётся нетронутым).
app.mount("/", StaticFiles(directory=DOCS_DIR), name="static")
