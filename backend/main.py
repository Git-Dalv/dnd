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
from dataclasses import dataclass, field

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

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


@dataclass
class Room:
    room_id: str
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
    return rooms.setdefault(room_id, Room(room_id=room_id))


@app.websocket("/ws/{room_id}/{member_id}")
async def ws_endpoint(ws: WebSocket, room_id: str, member_id: str):
    await ws.accept()
    role = ws.query_params.get("role", "player")
    name = ws.query_params.get("name", member_id)
    room = get_room(room_id)
    room.members[member_id] = Member(member_id, name, role, ws)

    await room.broadcast({"type": "member.connection", "memberId": member_id, "connected": True})
    # новому участнику — хвост журнала, чтобы догнал состояние
    await ws.send_text(json.dumps({"type": "log.snapshot", "log": room.log[-50:]}, ensure_ascii=False))

    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            await handle(room, member_id, role, msg)
    except WebSocketDisconnect:
        room.members.pop(member_id, None)
        await room.broadcast({"type": "member.connection", "memberId": member_id, "connected": False})


async def handle(room: Room, member_id: str, role: str, msg: dict):
    mtype = msg.get("type")

    # --- бросок кубика: результат генерит сервер ---
    if mtype == "dice.roll":
        try:
            rolls, total = roll_dice(msg.get("notation", ""), int(msg.get("modifier", 0)))
        except ValueError:
            return
        event = {
            "type": "dice.rolled",
            "by": member_id,
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

    # TODO: token.move (проверять ownerId), scene.switch (gm only),
    #       combat.start/next (gm only), audio.play (gm only), fog.reveal (gm only)


@app.get("/health")
def health():
    return {"ok": True, "rooms": len(rooms)}
