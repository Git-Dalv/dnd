"""
VTT backend — минимальный каркас.
Принцип: сервер — источник истины. Клиент шлёт намерения, сервер рассылает факты.
Броски считает сервер. Приватные сообщения фильтруются ДО отправки.

Это старт под «вертикальный срез»: dice.roll -> сервер кидает -> dice.rolled всем.
Дальше навешиваются token.move, scene.switch, combat.*, whisper.* по тому же паттерну.
"""
import copy
import json
import math
import random
import re
import secrets
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse, Response
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

# Ассеты мира (карты/токены/портреты) — файлы на диске, раздаются static-mount'ом.
# Каталог создаём заранее: StaticFiles при монтировании требует существующую папку.
ASSETS_DIR = Path(__file__).resolve().parent.parent / "data" / "assets"
ASSETS_DIR.mkdir(parents=True, exist_ok=True)

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


def _round_half_up(x: float) -> int:
    """Округление .5 ВВЕРХ (как JS Math.round) — чтобы серверный расчёт и
    клиентское превью пересчёта совпадали до единицы. Python round() банкирский,
    он бы расходился с фронтом."""
    return math.floor(x + 0.5)


# Ведущее "NdM" в нотации урона ("2d6", "1d8"…) — для умножения числа кубиков.
_DMG_DICE_RE = re.compile(r"^(\d+)d(\d+)", re.I)


def scale_statblock(statblock: dict, level: int) -> dict:
    """Масштабирует статблок существа под «уровень» (homebrew-баланс).

    level=1 — базовый статблок БЕЗ изменений; дальше существо крепчает. Все
    коэффициенты собраны ЗДЕСЬ — это игровой баланс «для себя», правится в одном
    месте. round — половинка вверх (см. _round_half_up), чтобы серверный расчёт
    совпал с превью на клиенте. Формула:
      • HP (max и current): base * (1 + 0.5*(level-1))   ≈ +50% за уровень
      • AC: base + (level-1)//2                          каждые 2 уровня +1
      • action.rolls type "attack": modifier += (level-1)//2
      • action.rolls type "damage": число кубиков *= max(1, round(level/2)),
        modifier += (level-1)   (1d6 → на ур.3 = 2d6)
      • abilities — без изменений
    Возвращает НОВЫЙ статблок (вход не мутируется)."""
    level = max(1, min(20, int(level)))
    sb = copy.deepcopy(statblock)
    if level == 1:
        return sb                                   # базовый статблок как есть

    factor = 1 + 0.5 * (level - 1)
    hp = sb.get("hp") or {}
    if hp:
        base_max = int(hp.get("max") or hp.get("current") or 0)
        base_cur = int(hp.get("current", base_max))
        sb["hp"] = {"max": _round_half_up(base_max * factor),
                    "current": _round_half_up(base_cur * factor)}
    if sb.get("ac") is not None:
        try:
            sb["ac"] = int(sb["ac"]) + (level - 1) // 2
        except (TypeError, ValueError):
            pass

    dice_mult = max(1, _round_half_up(level / 2))    # кубики урона ≈ ×(level/2)
    for action in sb.get("actions") or []:
        for roll in action.get("rolls") or []:
            rtype = roll.get("type")
            if rtype == "attack":
                roll["modifier"] = int(roll.get("modifier", 0)) + (level - 1) // 2
            elif rtype == "damage":
                m = _DMG_DICE_RE.match(str(roll.get("notation", "")))
                if m:
                    count, sides = int(m.group(1)), int(m.group(2))
                    roll["notation"] = f"{count * dice_mult}d{sides}"
                roll["modifier"] = int(roll.get("modifier", 0)) + (level - 1)
    return sb


@dataclass
class Member:
    member_id: str
    name: str
    role: str  # "gm" | "player"
    ws: WebSocket
    char_id: str | None = None
    char_name: str | None = None   # имя персонажа для журнала (а не member_id)


# Граница карты в клетках — для клампинга позиций токенов (защита от заброса).
GRID_MAX = 60

# Стартовая раскладка токенов сцены. controlledBy — источник прав движения:
# 'gm' (двигает только мастер) либо memberId игрока (двигает только он). GM
# двигает любой токен; игрок — только свой. Позиции (x,y) — в КЛЕТКАХ.
DEFAULT_TOKENS = [
    {"id": "tok_liandra", "name": "Лиэндра", "x": 3,  "y": 6, "enemy": False, "controlledBy": "player_dana", "hp": {"current": 22, "max": 27}, "conditions": []},
    {"id": "tok_torin",   "name": "Торин",   "x": 5,  "y": 4, "enemy": False, "controlledBy": "player_max",  "hp": {"current": 31, "max": 48}, "conditions": []},
    {"id": "tok_goblin1", "name": "Гоблин",  "x": 8,  "y": 3, "enemy": True,  "controlledBy": "gm", "hp": {"current": 7, "max": 7}, "conditions": []},
    {"id": "tok_goblin2", "name": "Гоблин",  "x": 10, "y": 5, "enemy": True,  "controlledBy": "gm", "hp": {"current": 7, "max": 7}, "conditions": []},
]


DEFAULT_GRID = {"size": 48, "offsetX": 0, "offsetY": 0}


def new_scene(sid: str, name: str, tokens: dict | None = None, **extra) -> dict:
    """Каноническая структура сцены. mapAssetId — id загруженной карты (клиент
    рисует её, если есть, иначе map-путь из библиотеки). fogEnabled ПО УМОЛЧАНИЮ
    false (город — без тумана; пещеру включают вручную)."""
    return {
        "id": sid, "name": name,
        "map": extra.get("map"),              # путь к предзагруженной карте (библиотека)
        "mapAssetId": extra.get("mapAssetId"),  # id загруженного PNG из таблицы assets
        "grid": dict(extra.get("grid") or DEFAULT_GRID),
        "fogEnabled": bool(extra.get("fogEnabled", False)),
        "notes": extra.get("notes", ""),
        "searchKey": extra.get("searchKey", ""),
        "description": extra.get("description", ""),
        "tokens": tokens if tokens is not None else {},
        "fog": set(),
    }


# Иерархия мира (CLAUDE.md): Мир(room) → scenarios[] → scenes{} → tokens/fog/grid/map.
# Сцены ВЛОЖЕНЫ в сценарий (scenario["scenes"] = {sid: scene}); активны и
# activeScenarioId, и activeSceneId. Дефолтный сценарий-обёртка для новой игры
# и для миграции старого плоского state.
DEFAULT_SCENARIO_ID = "scenario_default"


def new_scenario(scid: str, name: str, scenes: dict | None = None,
                 active_scene_id: str | None = None) -> dict:
    """Каноническая структура сценария: имя + словарь вложенных сцен + своя
    активная сцена (чтобы переключение сценария возвращало на последнюю сцену)."""
    return {"id": scid, "name": name,
            "scenes": scenes if scenes is not None else {},
            "activeSceneId": active_scene_id}


@dataclass
class Room:
    room_id: str
    name: str = ""
    gm_id: str | None = None
    members: dict[str, Member] = field(default_factory=dict)
    log: list = field(default_factory=list)
    # состояние боя (по образцу room-schema) и режим сцены — сессионное состояние
    combat: dict = field(default_factory=lambda: {"active": False, "round": 0, "turnIndex": 0, "order": []})
    mode: str = "explore"  # 'explore' | 'combat'
    # ИЕРАРХИЯ (CLAUDE.md): мир → scenarios{} → scenes{} → tokens/fog/grid/map.
    # Сцены ВЛОЖЕНЫ в сценарий (scenario["scenes"][sid]); токены/туман живут в
    # активной сцене активного сценария. tokens/fog делегируют туда — обработчики
    # token.*/fog.* без изменений. Активны и сценарий, и сцена.
    scenarios: dict[str, dict] = field(default_factory=dict)
    active_scenario_id: str | None = None
    active_scene_id: str | None = None
    lore: dict = field(default_factory=lambda: {"history": "", "notes": ""})  # история/заметки МИРА
    notes: str = ""   # приватные заметки мастера (видны только gm, в БД)

    def scenario_list(self) -> dict:
        """Лёгкий список сценариев (без вложенных сцен-блобов): id/имя + id сцен
        (sceneIds для совместимости клиента) + своя активная сцена."""
        return {"scenarios": [{"id": sc["id"], "name": sc["name"],
                               "sceneIds": list(sc["scenes"].keys()),
                               "activeSceneId": sc.get("activeSceneId")}
                              for sc in self.scenarios.values()],
                "activeScenarioId": self.active_scenario_id}

    def ensure_scene(self) -> dict:
        """Гарантирует дефолтный сценарий «Сценарий 1» с дефолтной сценой и
        валидные active_scenario_id/active_scene_id. Возвращает активную сцену."""
        if not self.scenarios:
            scene = new_scene("scene_default", "Сцена 1",
                              {t["id"]: dict(t) for t in DEFAULT_TOKENS})
            self.scenarios[DEFAULT_SCENARIO_ID] = new_scenario(
                DEFAULT_SCENARIO_ID, "Сценарий 1", {"scene_default": scene}, "scene_default")
            self.active_scenario_id = DEFAULT_SCENARIO_ID
            self.active_scene_id = "scene_default"
        if self.active_scenario_id not in self.scenarios:
            self.active_scenario_id = next(iter(self.scenarios))
        scenario = self.scenarios[self.active_scenario_id]
        scenes = scenario["scenes"]
        if not scenes:                  # сценарий без сцен — заводим дефолтную
            scenes["scene_default"] = new_scene(
                "scene_default", "Сцена 1", {t["id"]: dict(t) for t in DEFAULT_TOKENS})
            scenario["activeSceneId"] = "scene_default"
        if self.active_scene_id not in scenes:
            fallback = scenario.get("activeSceneId")
            self.active_scene_id = fallback if fallback in scenes else next(iter(scenes))
        scenario["activeSceneId"] = self.active_scene_id
        return scenes[self.active_scene_id]

    def active_scenario(self) -> dict:
        self.ensure_scene()
        return self.scenarios[self.active_scenario_id]

    def active_scenes(self) -> dict:
        """Сцены активного сценария (sid -> scene)."""
        return self.active_scenario()["scenes"]

    @property
    def tokens(self) -> dict:           # позиции токенов АКТИВНОЙ сцены (в памяти, «тик»)
        return self.ensure_scene()["tokens"]

    @property
    def fog(self) -> set:               # открытые клетки тумана АКТИВНОЙ сцены
        return self.ensure_scene()["fog"]

    def fog_list(self):
        return [[x, y] for (x, y) in self.fog]

    def seed_tokens_if_empty(self):
        self.ensure_scene()             # достаточно гарантировать дефолтный сценарий+сцену

    def _scene_serialize(self, s: dict) -> dict:
        """Полная сериализация сцены (для клиента и БД): fog set → list."""
        return {"id": s["id"], "name": s["name"], "map": s.get("map"),
                "mapAssetId": s.get("mapAssetId"), "grid": s.get("grid", dict(DEFAULT_GRID)),
                "fogEnabled": bool(s.get("fogEnabled", False)),
                "notes": s.get("notes", ""), "searchKey": s.get("searchKey", ""),
                "description": s.get("description", ""),
                "tokens": list(s["tokens"].values()),
                "fog": [[x, y] for (x, y) in s["fog"]]}

    def scene_state(self, sid: str) -> dict:
        return self._scene_serialize(self.active_scenes()[sid])

    def scene_list(self) -> dict:
        """Сцены АКТИВНОГО сценария + его активная сцена. map/mapAssetId — чтобы
        бар сцен рисовал превью фона без полного scene_state каждой сцены."""
        scenes = self.active_scenes()
        return {"scenes": [{"id": s["id"], "name": s["name"],
                            "map": s.get("map"), "mapAssetId": s.get("mapAssetId")}
                           for s in scenes.values()],
                "activeSceneId": self.active_scene_id}

    def scenarios_state(self) -> list:
        """Полная сериализация всех сценариев со вложенными сценами (для БД)."""
        return [{"id": sc["id"], "name": sc["name"],
                 "activeSceneId": sc.get("activeSceneId"),
                 "scenes": [self._scene_serialize(s) for s in sc["scenes"].values()]}
                for sc in self.scenarios.values()]

    def persist_tokens(self):
        """Снапшот мира в БД на СЕССИОННЫХ событиях (дисконнект GM, NPC, бой, сцены).
        НЕ зовём на каждый token.move (позиции — «тик»). Мерджим в state."""
        self.ensure_scene()
        state = db.get_game_state(self.room_id)
        state["scenarios"] = self.scenarios_state()
        state["activeScenarioId"] = self.active_scenario_id
        state["activeSceneId"] = self.active_scene_id
        state["lore"] = self.lore
        state["combat"] = self.combat
        state["mode"] = self.mode
        state["notes"] = self.notes
        # вычищаем старые плоские поля, чтобы миграция не путалась при перезапуске
        for legacy in ("scenes", "tokens", "fog"):
            state.pop(legacy, None)
        db.save_game_state(self.room_id, state)

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


def require_gm(member: "Member | None") -> bool:
    """Признак прав мастера для соединения. DM-команды этапов 1–4 будут
    вызывать это ДО действия (проверка прав на сервере, не на клиенте).
    Сейчас источник роли — роль соединения (из лобби: место 0 = gm)."""
    return bool(member) and member.role == "gm"


def merged_creatures(room_id: str) -> list[dict]:
    """Единый список бестиария: базовые существа из каталога (SRD) как основа,
    поверх — homebrew из БД по id (homebrew перекрывает srd). source проставлен:
    'SRD' для каталожных, 'HB' для пользовательских (db гарантирует source='HB').
    Пока никуда не подключён — доступен как сборщик для бестиария."""
    merged: dict[str, dict] = {}
    for rec in content_catalog.creatures.values():
        c = dict(rec)
        c.setdefault("source", "SRD")
        merged[c.get("id")] = c
    for hb in db.list_creatures(room_id):
        c = dict(hb)
        c["source"] = "HB"
        merged[c.get("id")] = c
    return list(merged.values())


async def broadcast_creatures(room: "Room"):
    """Разослать всем актуальный бестиарий (srd + homebrew). Зовётся после правки
    homebrew-существа (создан/изменён/удалён), чтобы клиенты обновили store.bestiary
    — тем же снапшотом, что приходит при подключении."""
    await room.broadcast({"type": "creatures.snapshot", "creatures": merged_creatures(room.room_id)})


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
        # мир поднимаем из сохранённого состояния; сид (ensure_scene) — фолбэк.
        # Поддерживаем три формы state ради миграции (не падаем на пустом/битом):
        #   1) НОВАЯ: scenarios=[{id,name,activeSceneId,scenes:[...]}] — иерархия.
        #   2) старая плоская: scenes=[...]            → обернуть в «Сценарий 1».
        #   3) совсем старая: tokens=[...] (до сцен)   → одна дефолтная сцена.
        state = db.get_game_state(room_id)

        def _scene_from_serialized(s: dict):
            sid = s.get("id")
            if not sid:
                return None, None
            sc = new_scene(sid, s.get("name", "Сцена"),
                           {t["id"]: dict(t) for t in (s.get("tokens") or []) if t.get("id")},
                           map=s.get("map"), mapAssetId=s.get("mapAssetId"),
                           grid=s.get("grid"), fogEnabled=s.get("fogEnabled", False),
                           notes=s.get("notes", ""), searchKey=s.get("searchKey", ""),
                           description=s.get("description", ""))
            sc["fog"] = set((int(c[0]), int(c[1])) for c in (s.get("fog") or []) if len(c) == 2)
            return sid, sc

        scenarios_data = state.get("scenarios")
        is_new = (isinstance(scenarios_data, list) and scenarios_data
                  and all(isinstance(x, dict) and "scenes" in x for x in scenarios_data))
        if is_new:
            for scd in scenarios_data:
                scid = scd.get("id")
                if not scid:
                    continue
                scenes = {}
                for s in (scd.get("scenes") or []):
                    sid, sc = _scene_from_serialized(s)
                    if sid:
                        scenes[sid] = sc
                room.scenarios[scid] = new_scenario(
                    scid, scd.get("name", "Сценарий"), scenes, scd.get("activeSceneId"))
            room.active_scenario_id = state.get("activeScenarioId") or (next(iter(room.scenarios), None))
            room.active_scene_id = state.get("activeSceneId")
        elif isinstance(state.get("scenes"), list) and state["scenes"]:
            scenes = {}
            for s in state["scenes"]:
                sid, sc = _scene_from_serialized(s)
                if sid:
                    scenes[sid] = sc
            active = state.get("activeSceneId") or (next(iter(scenes), None))
            room.scenarios[DEFAULT_SCENARIO_ID] = new_scenario(
                DEFAULT_SCENARIO_ID, "Сценарий 1", scenes, active)
            room.active_scenario_id = DEFAULT_SCENARIO_ID
            room.active_scene_id = active
        elif isinstance(state.get("tokens"), list) and state["tokens"]:
            sc = new_scene("scene_default", "Сцена 1",
                           {t["id"]: dict(t) for t in state["tokens"] if t.get("id")})
            sc["fog"] = set((int(c[0]), int(c[1])) for c in (state.get("fog") or []) if len(c) == 2)
            room.scenarios[DEFAULT_SCENARIO_ID] = new_scenario(
                DEFAULT_SCENARIO_ID, "Сценарий 1", {"scene_default": sc}, "scene_default")
            room.active_scenario_id = DEFAULT_SCENARIO_ID
            room.active_scene_id = "scene_default"
        if isinstance(state.get("lore"), dict):
            room.lore = {"history": str(state["lore"].get("history", "")),
                         "notes": str(state["lore"].get("notes", ""))}
        if isinstance(state.get("combat"), dict):
            room.combat = state["combat"]
        if state.get("mode") in ("explore", "combat"):
            room.mode = state["mode"]
        if isinstance(state.get("notes"), str):
            room.notes = state["notes"]
        rooms[room_id] = room
    room.seed_tokens_if_empty()   # фолбэк: новая игра без сохранённой раскладки
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
    # список сцен и ПОЛНОЕ состояние активной сцены (фон, сетка, токены, туман) —
    # новый клиент догоняет карту целиком (scene.switched несёт tokens+fog)
    await ws.send_text(json.dumps({"type": "scene.list", **room.scene_list()}, ensure_ascii=False))
    await ws.send_text(json.dumps({"type": "scenario.list", **room.scenario_list()}, ensure_ascii=False))
    await ws.send_text(json.dumps({"type": "scene.switched", "sceneId": room.active_scene_id,
                                   "state": room.scene_state(room.active_scene_id)}, ensure_ascii=False))
    # и текущее состояние боя/режима сцены
    await ws.send_text(json.dumps({"type": "combat.updated", "combat": room.combat}, ensure_ascii=False))
    await ws.send_text(json.dumps({"type": "mode.set", "mode": room.mode}, ensure_ascii=False))
    # каталог контента (заклинания/предметы/классы/расы/предыстории) — для codex/визарда
    await ws.send_text(json.dumps({"type": "catalog.snapshot", **content_catalog.snapshot()}, ensure_ascii=False))
    # бестиарий: srd + homebrew единым списком (как catalog.snapshot) — для выбора существ
    await ws.send_text(json.dumps({"type": "creatures.snapshot", "creatures": merged_creatures(room_id)}, ensure_ascii=False))
    # лор мира (история + заметки о мире) — виден ВСЕМ (read-only игрокам, правит GM)
    await ws.send_text(json.dumps({"type": "world.lore", **room.lore}, ensure_ascii=False))
    # приватные заметки мастера — только мастеру (игрокам не шлём)
    if role == "gm":
        await ws.send_text(json.dumps({"type": "notes.snapshot", "notes": room.notes}, ensure_ascii=False))
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
            # уход GM — сессионное событие: снапшотим текущие позиции токенов в БД
            if role == "gm":
                room.persist_tokens()
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

    # --- движение токена: проверка прав на сервере (источник истины) ---
    if mtype == "token.move":
        token = room.tokens.get(msg.get("tokenId"))
        if token is None:
            return  # нет такого токена — молча игнорируем
        # право двигать: GM двигает ЛЮБОЙ токен; игрок — только тот, чей
        # controlledBy == его member_id. Чужой токен сервер не двигает, что бы
        # клиент ни прислал (он мог нарисовать у себя что угодно).
        if role != "gm" and token.get("controlledBy") != member_id:
            return
        to = msg.get("to") or {}
        try:
            x, y = int(to.get("x")), int(to.get("y"))
        except (TypeError, ValueError):
            return
        # клампим в [0, GRID_MAX] — защита от заброса за карту
        x = max(0, min(GRID_MAX, x))
        y = max(0, min(GRID_MAX, y))
        token["x"], token["y"] = x, y
        await room.broadcast({"type": "token.moved", "tokenId": token["id"],
                              "to": {"x": x, "y": y}, "by": member_id})
        return

    # --- изменение HP токена: та же модель прав, что у token.move ---
    if mtype == "token.hp":
        token = room.tokens.get(msg.get("tokenId"))
        if token is None:
            return
        # GM меняет HP любому; игрок — только своему токену (controlledBy)
        if role != "gm" and token.get("controlledBy") != member_id:
            return
        hp = token.get("hp") or {"current": 0, "max": 0}
        mx = int(hp.get("max") or 0)
        try:
            if msg.get("set") is not None:
                cur = int(msg["set"])
            elif msg.get("delta") is not None:
                cur = int(hp.get("current", 0)) + int(msg["delta"])
            else:
                return
        except (TypeError, ValueError):
            return
        if mx <= 0:
            mx = max(cur, int(hp.get("current", 0)))   # нет max — берём за потолок текущее/новое
        cur = max(0, min(mx, cur))
        token["hp"] = {"current": cur, "max": mx}
        token["down"] = cur <= 0                        # маркер выбывания
        await room.broadcast({"type": "token.hp.changed", "tokenId": token["id"],
                              "hp": token["hp"], "down": token["down"]})
        room.persist_tokens()                           # HP — сессионное событие (не позиция)
        return

    # --- состояния токена (poisoned/stunned/...): та же модель прав ---
    if mtype == "token.condition":
        token = room.tokens.get(msg.get("tokenId"))
        if token is None:
            return
        if role != "gm" and token.get("controlledBy") != member_id:
            return
        conds = list(token.get("conditions") or [])
        for c in (msg.get("add") or []):
            if c and c not in conds:
                conds.append(c)
        for c in (msg.get("remove") or []):
            if c in conds:
                conds.remove(c)
        token["conditions"] = conds
        await room.broadcast({"type": "token.condition.changed", "tokenId": token["id"],
                              "conditions": conds})
        room.persist_tokens()
        return

    # --- туман войны (gm-only): кисть открыть/скрыть клетки ---
    if mtype in ("fog.reveal", "fog.hide"):
        if not require_gm(room.members.get(member_id)):
            return
        reveal = mtype == "fog.reveal"
        for c in (msg.get("cells") or []):
            try:
                x = max(0, min(GRID_MAX, int(c[0])))
                y = max(0, min(GRID_MAX, int(c[1])))
            except (TypeError, ValueError, IndexError):
                continue
            if reveal:
                room.fog.add((x, y))
            else:
                room.fog.discard((x, y))
        await room.broadcast({"type": "fog.updated", "revealed": room.fog_list()})
        room.persist_tokens()
        return

    # --- приватные заметки мастера (gm-only): храним в БД, игрокам НЕ шлём ---
    if mtype == "notes.set":
        if not require_gm(room.members.get(member_id)):
            return
        room.notes = str(msg.get("notes", ""))
        room.persist_tokens()                 # пишем в games.state
        return

    # --- лор мира (gm-only): история+заметки о мире; видны ВСЕМ (read-only) ---
    if mtype == "lore.set":
        if not require_gm(room.members.get(member_id)):
            return
        if "history" in msg:
            room.lore["history"] = str(msg.get("history") or "")
        if "notes" in msg:
            room.lore["notes"] = str(msg.get("notes") or "")
        await room.broadcast({"type": "world.lore", **room.lore})
        room.persist_tokens()                 # пишем в games.state (state.lore)
        return

    # --- сцены (gm-only): создать / переключить / настроить фон-сетку ---
    # Сцена живёт ВНУТРИ активного сценария (scenario["scenes"]).
    if mtype == "scene.create":
        if not require_gm(room.members.get(member_id)):
            return
        scenario = room.active_scenario()      # гарантирует дефолтный сценарий
        sid = "scene_" + secrets.token_hex(3)
        scenario["scenes"][sid] = new_scene(
            sid, msg.get("name") or "Новая сцена",
            map=msg.get("map"), mapAssetId=msg.get("mapAssetId"), grid=msg.get("grid"),
            fogEnabled=msg.get("fogEnabled", False), notes=msg.get("notes", ""),
            searchKey=msg.get("searchKey", ""), description=msg.get("description", ""))
        if not scenario.get("activeSceneId"):
            scenario["activeSceneId"] = sid
        await room.broadcast({"type": "scenario.list", **room.scenario_list()})
        await room.broadcast({"type": "scene.list", **room.scene_list()})
        room.persist_tokens()
        return

    # --- сценарии (gm-only): слой между миром и сценами; сцены вложены внутрь ---
    if mtype == "scenario.create":
        if not require_gm(room.members.get(member_id)):
            return
        room.ensure_scene()                    # гарантируем, что активный мир валиден
        scid = "scenario_" + secrets.token_hex(3)
        sid = "scene_" + secrets.token_hex(3)
        scene = new_scene(sid, "Сцена 1")      # одна ПУСТАЯ дефолтная сцена (без токенов)
        room.scenarios[scid] = new_scenario(
            scid, msg.get("name") or "Новый сценарий", {sid: scene}, sid)
        # просто добавляем — активный сценарий НЕ меняем
        await room.broadcast({"type": "scenario.list", **room.scenario_list()})
        room.persist_tokens()
        return

    if mtype == "scenario.switch":
        if not require_gm(room.members.get(member_id)):
            return
        sc = room.scenarios.get(msg.get("scenarioId"))
        if not sc:
            return
        room.active_scenario_id = sc["id"]
        room.active_scene_id = next(iter(sc["scenes"]), None)   # активной — первая сцена
        room.ensure_scene()                    # гарантирует сцену + валидный active
        sc["activeSceneId"] = room.active_scene_id
        await room.broadcast({"type": "scenario.switched", "scenarioId": sc["id"],
                              "activeSceneId": room.active_scene_id,
                              "scenes": room.scene_list()["scenes"]})
        await room.broadcast({"type": "scene.switched", "sceneId": room.active_scene_id,
                              "state": room.scene_state(room.active_scene_id)})
        room.persist_tokens()
        return

    if mtype == "scenario.rename":
        if not require_gm(room.members.get(member_id)):
            return
        sc = room.scenarios.get(msg.get("scenarioId"))
        name = msg.get("name")
        if not sc or not name:
            return
        sc["name"] = str(name)
        await room.broadcast({"type": "scenario.list", **room.scenario_list()})
        room.persist_tokens()
        return

    if mtype == "scenario.delete":
        if not require_gm(room.members.get(member_id)):
            return
        scid = msg.get("scenarioId")
        if scid not in room.scenarios or len(room.scenarios) <= 1:
            return                             # нельзя удалить последний сценарий
        was_active = room.active_scenario_id == scid
        del room.scenarios[scid]
        if was_active:                         # удалили активный — переключиться на другой
            room.active_scenario_id = next(iter(room.scenarios))
            room.active_scene_id = next(iter(room.scenarios[room.active_scenario_id]["scenes"]), None)
            room.ensure_scene()
            room.scenarios[room.active_scenario_id]["activeSceneId"] = room.active_scene_id
            await room.broadcast({"type": "scenario.switched", "scenarioId": room.active_scenario_id,
                                  "activeSceneId": room.active_scene_id,
                                  "scenes": room.scene_list()["scenes"]})
            await room.broadcast({"type": "scene.switched", "sceneId": room.active_scene_id,
                                  "state": room.scene_state(room.active_scene_id)})
        await room.broadcast({"type": "scenario.list", **room.scenario_list()})
        room.persist_tokens()
        return

    if mtype in ("scenario.addScene", "scenario.removeScene"):
        if not require_gm(room.members.get(member_id)):
            return
        sc = room.scenarios.get(msg.get("scenarioId"))
        sid = msg.get("sceneId")
        if not sc:
            return
        # В новой модели сцена принадлежит ровно одному сценарию (общего пула нет):
        # addScene — нечего добавлять, тихо игнорируем; removeScene — удаляет сцену.
        if mtype == "scenario.removeScene" and sid in sc["scenes"]:
            del sc["scenes"][sid]
            if sc.get("activeSceneId") == sid:
                sc["activeSceneId"] = next(iter(sc["scenes"]), None)
            # если удалили активную сцену активного сценария — переключиться
            if room.active_scenario_id == sc["id"] and room.active_scene_id == sid:
                room.active_scene_id = sc.get("activeSceneId")
                room.ensure_scene()
                await room.broadcast({"type": "scene.switched", "sceneId": room.active_scene_id,
                                      "state": room.scene_state(room.active_scene_id)})
        await room.broadcast({"type": "scenario.list", **room.scenario_list()})
        await room.broadcast({"type": "scene.list", **room.scene_list()})
        room.persist_tokens()
        return

    if mtype == "scenario.activate":
        if not require_gm(room.members.get(member_id)):
            return
        sc = room.scenarios.get(msg.get("scenarioId"))
        if not sc:
            return
        room.active_scenario_id = sc["id"]
        room.active_scene_id = sc.get("activeSceneId")
        room.ensure_scene()                    # гарантирует сцену в сценарии + валидный active
        await room.broadcast({"type": "scenario.list", **room.scenario_list()})
        await room.broadcast({"type": "scene.list", **room.scene_list()})
        await room.broadcast({"type": "scene.switched", "sceneId": room.active_scene_id,
                              "state": room.scene_state(room.active_scene_id)})
        room.persist_tokens()
        return

    if mtype == "scene.switch":
        if not require_gm(room.members.get(member_id)):
            return
        sid = msg.get("sceneId")
        if sid not in room.active_scenes():
            return
        room.active_scene_id = sid
        room.active_scenario()["activeSceneId"] = sid   # сценарий помнит свою сцену
        await room.broadcast({"type": "scene.switched", "sceneId": sid, "state": room.scene_state(sid)})
        await room.broadcast({"type": "scene.list", **room.scene_list()})
        room.persist_tokens()
        return

    # --- перестановка порядка сцен в сценарии (gm-only, drag в баре сцен) ---
    if mtype == "scene.reorder":
        if not require_gm(room.members.get(member_id)):
            return
        sc = room.scenarios.get(msg.get("scenarioId"))
        if not sc:
            return
        scenes = sc["scenes"]
        # пересобрать dict в присланном порядке; чужие id игнор, неупомянутые — в конец
        reordered = {sid: scenes[sid] for sid in (msg.get("sceneIds") or []) if sid in scenes}
        for sid, s in scenes.items():
            reordered.setdefault(sid, s)
        sc["scenes"] = reordered
        await room.broadcast({"type": "scenario.list", **room.scenario_list()})
        if room.active_scenario_id == sc["id"]:
            await room.broadcast({"type": "scene.list", **room.scene_list()})
        room.persist_tokens()
        return

    # --- переименование сцены (gm-only) ---
    if mtype == "scene.rename":
        if not require_gm(room.members.get(member_id)):
            return
        s = room.active_scenes().get(msg.get("sceneId"))
        name = msg.get("name")
        if not s or not name:
            return
        s["name"] = str(name)
        await room.broadcast({"type": "scene.list", **room.scene_list()})
        await room.broadcast({"type": "scene.updated", "sceneId": s["id"], "state": room.scene_state(s["id"])})
        room.persist_tokens()
        return

    if mtype == "scene.config":
        if not require_gm(room.members.get(member_id)):
            return
        s = room.active_scenes().get(msg.get("sceneId"))
        if not s:
            return
        # обновляем только переданные поля (как map/grid)
        if "map" in msg:
            s["map"] = msg.get("map")
        if "mapAssetId" in msg:
            s["mapAssetId"] = msg.get("mapAssetId")
        if "name" in msg and msg.get("name"):
            s["name"] = msg["name"]
        if "fogEnabled" in msg:
            s["fogEnabled"] = bool(msg["fogEnabled"])
        for k in ("notes", "searchKey", "description"):
            if k in msg:
                s[k] = str(msg.get(k) or "")
        if isinstance(msg.get("grid"), dict):
            g = s.get("grid", dict(DEFAULT_GRID))
            for k in ("size", "offsetX", "offsetY"):
                if msg["grid"].get(k) is not None:
                    try:
                        g[k] = int(msg["grid"][k])
                    except (TypeError, ValueError):
                        pass
            s["grid"] = g
        # broadcast ПОЛНОЕ состояние сцены (а не только map/grid)
        await room.broadcast({"type": "scene.updated", "sceneId": s["id"], "state": room.scene_state(s["id"])})
        room.persist_tokens()
        return

    # --- добавить токен (gm-only): расстановка из песочницы/игры ---
    if mtype == "token.add":
        if not require_gm(room.members.get(member_id)):
            return
        t = dict(msg.get("token") or {})
        t["id"] = t.get("id") or ("tok_" + secrets.token_hex(3))
        t.setdefault("controlledBy", "gm")
        t.setdefault("enemy", False)
        t.setdefault("name", "Токен")
        try:
            t["x"] = max(0, min(GRID_MAX, int(t.get("x", 0))))
            t["y"] = max(0, min(GRID_MAX, int(t.get("y", 0))))
        except (TypeError, ValueError):
            return
        room.tokens[t["id"]] = t
        room.persist_tokens()
        await room.broadcast({"type": "token.added", "token": t})
        return

    # --- удалить токен (gm-only) ---
    if mtype == "token.remove":
        if not require_gm(room.members.get(member_id)):
            return
        tid = msg.get("tokenId")
        if tid in room.tokens:
            del room.tokens[tid]
            room.persist_tokens()
            await room.broadcast({"type": "token.removed", "tokenId": tid})
        return

    # --- старт боя: сервер кидает инициативу (gm-only, бросок считает сервер) ---
    if mtype == "combat.start":
        if not require_gm(room.members.get(member_id)):
            return
        order = []
        for tid in (msg.get("participants") or []):
            tok = room.tokens.get(tid)
            if not tok:
                continue
            # модификатор инициативы: из статблока (Ловкость), иначе 0
            abil = (tok.get("statblock") or {}).get("abilities") or {}
            dex = abil.get("dex")
            dex_mod = (int(dex) - 10) // 2 if dex is not None else 0
            roll = random.randint(1, 20) + dex_mod
            order.append({"tokenId": tid, "name": tok.get("name", "?"),
                          "initiative": roll, "dexMod": dex_mod})
        # сортировка по инициативе (убыв.), ничьи — по Ловкости, затем стабильно
        order.sort(key=lambda o: (-o["initiative"], -o["dexMod"]))
        room.combat = {"active": True, "round": 1, "turnIndex": 0, "order": order}
        room.mode = "combat"
        await room.broadcast({"type": "combat.updated", "combat": room.combat})
        await room.broadcast({"type": "mode.set", "mode": room.mode})
        room.persist_tokens()  # combat/mode — сессионное состояние
        return

    # --- следующий ход (gm-only) ---
    if mtype == "combat.next":
        if not require_gm(room.members.get(member_id)):
            return
        c = room.combat
        if not c.get("active") or not c.get("order"):
            return
        c["turnIndex"] += 1
        if c["turnIndex"] >= len(c["order"]):
            c["turnIndex"] = 0
            c["round"] += 1            # полный круг — новый раунд
        await room.broadcast({"type": "combat.updated", "combat": c})
        room.persist_tokens()
        return

    # --- конец боя (gm-only) -> возврат в режим исследования ---
    if mtype == "combat.end":
        if not require_gm(room.members.get(member_id)):
            return
        room.combat = {"active": False, "round": 0, "turnIndex": 0, "order": []}
        room.mode = "explore"
        await room.broadcast({"type": "combat.updated", "combat": room.combat})
        await room.broadcast({"type": "mode.set", "mode": room.mode})
        room.persist_tokens()
        return

    # --- переключение режима сцены (gm-only) ---
    if mtype == "mode.set":
        if not require_gm(room.members.get(member_id)):
            return
        mode = msg.get("mode")
        if mode not in ("explore", "combat"):
            return
        room.mode = mode
        await room.broadcast({"type": "mode.set", "mode": mode})
        room.persist_tokens()
        return

    # --- создание NPC-фишек: только мастер (проверка прав на сервере) ---
    if mtype == "npc.create":
        if not require_gm(room.members.get(member_id)):
            return  # gm-only: игроку молча отказываем
        statblock = dict(msg.get("statblock") or {})
        # «уровень» существа — множитель статблока. Источник истины — СЕРВЕР:
        # клиент шлёт только level, пересчёт делает scale_statblock (клиентское
        # превью — лишь подсказка GM, на сцену кладётся посчитанное здесь).
        try:
            level = max(1, min(20, int(msg.get("level", 1))))
        except (TypeError, ValueError):
            level = 1
        if level > 1:
            statblock = scale_statblock(statblock, level)
        statblock["npc"] = True                       # единая структура с character-schema
        base_name = statblock.get("name") or "NPC"
        try:
            count = int(msg.get("count", 1))
        except (TypeError, ValueError):
            count = 1
        count = max(1, min(20, count))
        try:
            bx, by = int(msg.get("x", 5)), int(msg.get("y", 5))
        except (TypeError, ValueError):
            bx, by = 5, 5
        added = []
        for i in range(count):
            tok_id = "tok_npc_" + secrets.token_hex(3)
            name = base_name if count == 1 else f"{base_name} {i + 1}"
            # копии чуть смещаем, чтобы не легли в одну клетку; клампим в карту
            x = max(0, min(GRID_MAX, bx + (i % 5)))
            y = max(0, min(GRID_MAX, by + (i // 5)))
            hp = dict(statblock.get("hp") or {"current": 1, "max": 1})
            tok = {"id": tok_id, "name": name, "x": x, "y": y, "enemy": True,
                   "controlledBy": "gm", "statblock": dict(statblock),
                   "hp": hp, "conditions": [], "down": False, "level": level}
            room.tokens[tok_id] = tok
            added.append(tok)
        room.persist_tokens()                          # сессионное событие — сохраняем
        for tok in added:
            await room.broadcast({"type": "token.added", "token": tok})
        return

    # TODO: scene.switch (gm only), combat.start/next (gm only),
    #       audio.play (gm only), fog.reveal (gm only)


@app.get("/health")
def health():
    return {"ok": True, "rooms": len(rooms)}


# ---- Лобби (HTTP) — экраны ДО входа в WS-комнату, удобнее обычным fetch ----

@app.get("/api/catalog")
def api_catalog():
    # каталог для мастера создания персонажа (классы/расы/предыстории + spells/items)
    return content_catalog.snapshot()


MAX_ASSET_BYTES = 20 * 1024 * 1024   # 20 МБ — карты тяжёлые, но не безразмерные
EXT_BY_MIME = {"image/png": "png", "image/jpeg": "jpg", "image/webp": "webp"}


def _game_exists(room_id: str) -> bool:
    return any(g["roomId"] == room_id for g in db.list_games())


def _image_size(data: bytes):
    """Размеры картинки (w, h) из заголовков PNG/JPEG/WEBP без зависимостей.
    Не распознали — (None, None) (размеры не критичны, как и сказано в задаче)."""
    try:
        if data[:8] == b"\x89PNG\r\n\x1a\n" and len(data) >= 24:
            return int.from_bytes(data[16:20], "big"), int.from_bytes(data[20:24], "big")
        if data[:2] == b"\xff\xd8":                       # JPEG: ищем SOF-маркер
            i, n = 2, len(data)
            while i + 9 < n:
                if data[i] != 0xFF:
                    i += 1
                    continue
                marker = data[i + 1]
                if 0xC0 <= marker <= 0xCF and marker not in (0xC4, 0xC8, 0xCC):
                    return (int.from_bytes(data[i + 7:i + 9], "big"),
                            int.from_bytes(data[i + 5:i + 7], "big"))
                i += 2 + int.from_bytes(data[i + 2:i + 4], "big")
        if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
            fmt = data[12:16]
            if fmt == b"VP8X":
                return (int.from_bytes(data[24:27], "little") + 1,
                        int.from_bytes(data[27:30], "little") + 1)
            if fmt == b"VP8 ":
                return (int.from_bytes(data[26:28], "little") & 0x3FFF,
                        int.from_bytes(data[28:30], "little") & 0x3FFF)
    except Exception:
        pass
    return None, None


@app.post("/api/games/{room_id}/assets")
async def api_upload_asset(room_id: str, file: UploadFile = File(...),
                           name: str = Form(""), kind: str = Form("map")):
    # Загрузка картинки мира: файл на ДИСК (data/assets/{room}/{file}), в БД —
    # только метаданные+ссылка. Тяжёлый бинарь по HTTP (не через WS-снапшоты).
    # TODO: проверка прав (gm) — пока открыто для «себя с друзьями».
    if not _game_exists(room_id):
        raise HTTPException(status_code=404, detail="game not found")
    mime = (file.content_type or "").split(";")[0].strip()
    ext = EXT_BY_MIME.get(mime)
    if not ext:
        raise HTTPException(status_code=415, detail="image png/jpeg/webp required")
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty file")
    if len(data) > MAX_ASSET_BYTES:
        raise HTTPException(status_code=413, detail="asset too large")
    if kind not in ("map", "token", "portrait"):
        kind = "map"
    asset_id = "asset_" + secrets.token_hex(6)
    room_dir = ASSETS_DIR / room_id
    room_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{asset_id}.{ext}"
    (room_dir / fname).write_bytes(data)
    rel = f"{room_id}/{fname}"                # относительный путь от ASSETS_DIR
    url = f"/assets/{rel}"
    w, h = _image_size(data)
    return db.create_asset(asset_id, room_id, name or fname, kind, rel, url, w, h)


@app.delete("/api/games/{room_id}/assets/{asset_id}")
def api_delete_asset(room_id: str, asset_id: str):
    # Удаляем запись и файл с диска. TODO: проверка прав (gm).
    meta = db.get_asset(asset_id)
    if not meta or meta["roomId"] != room_id:
        raise HTTPException(status_code=404, detail="asset not found")
    path = db.delete_asset(asset_id)
    if path:
        try:
            (ASSETS_DIR / path).unlink(missing_ok=True)
        except OSError:
            pass
    return {"ok": True, "assetId": asset_id}


@app.get("/api/games/{room_id}/assets")
def api_list_assets(room_id: str):
    return db.list_assets(room_id)


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


@app.get("/api/games/{room_id}/characters/{char_id}")
def api_get_character(room_id: str, char_id: str):
    # полный персонаж по id (бестиарий: поставить токен с его статблоком)
    ch = db.get_character(char_id)
    if ch is None:
        raise HTTPException(status_code=404, detail="character not found")
    return ch


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


# Файлы ассетов мира (карты/токены/портреты) — отдаём с диска. ВАЖНО: монтируем
# ДО корневого "/", иначе корневой mount перехватит /assets/*.
app.mount("/assets", StaticFiles(directory=ASSETS_DIR), name="assets")

# Остальные файлы docs/ (схемы, эскизы) — статикой. Mount на "/" ловит всё,
# что не совпало с маршрутами выше (включая /ws — он остаётся нетронутым).
app.mount("/", StaticFiles(directory=DOCS_DIR), name="static")
