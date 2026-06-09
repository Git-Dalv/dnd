"""
db.py — persistence на SQLite. Появляется ТОЛЬКО для того, что должно пережить
перезапуск: каталог игр, созданные персонажи, состав мест. Живое состояние
сессии (кто сейчас подключён, открытый whisper, текущие броски) остаётся в
памяти в Room — БД его не заменяет, а служит «полкой», с которой комната
поднимается при старте.

Граница (важно, см. CLAUDE.md «состояние в памяти»):
  В БД          — игры, персонажи, занятые места (события уровня «сессия»).
  В памяти      — подключения, лог, позиции токенов в моменте (события «тика»).
Не писать в БД на каждое движение токена: только на события уровня сессии
(создан персонаж, занято/освобождено место, игра закрыта).

Гибрид хранения: реляционные колонки для того, по чему ищем и показываем список
(id, имя игры, дата), + JSON-блоб для сложных объектов, форма которых уже задана
схемами проекта (character-schema.json целиком лежит в characters.data).
"""
import sqlite3
import json
import time
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "vtt.db"

SCHEMA = """
-- Игра = комната, переживающая рестарт. room_id (он же код для входа по ссылке)
-- остаётся первичным ключом, совместимым с /ws/{room_id}/...
CREATE TABLE IF NOT EXISTS games (
    room_id     TEXT PRIMARY KEY,         -- код комнаты (вход по ссылке)
    name        TEXT NOT NULL,            -- "Waterdeep: Dragon Heist"
    gm_id       TEXT,                     -- кто создал (роль gm)
    created_at  INTEGER NOT NULL,
    -- редко меняющееся состояние игры (активная сцена, аудио, combat) — блобом.
    -- Живые позиции токенов в моменте сюда НЕ пишем на каждый тик; снапшот по сессии.
    state       TEXT NOT NULL DEFAULT '{}'
);

-- Персонаж принадлежит игре. Сам объект — по character-schema.json, блобом в data.
-- Наверх вынесены только поля для списка выбора (имя, класс, уровень, владелец).
CREATE TABLE IF NOT EXISTS characters (
    char_id     TEXT PRIMARY KEY,
    room_id     TEXT NOT NULL REFERENCES games(room_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    cls         TEXT,                     -- класс (для списка)
    level       INTEGER,                  -- уровень (для списка)
    owner_id    TEXT,                     -- какому member принадлежит (может быть null)
    data        TEXT NOT NULL,            -- весь персонаж по character-schema.json
    created_at  INTEGER NOT NULL
);

-- Места в игре: 4 игрока + 1 DM. Правило лимита проверяет СЕРВЕР при попытке
-- занять место (источник истины), не клиент. Строка = одно место.
-- role: 'gm' | 'player'. seat_no: 0 для gm, 1..4 для игроков.
CREATE TABLE IF NOT EXISTS seats (
    room_id      TEXT NOT NULL REFERENCES games(room_id) ON DELETE CASCADE,
    seat_no      INTEGER NOT NULL,        -- 0=DM, 1..4=игроки
    role         TEXT NOT NULL,           -- 'gm' | 'player'
    member_id    TEXT,                    -- кто занял (null = свободно)
    char_id      TEXT REFERENCES characters(char_id) ON DELETE SET NULL,
    display_name TEXT,                    -- имя игрока (для журнала при «голом» входе без ?name=)
    PRIMARY KEY (room_id, seat_no)
);

CREATE INDEX IF NOT EXISTS idx_characters_room ON characters(room_id);

-- Homebrew-существа бестиария. Базовые существа (SRD) — статичный JSON в каталоге
-- (read-only), а пользовательские правки/новые существа живут здесь, как characters.
-- Наверх вынесены поля для списка/вкладок (имя, категория, расположение),
-- полный статблок — блобом в data (source всегда 'HB'). В бестиарии srd+hb
-- сливаются по id: homebrew перекрывает srd.
CREATE TABLE IF NOT EXISTS creatures (
    creature_id TEXT PRIMARY KEY,
    room_id     TEXT NOT NULL REFERENCES games(room_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    category    TEXT,
    disposition TEXT,
    data        TEXT NOT NULL,            -- полный статблок блобом
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_creatures_room ON creatures(room_id);

-- Wiki мира (закладываем схему сразу, UI позже). Фракции и досье — пер-мир
-- (room_id), как персонажи/существа: реляционные колонки для списка + JSON-блоб.
-- Пока НИКЕМ не используются; таблицы заводятся, чтобы база миров была полной.
CREATE TABLE IF NOT EXISTS factions (
    faction_id  TEXT PRIMARY KEY,
    room_id     TEXT NOT NULL REFERENCES games(room_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    data        TEXT NOT NULL,            -- полная фракция блобом
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_factions_room ON factions(room_id);

-- Досье (dossier): запись о персонаже/NPC мира. char_id опц. связывает с листом
-- персонажа (characters), kind различает игроков и NPC.
CREATE TABLE IF NOT EXISTS dossiers (
    dossier_id  TEXT PRIMARY KEY,
    room_id     TEXT NOT NULL REFERENCES games(room_id) ON DELETE CASCADE,
    kind        TEXT,                     -- 'pc' | 'npc'
    name        TEXT NOT NULL,
    char_id     TEXT,                     -- опц. ссылка на characters.char_id
    data        TEXT NOT NULL,            -- полное досье блобом
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dossiers_room ON dossiers(room_id);

-- Ассеты мира (карты/токены/портреты). Картинки ТЯЖЁЛЫЕ — файл лежит на ДИСКЕ
-- (data/assets/{room}/{file}), в БД только метаданные и ссылка (НЕ dataURL в
-- состоянии — оно раздувает БД/снапшоты). Отдаются по HTTP со static-mount.
CREATE TABLE IF NOT EXISTS assets (
    asset_id    TEXT PRIMARY KEY,
    room_id     TEXT NOT NULL REFERENCES games(room_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,            -- человекочитаемое имя
    kind        TEXT NOT NULL,            -- map | token | portrait
    path        TEXT NOT NULL,            -- относительный путь файла ({room}/{file})
    url         TEXT NOT NULL,            -- /assets/{room}/{file}
    w INTEGER, h INTEGER,
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_assets_room ON assets(room_id);
"""

MAX_PLAYER_SEATS = 4   # + 1 DM = 5 мест всего


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    with connect() as conn:
        conn.executescript(SCHEMA)
        # миграция для БД, созданных до колонки seats.display_name (идемпотентно)
        try:
            conn.execute("ALTER TABLE seats ADD COLUMN display_name TEXT")
        except sqlite3.OperationalError:
            pass                          # колонка уже есть — ок


# ---- игры -----------------------------------------------------------------

def create_game(room_id: str, name: str, gm_id: str) -> dict:
    now = int(time.time())
    with connect() as conn:
        conn.execute(
            "INSERT INTO games (room_id, name, gm_id, created_at) VALUES (?,?,?,?)",
            (room_id, name, gm_id, now),
        )
        # заводим 5 мест: 0 = DM, 1..4 = игроки
        conn.execute("INSERT INTO seats (room_id, seat_no, role) VALUES (?,?,?)",
                     (room_id, 0, "gm"))
        for n in range(1, MAX_PLAYER_SEATS + 1):
            conn.execute("INSERT INTO seats (room_id, seat_no, role) VALUES (?,?,?)",
                         (room_id, n, "player"))
    return {"roomId": room_id, "name": name, "gmId": gm_id, "createdAt": now}


def list_games() -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT room_id, name, gm_id, created_at FROM games ORDER BY created_at DESC"
        ).fetchall()
    return [{"roomId": r["room_id"], "name": r["name"],
             "gmId": r["gm_id"], "createdAt": r["created_at"]} for r in rows]


def ensure_game(room_id: str, name: str | None = None, gm_id: str | None = None) -> bool:
    """Создаёт игру (+ места), если её ещё нет. Идемпотентно. Нужно для ad-hoc/
    песочница-комнат, которые открываются по WS/ссылке без лобби: без строки в
    games у них ломаются ассеты (FK) и не пишется состояние. True, если создал."""
    with connect() as conn:
        if conn.execute("SELECT 1 FROM games WHERE room_id=?", (room_id,)).fetchone():
            return False
    try:
        create_game(room_id, name or room_id, gm_id)
        return True
    except sqlite3.IntegrityError:
        return False                 # гонка: кто-то создал параллельно — это ок


def get_game(room_id: str) -> dict | None:
    """Точечная карточка игры (метаданные без state). Источник истины о GM —
    games.gm_id (для проверки прав на HTTP)."""
    with connect() as conn:
        row = conn.execute(
            "SELECT room_id, name, gm_id, created_at FROM games WHERE room_id=?", (room_id,)
        ).fetchone()
    return {"roomId": row["room_id"], "name": row["name"],
            "gmId": row["gm_id"], "createdAt": row["created_at"]} if row else None


def get_game_state(room_id: str) -> dict:
    """Читает games.state (редко меняющееся состояние сцены: токены, позиции,
    controlledBy). Пустой {}, если игры нет или state пуст/битый."""
    with connect() as conn:
        row = conn.execute("SELECT state FROM games WHERE room_id=?", (room_id,)).fetchone()
    if not row or not row["state"]:
        return {}
    try:
        return json.loads(row["state"])
    except (TypeError, ValueError):
        return {}


def save_game_state(room_id: str, state: dict) -> None:
    """Пишет состояние сцены в games.state по room_id. Это «полка» для редкого
    состояния; живое состояние сессии (подключения, лог) остаётся в памяти Room."""
    with connect() as conn:
        conn.execute("UPDATE games SET state=? WHERE room_id=?",
                     (json.dumps(state, ensure_ascii=False), room_id))


# ---- персонажи ------------------------------------------------------------

def create_character(char_id: str, room_id: str, data: dict, owner_id: str | None) -> dict:
    now = int(time.time())
    meta = data.get("meta", {})
    with connect() as conn:
        conn.execute(
            "INSERT INTO characters (char_id, room_id, name, cls, level, owner_id, data, created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (char_id, room_id, data.get("name", "?"), meta.get("class"),
             meta.get("level"), owner_id, json.dumps(data, ensure_ascii=False), now),
        )
    return data


def list_characters(room_id: str) -> list[dict]:
    """Список для экрана выбора: лёгкие метаданные, без полного блоба."""
    with connect() as conn:
        rows = conn.execute(
            "SELECT char_id, name, cls, level, owner_id FROM characters WHERE room_id=?",
            (room_id,),
        ).fetchall()
    return [{"charId": r["char_id"], "name": r["name"], "class": r["cls"],
             "level": r["level"], "ownerId": r["owner_id"]} for r in rows]


def get_character(char_id: str) -> dict | None:
    with connect() as conn:
        row = conn.execute("SELECT data FROM characters WHERE char_id=?", (char_id,)).fetchone()
    return json.loads(row["data"]) if row else None


def update_character_hotbar(char_id: str, hotbar: list) -> bool:
    """Обновить ТОЛЬКО раскладку бара внутри блоба персонажа (characters.data).
    Раскладка едет вместе с персонажем — отдельная таблица не нужна. Это событие
    уровня сессии (перетащил в слот), пишем сразу. Возвращает True, если нашли."""
    with connect() as conn:
        row = conn.execute("SELECT data FROM characters WHERE char_id=?", (char_id,)).fetchone()
        if not row:
            return False
        data = json.loads(row["data"])
        data["hotbar"] = hotbar
        conn.execute("UPDATE characters SET data=? WHERE char_id=?",
                     (json.dumps(data, ensure_ascii=False), char_id))
        return True


# ---- существа бестиария (homebrew, по образцу персонажей) ------------------

def create_creature(creature_id: str, room_id: str, data: dict) -> dict:
    """Создаёт homebrew-существо. data — полный статблок; source проставляем 'HB'."""
    now = int(time.time())
    data = dict(data)
    data["source"] = "HB"
    data.setdefault("id", creature_id)
    with connect() as conn:
        conn.execute(
            "INSERT INTO creatures (creature_id, room_id, name, category, disposition, data, created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (creature_id, room_id, data.get("name", "?"), data.get("category"),
             data.get("disposition"), json.dumps(data, ensure_ascii=False), now),
        )
    return data


def update_creature(creature_id: str, data: dict) -> bool:
    """Перезаписывает статблок существа. source остаётся 'HB'. True, если нашли."""
    data = dict(data)
    data["source"] = "HB"
    data.setdefault("id", creature_id)
    with connect() as conn:
        cur = conn.execute(
            "UPDATE creatures SET name=?, category=?, disposition=?, data=? WHERE creature_id=?",
            (data.get("name", "?"), data.get("category"), data.get("disposition"),
             json.dumps(data, ensure_ascii=False), creature_id),
        )
        return cur.rowcount > 0


def delete_creature(creature_id: str) -> bool:
    with connect() as conn:
        cur = conn.execute("DELETE FROM creatures WHERE creature_id=?", (creature_id,))
        return cur.rowcount > 0


def get_creature(creature_id: str) -> dict | None:
    with connect() as conn:
        row = conn.execute("SELECT data FROM creatures WHERE creature_id=?", (creature_id,)).fetchone()
    return json.loads(row["data"]) if row else None


def list_creatures(room_id: str) -> list[dict]:
    """Полные статблоки существ игры (их немного — отдаём целиком, чтобы бестиарий
    рисовал карточки без доп. запросов)."""
    with connect() as conn:
        rows = conn.execute(
            "SELECT data FROM creatures WHERE room_id=? ORDER BY created_at", (room_id,)
        ).fetchall()
    return [json.loads(r["data"]) for r in rows]


# ---- wiki мира: фракции (по образцу персонажей; пока никем не зовётся) ------

def create_faction(faction_id: str, room_id: str, data: dict) -> dict:
    now = int(time.time())
    data = dict(data)
    data.setdefault("id", faction_id)
    with connect() as conn:
        conn.execute(
            "INSERT INTO factions (faction_id, room_id, name, data, created_at) VALUES (?,?,?,?,?)",
            (faction_id, room_id, data.get("name", "?"),
             json.dumps(data, ensure_ascii=False), now),
        )
    return data


def update_faction(faction_id: str, data: dict) -> bool:
    data = dict(data)
    data.setdefault("id", faction_id)
    with connect() as conn:
        cur = conn.execute(
            "UPDATE factions SET name=?, data=? WHERE faction_id=?",
            (data.get("name", "?"), json.dumps(data, ensure_ascii=False), faction_id),
        )
        return cur.rowcount > 0


def delete_faction(faction_id: str) -> bool:
    with connect() as conn:
        cur = conn.execute("DELETE FROM factions WHERE faction_id=?", (faction_id,))
        return cur.rowcount > 0


def get_faction(faction_id: str) -> dict | None:
    with connect() as conn:
        row = conn.execute("SELECT data FROM factions WHERE faction_id=?", (faction_id,)).fetchone()
    return json.loads(row["data"]) if row else None


def list_factions(room_id: str) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT data FROM factions WHERE room_id=? ORDER BY created_at", (room_id,)
        ).fetchall()
    return [json.loads(r["data"]) for r in rows]


# ---- wiki мира: досье (kind 'pc'|'npc', опц. char_id; пока никем не зовётся) -

def create_dossier(dossier_id: str, room_id: str, data: dict) -> dict:
    now = int(time.time())
    data = dict(data)
    data.setdefault("id", dossier_id)
    with connect() as conn:
        conn.execute(
            "INSERT INTO dossiers (dossier_id, room_id, kind, name, char_id, data, created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (dossier_id, room_id, data.get("kind"), data.get("name", "?"),
             data.get("charId"), json.dumps(data, ensure_ascii=False), now),
        )
    return data


def update_dossier(dossier_id: str, data: dict) -> bool:
    data = dict(data)
    data.setdefault("id", dossier_id)
    with connect() as conn:
        cur = conn.execute(
            "UPDATE dossiers SET kind=?, name=?, char_id=?, data=? WHERE dossier_id=?",
            (data.get("kind"), data.get("name", "?"), data.get("charId"),
             json.dumps(data, ensure_ascii=False), dossier_id),
        )
        return cur.rowcount > 0


def delete_dossier(dossier_id: str) -> bool:
    with connect() as conn:
        cur = conn.execute("DELETE FROM dossiers WHERE dossier_id=?", (dossier_id,))
        return cur.rowcount > 0


def get_dossier(dossier_id: str) -> dict | None:
    with connect() as conn:
        row = conn.execute("SELECT data FROM dossiers WHERE dossier_id=?", (dossier_id,)).fetchone()
    return json.loads(row["data"]) if row else None


def list_dossiers(room_id: str) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT data FROM dossiers WHERE room_id=? ORDER BY created_at", (room_id,)
        ).fetchall()
    return [json.loads(r["data"]) for r in rows]


# ---- места (лимит 4+1 проверяет сервер) -----------------------------------

def list_seats(room_id: str) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT seat_no, role, member_id, char_id, display_name FROM seats WHERE room_id=? ORDER BY seat_no",
            (room_id,),
        ).fetchall()
    return [{"seatNo": r["seat_no"], "role": r["role"], "memberId": r["member_id"],
             "charId": r["char_id"], "displayName": r["display_name"]} for r in rows]


def take_seat(room_id: str, seat_no: int, member_id: str, char_id: str | None,
              display_name: str | None = None) -> bool:
    """
    Занять место. Возвращает True при успехе. Сервер вызывает это как источник
    истины — гонку двух игроков за одно место решает атомарный UPDATE с условием
    member_id IS NULL (или это тот же member). Лимит мест уже зашит числом строк.
    display_name — имя игрока, чтобы журнал подписывался им при «голом» входе.
    """
    with connect() as conn:
        cur = conn.execute(
            "UPDATE seats SET member_id=?, char_id=?, display_name=? "
            "WHERE room_id=? AND seat_no=? AND (member_id IS NULL OR member_id=?)",
            (member_id, char_id, display_name, room_id, seat_no, member_id),
        )
        return cur.rowcount > 0


def free_seat(room_id: str, member_id: str):
    with connect() as conn:
        conn.execute("UPDATE seats SET member_id=NULL, char_id=NULL, display_name=NULL "
                     "WHERE room_id=? AND member_id=?", (room_id, member_id))


# ---- ассеты (метаданные; файл лежит на диске data/assets/{room}/{file}) ----

def _asset_dict(r) -> dict:
    return {"assetId": r["asset_id"], "roomId": r["room_id"], "name": r["name"],
            "kind": r["kind"], "path": r["path"], "url": r["url"],
            "w": r["w"], "h": r["h"], "createdAt": r["created_at"]}


def create_asset(asset_id: str, room_id: str, name: str, kind: str,
                 path: str, url: str, w: int | None, h: int | None) -> dict:
    """Запись метаданных ассета (сам файл уже сохранён на диск вызывающим)."""
    now = int(time.time())
    with connect() as conn:
        conn.execute(
            "INSERT INTO assets (asset_id, room_id, name, kind, path, url, w, h, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (asset_id, room_id, name, kind, path, url, w, h, now),
        )
    return {"assetId": asset_id, "roomId": room_id, "name": name, "kind": kind,
            "path": path, "url": url, "w": w, "h": h, "createdAt": now}


def get_asset(asset_id: str) -> dict | None:
    with connect() as conn:
        row = conn.execute("SELECT * FROM assets WHERE asset_id=?", (asset_id,)).fetchone()
    return _asset_dict(row) if row else None


def list_assets(room_id: str, kind: str | None = None) -> list[dict]:
    """Метаданные ассетов мира (для библиотеки карт). Файлы — на диске, тут ссылки."""
    with connect() as conn:
        if kind:
            rows = conn.execute("SELECT * FROM assets WHERE room_id=? AND kind=? ORDER BY created_at DESC",
                                (room_id, kind)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM assets WHERE room_id=? ORDER BY created_at DESC",
                                (room_id,)).fetchall()
    return [_asset_dict(r) for r in rows]


def delete_asset(asset_id: str) -> str | None:
    """Удаляет строку ассета. Возвращает path удалённого файла (чтобы вызывающий
    стёр файл с диска) или None, если записи не было."""
    with connect() as conn:
        row = conn.execute("SELECT path FROM assets WHERE asset_id=?", (asset_id,)).fetchone()
        if not row:
            return None
        conn.execute("DELETE FROM assets WHERE asset_id=?", (asset_id,))
        return row["path"]


if __name__ == "__main__":
    init_db()
    print("db initialized at", DB_PATH)

    # --- самопроверка на ВРЕМЕННОЙ БД (рабочую data/vtt.db не трогаем) ---
    import os
    import tempfile

    _real_path = DB_PATH
    _tmp_dir = tempfile.mkdtemp()
    DB_PATH = Path(_tmp_dir) / "test_vtt.db"   # connect() читает глобальный DB_PATH
    try:
        init_db()
        create_game("room_test", "Test Game", "gm_vlad")

        # ровно 5 мест: 1 gm + 4 player
        seats = list_seats("room_test")
        assert len(seats) == 5, f"ждали 5 мест, получили {len(seats)}"
        assert sum(1 for s in seats if s["role"] == "gm") == 1, "должно быть одно gm-место"
        assert sum(1 for s in seats if s["role"] == "player") == MAX_PLAYER_SEATS, \
            f"должно быть {MAX_PLAYER_SEATS} мест игроков"
        print("OK: создано ровно 5 мест (1 gm + 4 player)")

        # гонка за место: первый занимает, второй на то же место — False
        assert take_seat("room_test", 1, "player_dana", None) is True, "первый должен занять"
        assert take_seat("room_test", 1, "player_max", None) is False, \
            "второй на занятое место должен получить False"
        # тот же игрок повторно на своё место — идемпотентно True
        assert take_seat("room_test", 1, "player_dana", None) is True
        print("OK: занятое место не отдаётся второму игроку (атомарный UPDATE)")

        # персонаж: get_character возвращает ПОЛНЫЙ объект, list — лёгкие метаданные
        char = {
            "id": "char_a1", "name": "Лиэндра",
            "meta": {"class": "Wizard", "level": 5},
            "abilities": {"int": 18},
            "actions": [{"ref": "spell_fireball", "overrides": {"save": {"dc": 15}}}],
        }
        create_character("char_a1", "room_test", char, "player_dana")
        full = get_character("char_a1")
        assert full == char, "get_character должен вернуть полный объект"
        light = list_characters("room_test")
        assert light and "data" not in light[0], "list_characters не должен тащить блоб"
        assert light[0]["class"] == "Wizard" and light[0]["level"] == 5
        print("OK: get_character вернул полный блоб; list_characters — лёгкие метаданные")

        # free_seat освобождает место; после него его может занять другой
        free_seat("room_test", "player_dana")
        seat1 = next(s for s in list_seats("room_test") if s["seatNo"] == 1)
        assert seat1["memberId"] is None, "free_seat должен освободить место"
        assert take_seat("room_test", 1, "player_max", None) is True, \
            "освобождённое место должно занять другому"
        print("OK: free_seat освободил место, его занял другой игрок")

        print("Все проверки пройдены.")
    finally:
        try:
            os.remove(DB_PATH)
        except OSError:
            pass
        os.rmdir(_tmp_dir)
        DB_PATH = _real_path
