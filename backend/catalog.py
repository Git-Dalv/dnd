"""
Справочник контента (заклинания/предметы) — в памяти, без БД.
Грузится из JSON при старте: data/srd/*.json + data/homebrew/*.json.

Принцип CLAUDE.md: каталог хранит МЕХАНИКУ без персонажа — у бросков нет
modifier, у спасброска dc=null. Персонаж-зависимое подставляется при сборке
(resolve_action / resolve_character) через overrides: к шаблонной notation из
каталога добавляется посчитанный modifier игрока. Слияние rolls — по полю
label, не по индексу.
"""
import copy
import glob
import json
import os
from pathlib import Path

# data/ лежит в корне репо, на уровень выше backend/
DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _strip_comments(value):
    """Рекурсивно убирает служебные _comment*-поля (для отправки клиенту)."""
    if isinstance(value, dict):
        return {k: _strip_comments(v) for k, v in value.items()
                if not k.startswith("_comment")}
    if isinstance(value, list):
        return [_strip_comments(v) for v in value]
    return value


def _merge_rolls(base_rolls, override_rolls):
    """Сливает списки rolls ПО label: к шаблонному броску добавляются поля
    игрока (в т.ч. modifier). Бросок из overrides без пары — добавляется."""
    merged = [copy.deepcopy(r) for r in base_rolls]
    by_label = {r.get("label"): r for r in merged}
    for ov in override_rolls:
        label = ov.get("label")
        target = by_label.get(label)
        if target is not None:
            target.update(copy.deepcopy(ov))   # notation из шаблона + modifier игрока
        else:
            new = copy.deepcopy(ov)
            merged.append(new)
            by_label[label] = new
    return merged


def _deep_merge(base, override):
    """Глубокое слияние override поверх base. Ключ 'rolls' (на любой глубине)
    сливается по label; вложенные словари — рекурсивно; прочее — заменяется."""
    result = copy.deepcopy(base)
    for key, ov_val in override.items():
        cur = result.get(key)
        if key == "rolls" and isinstance(cur, list) and isinstance(ov_val, list):
            result[key] = _merge_rolls(cur, ov_val)
        elif isinstance(cur, dict) and isinstance(ov_val, dict):
            result[key] = _deep_merge(cur, ov_val)
        else:
            result[key] = copy.deepcopy(ov_val)
    return result


class Catalog:
    """Справочник в памяти. spells/items — dict записей по id."""

    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = Path(data_dir)
        self.spells: dict[str, dict] = {}
        self.items: dict[str, dict] = {}

    def load(self) -> "Catalog":
        """Грузит srd → затем homebrew ПОВЕРХ (homebrew может переопределить
        запись по id). Битые/отсутствующие файлы пропускаются."""
        self.spells.clear()
        self.items.clear()
        # порядок важен: srd первым, homebrew последним (перекрывает)
        for section in ("srd", "homebrew"):
            section_dir = self.data_dir / section
            for path in sorted(glob.glob(str(section_dir / "*.json"))):
                name = os.path.basename(path).lower()
                if "spell" in name:
                    target = self.spells
                elif "item" in name:
                    target = self.items
                else:
                    continue
                try:
                    with open(path, encoding="utf-8") as fh:
                        records = json.load(fh)
                except (OSError, json.JSONDecodeError):
                    continue
                if not isinstance(records, list):
                    continue
                for rec in records:
                    rec_id = rec.get("id")
                    if rec_id:
                        target[rec_id] = rec
        return self

    def get(self, ref: str):
        """Запись по id из любого раздела (или None)."""
        return self.spells.get(ref) or self.items.get(ref)

    def snapshot(self) -> dict:
        """Весь каталог для клиента, без _comment-полей."""
        return {
            "spells": [_strip_comments(r) for r in self.spells.values()],
            "items": [_strip_comments(r) for r in self.items.values()],
        }


def resolve_action(catalog: Catalog, action: dict):
    """{ ref, overrides? } → собранный объект каталога с наложенными overrides.
    Действие без ref — инлайн, возвращается как есть (совместимость).
    ref не найден → None (комната не должна падать из-за битой ссылки)."""
    ref = action.get("ref")
    if not ref:
        return copy.deepcopy(action)        # инлайн-действие (старый формат)
    record = catalog.get(ref)
    if record is None:
        return None                          # битый ref — пусть вызывающий решит
    resolved = copy.deepcopy(record)
    overrides = action.get("overrides")
    if overrides:
        resolved = _deep_merge(resolved, overrides)
    return resolved


def resolve_character(catalog: Catalog, character: dict) -> dict:
    """Копия персонажа с резолвнутыми actions. Битые ссылки отбрасываются."""
    result = copy.deepcopy(character)
    resolved_actions = []
    for action in character.get("actions", []):
        built = resolve_action(catalog, action)
        if built is not None:
            resolved_actions.append(built)
    result["actions"] = resolved_actions
    return result


if __name__ == "__main__":
    cat = Catalog().load()
    print(f"Загружено: заклинаний={len(cat.spells)}, предметов={len(cat.items)}, "
          f"всего={len(cat.spells) + len(cat.items)}")

    # --- самопроверка на временной записи (каталог пуст на старте) ---
    cat.spells["spell_test_bolt"] = {
        "id": "spell_test_bolt",
        "name": "Test Bolt",
        "kind": "spell",
        "_comment": "временная запись для проверки resolve_action",
        "save": {"ability": "dex", "dc": None},
        "rolls": [
            {"label": "Attack", "notation": "1d20", "type": "attack"},
            {"label": "Damage", "notation": "2d10", "type": "damage", "damageType": "fire"},
        ],
    }

    action = {
        "ref": "spell_test_bolt",
        "overrides": {
            "rolls": [
                {"label": "Attack", "modifier": 7},
                {"label": "Damage", "modifier": 0},
            ],
            "save": {"dc": 15},
        },
    }
    resolved = resolve_action(cat, action)
    rolls = {r["label"]: r for r in resolved["rolls"]}

    # modifier наложился ПО label, шаблонная notation сохранилась
    assert rolls["Attack"]["modifier"] == 7, "modifier не наложился на Attack"
    assert rolls["Attack"]["notation"] == "1d20", "notation шаблона потеряна"
    assert rolls["Damage"]["modifier"] == 0, "modifier не наложился на Damage"
    assert rolls["Damage"]["notation"] == "2d10", "notation шаблона потеряна"
    assert resolved["save"]["dc"] == 15, "dc спасброска не подставлен"
    print("OK: resolve_action наложил modifier на rolls по label, dc подставлен")

    # битый ref → None, комната не падает
    assert resolve_action(cat, {"ref": "spell_does_not_exist"}) is None
    print("OK: битый ref даёт None")

    # инлайн-действие без ref возвращается как есть
    inline = {"id": "act_inline", "name": "Bite",
              "rolls": [{"label": "Damage", "notation": "1d6", "modifier": 2, "type": "damage"}]}
    assert resolve_action(cat, inline) == inline
    print("OK: инлайн-действие без ref возвращено как есть")

    # snapshot без _comment-полей
    snap = resolve_action(cat, action)
    assert "_comment" not in cat.snapshot()["spells"][0], "_comment не вырезан в snapshot"
    print("OK: snapshot вырезает _comment-поля")

    print("Все проверки пройдены.")
