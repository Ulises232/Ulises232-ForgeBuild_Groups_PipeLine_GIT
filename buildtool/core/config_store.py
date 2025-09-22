from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional, Any, Dict

SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS groups (
    key TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TRIGGER IF NOT EXISTS trg_groups_updated
AFTER UPDATE ON groups
FOR EACH ROW
BEGIN
    UPDATE groups SET updated_at = CURRENT_TIMESTAMP WHERE key = OLD.key;
END;
"""


def _state_dir() -> Path:
    base = os.environ.get("APPDATA")
    if base:
        return Path(base) / "ForgeBuild"
    return Path.home() / ".forgebuild"


def _serialize_model(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "dict"):
        return obj.dict()
    if isinstance(obj, dict):
        return obj
    raise TypeError(f"Cannot serialize object of type {type(obj)!r}")


class ConfigStore:
    """Persistencia de grupos y proyectos en SQLite."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = Path(db_path or (_state_dir() / "config.sqlite3"))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as cx:
            cx.executescript(SCHEMA)

    # ------------------------------------------------------------------
    def is_empty(self) -> bool:
        with sqlite3.connect(self.db_path) as cx:
            cur = cx.execute("SELECT COUNT(*) FROM groups")
            (count,) = cur.fetchone()
        return int(count) == 0

    # ------------------------------------------------------------------
    def replace_groups(self, groups: Iterable[Any]) -> None:
        payload: List[tuple[str, str]] = []
        for group in groups:
            if group is None:
                continue
            data = _serialize_model(group)
            key = data.get("key")
            if not key:
                continue
            payload.append((str(key), json.dumps(data, ensure_ascii=False)))

        with sqlite3.connect(self.db_path) as cx:
            cx.execute("DELETE FROM groups")
            if payload:
                cx.executemany(
                    "INSERT INTO groups(key, data) VALUES(?, ?)",
                    payload,
                )
            cx.commit()

    # ------------------------------------------------------------------
    def list_groups(self) -> List[Any]:
        from .config import Group  # import diferido para evitar ciclos

        with sqlite3.connect(self.db_path) as cx:
            rows = cx.execute(
                "SELECT data FROM groups ORDER BY key COLLATE NOCASE"
            ).fetchall()
        groups: List[Any] = []
        for (raw,) in rows:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            try:
                groups.append(Group(**data))
            except Exception:
                continue
        return groups

    # ------------------------------------------------------------------
    def update_group(self, group: Any) -> None:
        data = _serialize_model(group)
        key = data.get("key")
        if not key:
            raise ValueError("El grupo debe tener un 'key' válido")
        payload = json.dumps(data, ensure_ascii=False)
        with sqlite3.connect(self.db_path) as cx:
            cx.execute(
                "INSERT INTO groups(key, data) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET data = excluded.data",
                (str(key), payload),
            )
            cx.commit()

    # ------------------------------------------------------------------
    def delete_group(self, key: str) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("DELETE FROM groups WHERE key = ?", (key,))
            cx.commit()

    # ------------------------------------------------------------------
    def load_metadata(self, key: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as cx:
            cur = cx.execute("SELECT value FROM metadata WHERE key = ?", (key,))
            row = cur.fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------
    def save_metadata(self, key: str, value: str) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute(
                "INSERT INTO metadata(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            cx.commit()


__all__ = ["ConfigStore"]
