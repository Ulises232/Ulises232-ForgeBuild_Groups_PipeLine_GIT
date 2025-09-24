from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional


BRANCH_COLUMNS = [
    "key",
    "branch",
    "group_name",
    "project",
    "created_at",
    "created_by",
    "exists_local",
    "exists_origin",
    "merge_status",
    "diverged",
    "stale_days",
    "last_action",
    "last_updated_at",
    "last_updated_by",
]


ACTIVITY_COLUMNS = [
    "ts",
    "user",
    "group_name",
    "project",
    "branch",
    "action",
    "result",
    "message",
    "branch_key",
]


class BranchHistoryDB:
    """SQLite persistence for branch index and activity log."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    # ------------------------------------------------------------------
    # basic helpers
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            wal_enabled = False
            try:
                result = conn.execute("PRAGMA journal_mode=WAL").fetchone()
                wal_enabled = bool(result and str(result[0]).lower() == "wal")
            except sqlite3.OperationalError:
                wal_enabled = False

            if not wal_enabled:
                conn.execute("PRAGMA journal_mode=DELETE")
                logging.warning(
                    "BranchHistoryDB journal_mode WAL unavailable for %s; falling back to DELETE",
                    self.path,
                )

            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS branches (
                    key TEXT PRIMARY KEY,
                    branch TEXT NOT NULL,
                    group_name TEXT,
                    project TEXT,
                    created_at INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT,
                    exists_local INTEGER NOT NULL DEFAULT 0,
                    exists_origin INTEGER NOT NULL DEFAULT 0,
                    merge_status TEXT,
                    diverged INTEGER,
                    stale_days INTEGER,
                    last_action TEXT,
                    last_updated_at INTEGER NOT NULL DEFAULT 0,
                    last_updated_by TEXT
                );

                CREATE TABLE IF NOT EXISTS activity_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    user TEXT,
                    group_name TEXT,
                    project TEXT,
                    branch TEXT,
                    action TEXT,
                    result TEXT,
                    message TEXT,
                    branch_key TEXT,
                    UNIQUE (ts, user, group_name, project, branch, action, result, message)
                );

                CREATE INDEX IF NOT EXISTS idx_activity_branch_key
                    ON activity_log(branch_key);
                CREATE INDEX IF NOT EXISTS idx_activity_ts
                    ON activity_log(ts DESC);

                CREATE TABLE IF NOT EXISTS sprint_roles (
                    name TEXT PRIMARY KEY,
                    description TEXT
                );

                CREATE TABLE IF NOT EXISTS sprint_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    display_name TEXT,
                    role_name TEXT,
                    created_at INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(role_name) REFERENCES sprint_roles(name)
                        ON UPDATE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_sprint_users_role
                    ON sprint_users(role_name);

                CREATE TABLE IF NOT EXISTS sprints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    version_branch TEXT NOT NULL,
                    group_name TEXT,
                    project_name TEXT,
                    created_at INTEGER NOT NULL DEFAULT 0,
                    created_by INTEGER,
                    updated_at INTEGER NOT NULL DEFAULT 0,
                    updated_by INTEGER,
                    status TEXT NOT NULL DEFAULT 'active',
                    FOREIGN KEY(created_by) REFERENCES sprint_users(id)
                        ON UPDATE CASCADE ON DELETE SET NULL,
                    FOREIGN KEY(updated_by) REFERENCES sprint_users(id)
                        ON UPDATE CASCADE ON DELETE SET NULL
                );

                CREATE INDEX IF NOT EXISTS idx_sprints_group
                    ON sprints(group_name, project_name);

                CREATE TABLE IF NOT EXISTS sprint_cards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT NOT NULL UNIQUE,
                    sprint_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    branch_name TEXT NOT NULL,
                    assignee_id INTEGER,
                    created_at INTEGER NOT NULL DEFAULT 0,
                    created_by INTEGER,
                    updated_at INTEGER NOT NULL DEFAULT 0,
                    updated_by INTEGER,
                    unit_status TEXT NOT NULL DEFAULT 'pending',
                    qa_status TEXT NOT NULL DEFAULT 'pending',
                    unit_checked_at INTEGER,
                    qa_checked_at INTEGER,
                    unit_checked_by INTEGER,
                    qa_checked_by INTEGER,
                    is_qa_branch INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(sprint_id) REFERENCES sprints(id)
                        ON DELETE CASCADE,
                    FOREIGN KEY(assignee_id) REFERENCES sprint_users(id)
                        ON UPDATE CASCADE ON DELETE SET NULL,
                    FOREIGN KEY(created_by) REFERENCES sprint_users(id)
                        ON UPDATE CASCADE ON DELETE SET NULL,
                    FOREIGN KEY(updated_by) REFERENCES sprint_users(id)
                        ON UPDATE CASCADE ON DELETE SET NULL,
                    FOREIGN KEY(unit_checked_by) REFERENCES sprint_users(id)
                        ON UPDATE CASCADE ON DELETE SET NULL,
                    FOREIGN KEY(qa_checked_by) REFERENCES sprint_users(id)
                        ON UPDATE CASCADE ON DELETE SET NULL
                );

                CREATE INDEX IF NOT EXISTS idx_sprint_cards_sprint
                    ON sprint_cards(sprint_id);
                CREATE INDEX IF NOT EXISTS idx_sprint_cards_branch
                    ON sprint_cards(branch_name);
                CREATE INDEX IF NOT EXISTS idx_sprint_cards_assignee
                    ON sprint_cards(assignee_id);
                """
            )

    def connect(self) -> sqlite3.Connection:
        """Public accessor returning a SQLite connection with the proper settings."""

        return self._connect()

    # ------------------------------------------------------------------
    # branches
    def fetch_branches(self, *, filter_origin: bool = False) -> List[dict]:
        query = "SELECT * FROM branches"
        if filter_origin:
            query += " WHERE exists_origin = 1"
        with self._connect() as conn:
            rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]

    def replace_branches(self, records: Iterable[dict]) -> None:
        payload = [self._normalize_branch_payload(rec) for rec in records]
        with self._connect() as conn:
            conn.execute("DELETE FROM branches")
            conn.executemany(
                """
                INSERT INTO branches (
                    key, branch, group_name, project, created_at, created_by,
                    exists_local, exists_origin, merge_status, diverged,
                    stale_days, last_action, last_updated_at, last_updated_by
                ) VALUES (
                    :key, :branch, :group_name, :project, :created_at, :created_by,
                    :exists_local, :exists_origin, :merge_status, :diverged,
                    :stale_days, :last_action, :last_updated_at, :last_updated_by
                )
                """,
                payload,
            )

    def upsert_branch(self, record: dict) -> None:
        payload = self._normalize_branch_payload(record)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO branches (
                    key, branch, group_name, project, created_at, created_by,
                    exists_local, exists_origin, merge_status, diverged,
                    stale_days, last_action, last_updated_at, last_updated_by
                ) VALUES (
                    :key, :branch, :group_name, :project, :created_at, :created_by,
                    :exists_local, :exists_origin, :merge_status, :diverged,
                    :stale_days, :last_action, :last_updated_at, :last_updated_by
                )
                ON CONFLICT(key) DO UPDATE SET
                    branch = excluded.branch,
                    group_name = excluded.group_name,
                    project = excluded.project,
                    created_at = excluded.created_at,
                    created_by = excluded.created_by,
                    exists_local = excluded.exists_local,
                    exists_origin = excluded.exists_origin,
                    merge_status = excluded.merge_status,
                    diverged = excluded.diverged,
                    stale_days = excluded.stale_days,
                    last_action = excluded.last_action,
                    last_updated_at = excluded.last_updated_at,
                    last_updated_by = excluded.last_updated_by
                """,
                payload,
            )

    def delete_branch(self, key: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM branches WHERE key = ?", (key,))

    # ------------------------------------------------------------------
    # activity log
    def fetch_activity(self, *, branch_keys: Optional[Iterable[str]] = None) -> List[dict]:
        sql = "SELECT ts, user, group_name, project, branch, action, result, message, branch_key FROM activity_log"
        params: List[str] = []
        if branch_keys:
            keys = list(dict.fromkeys(branch_keys))
            if keys:
                placeholders = ",".join("?" for _ in keys)
                sql += f" WHERE branch_key IN ({placeholders})"
                params.extend(keys)
        sql += " ORDER BY ts DESC, id DESC"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def append_activity(self, entries: Iterable[dict]) -> None:
        payload = [self._normalize_activity_payload(entry) for entry in entries]
        if not payload:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO activity_log (
                    ts, user, group_name, project, branch, action,
                    result, message, branch_key
                ) VALUES (
                    :ts, :user, :group_name, :project, :branch, :action,
                    :result, :message, :branch_key
                )
                """,
                payload,
            )

    def prune_activity(self, valid_keys: Iterable[str]) -> None:
        keys = list(dict.fromkeys(valid_keys))
        if not keys:
            with self._connect() as conn:
                conn.execute("DELETE FROM activity_log")
            return
        placeholders = ",".join("?" for _ in keys)
        sql = f"DELETE FROM activity_log WHERE branch_key NOT IN ({placeholders})"
        with self._connect() as conn:
            conn.execute(sql, keys)

    # ------------------------------------------------------------------
    # normalization helpers
    def _normalize_branch_payload(self, record: dict) -> Dict[str, Optional[int]]:
        data = {col: record.get(col) for col in BRANCH_COLUMNS}
        data["exists_local"] = 1 if data.get("exists_local") else 0
        data["exists_origin"] = 1 if data.get("exists_origin") else 0
        data["diverged"] = None if data.get("diverged") is None else (1 if data.get("diverged") else 0)
        data["stale_days"] = None if data.get("stale_days") in (None, "") else int(data.get("stale_days") or 0)
        data["created_at"] = int(data.get("created_at") or 0)
        data["last_updated_at"] = int(data.get("last_updated_at") or 0)
        return data

    def _normalize_activity_payload(self, entry: dict) -> Dict[str, Optional[int]]:
        data = {col: entry.get(col) for col in ACTIVITY_COLUMNS}
        data["ts"] = int(data.get("ts") or 0)
        branch_key = entry.get("branch_key")
        if not branch_key:
            group = entry.get("group") or entry.get("group_name") or ""
            project = entry.get("project") or ""
            branch = entry.get("branch") or ""
            branch_key = f"{group}/{project}/{branch}" if any((group, project, branch)) else ""
        data["branch_key"] = branch_key
        data.setdefault("group_name", entry.get("group"))
        return data

