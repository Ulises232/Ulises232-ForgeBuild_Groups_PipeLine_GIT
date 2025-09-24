from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence


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


ROLE_COLUMNS = [
    "key",
    "display_name",
    "permissions",
    "description",
    "is_system",
    "created_at",
    "created_by",
]


USER_COLUMNS = [
    "username",
    "display_name",
    "email",
    "role_key",
    "is_active",
    "created_at",
    "created_by",
]


SPRINT_COLUMNS = [
    "key",
    "name",
    "version",
    "group_name",
    "project",
    "base_branch",
    "base_branch_key",
    "status",
    "start_date",
    "end_date",
    "description",
    "created_at",
    "created_by",
]


TICKET_COLUMNS = [
    "key",
    "sprint_key",
    "title",
    "description",
    "branch_name",
    "assignee",
    "qa_owner",
    "requires_qa",
    "unit_status",
    "unit_updated_at",
    "unit_updated_by",
    "qa_status",
    "qa_updated_at",
    "qa_updated_by",
    "created_at",
    "created_by",
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

                CREATE TABLE IF NOT EXISTS roles (
                    key TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    permissions TEXT NOT NULL DEFAULT '[]',
                    description TEXT,
                    is_system INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT
                );

                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    display_name TEXT,
                    email TEXT,
                    role_key TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT,
                    FOREIGN KEY(role_key) REFERENCES roles(key)
                );

                CREATE TABLE IF NOT EXISTS sprints (
                    key TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    group_name TEXT,
                    project TEXT,
                    base_branch TEXT NOT NULL,
                    base_branch_key TEXT,
                    status TEXT NOT NULL DEFAULT 'planned',
                    start_date INTEGER,
                    end_date INTEGER,
                    description TEXT,
                    created_at INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT,
                    FOREIGN KEY(base_branch_key) REFERENCES branches(key)
                );

                CREATE TABLE IF NOT EXISTS sprint_tickets (
                    key TEXT PRIMARY KEY,
                    sprint_key TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    branch_name TEXT NOT NULL,
                    assignee TEXT,
                    qa_owner TEXT,
                    requires_qa INTEGER NOT NULL DEFAULT 1,
                    unit_status TEXT NOT NULL DEFAULT 'pending',
                    unit_updated_at INTEGER NOT NULL DEFAULT 0,
                    unit_updated_by TEXT,
                    qa_status TEXT NOT NULL DEFAULT 'pending',
                    qa_updated_at INTEGER NOT NULL DEFAULT 0,
                    qa_updated_by TEXT,
                    created_at INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT,
                    FOREIGN KEY(sprint_key) REFERENCES sprints(key) ON DELETE CASCADE,
                    FOREIGN KEY(assignee) REFERENCES users(username),
                    FOREIGN KEY(qa_owner) REFERENCES users(username)
                );

                CREATE INDEX IF NOT EXISTS idx_users_role
                    ON users(role_key);
                CREATE INDEX IF NOT EXISTS idx_sprints_group_project
                    ON sprints(group_name, project);
                CREATE INDEX IF NOT EXISTS idx_tickets_sprint
                    ON sprint_tickets(sprint_key);
                CREATE INDEX IF NOT EXISTS idx_tickets_assignee
                    ON sprint_tickets(assignee);
                """
            )

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
    # roles
    def fetch_roles(self) -> List[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT key, display_name, permissions, description, is_system, created_at, created_by"
                " FROM roles ORDER BY is_system DESC, display_name"
            ).fetchall()
        return [self._decode_role(dict(row)) for row in rows]

    def get_role(self, key: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT key, display_name, permissions, description, is_system, created_at, created_by"
                " FROM roles WHERE key = ?",
                (key,),
            ).fetchone()
        return self._decode_role(dict(row)) if row else None

    def upsert_role(self, record: dict) -> None:
        payload = self._normalize_role_payload(record)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO roles (
                    key, display_name, permissions, description,
                    is_system, created_at, created_by
                ) VALUES (
                    :key, :display_name, :permissions, :description,
                    :is_system, :created_at, :created_by
                )
                ON CONFLICT(key) DO UPDATE SET
                    display_name = excluded.display_name,
                    permissions = excluded.permissions,
                    description = excluded.description,
                    is_system = excluded.is_system,
                    created_at = CASE WHEN roles.created_at = 0 THEN excluded.created_at ELSE roles.created_at END,
                    created_by = CASE WHEN roles.created_by IS NULL OR roles.created_by = '' THEN excluded.created_by ELSE roles.created_by END
                """,
                payload,
            )

    def delete_role(self, key: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM roles WHERE key = ? AND is_system = 0", (key,))

    # ------------------------------------------------------------------
    # users
    def fetch_users(self, *, active_only: bool = False) -> List[dict]:
        sql = (
            "SELECT username, display_name, email, role_key, is_active, created_at, created_by"
            " FROM users"
        )
        params: Sequence[object] = []
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY username"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._decode_user(dict(row)) for row in rows]

    def get_user(self, username: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT username, display_name, email, role_key, is_active, created_at, created_by"
                " FROM users WHERE username = ?",
                (username,),
            ).fetchone()
        return self._decode_user(dict(row)) if row else None

    def upsert_user(self, record: dict) -> None:
        payload = self._normalize_user_payload(record)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users (
                    username, display_name, email, role_key,
                    is_active, created_at, created_by
                ) VALUES (
                    :username, :display_name, :email, :role_key,
                    :is_active, :created_at, :created_by
                )
                ON CONFLICT(username) DO UPDATE SET
                    display_name = excluded.display_name,
                    email = excluded.email,
                    role_key = excluded.role_key,
                    is_active = excluded.is_active,
                    created_at = CASE WHEN users.created_at = 0 THEN excluded.created_at ELSE users.created_at END,
                    created_by = CASE WHEN users.created_by IS NULL OR users.created_by = '' THEN excluded.created_by ELSE users.created_by END
                """,
                payload,
            )

    def delete_user(self, username: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM users WHERE username = ?", (username,))

    # ------------------------------------------------------------------
    # sprints
    def fetch_sprints(self, *, group: Optional[str] = None, project: Optional[str] = None) -> List[dict]:
        sql = (
            "SELECT key, name, version, group_name, project, base_branch, base_branch_key,"
            " status, start_date, end_date, description, created_at, created_by"
            " FROM sprints"
        )
        params: List[object] = []
        clauses: List[str] = []
        if group:
            clauses.append("group_name = ?")
            params.append(group)
        if project:
            clauses.append("project = ?")
            params.append(project)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC, key"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._decode_sprint(dict(row)) for row in rows]

    def get_sprint(self, key: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT key, name, version, group_name, project, base_branch, base_branch_key,"
                " status, start_date, end_date, description, created_at, created_by"
                " FROM sprints WHERE key = ?",
                (key,),
            ).fetchone()
        return self._decode_sprint(dict(row)) if row else None

    def upsert_sprint(self, record: dict) -> None:
        payload = self._normalize_sprint_payload(record)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO sprints (
                    key, name, version, group_name, project,
                    base_branch, base_branch_key, status,
                    start_date, end_date, description,
                    created_at, created_by
                ) VALUES (
                    :key, :name, :version, :group_name, :project,
                    :base_branch, :base_branch_key, :status,
                    :start_date, :end_date, :description,
                    :created_at, :created_by
                )
                ON CONFLICT(key) DO UPDATE SET
                    name = excluded.name,
                    version = excluded.version,
                    group_name = excluded.group_name,
                    project = excluded.project,
                    base_branch = excluded.base_branch,
                    base_branch_key = excluded.base_branch_key,
                    status = excluded.status,
                    start_date = excluded.start_date,
                    end_date = excluded.end_date,
                    description = excluded.description,
                    created_at = CASE WHEN sprints.created_at = 0 THEN excluded.created_at ELSE sprints.created_at END,
                    created_by = CASE WHEN sprints.created_by IS NULL OR sprints.created_by = '' THEN excluded.created_by ELSE sprints.created_by END
                """,
                payload,
            )

    def delete_sprint(self, key: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM sprints WHERE key = ?", (key,))

    # ------------------------------------------------------------------
    # tickets
    def fetch_tickets(self, *, sprint_keys: Optional[Iterable[str]] = None) -> List[dict]:
        sql = (
            "SELECT key, sprint_key, title, description, branch_name, assignee, qa_owner,"
            " requires_qa, unit_status, unit_updated_at, unit_updated_by,"
            " qa_status, qa_updated_at, qa_updated_by, created_at, created_by"
            " FROM sprint_tickets"
        )
        params: List[object] = []
        if sprint_keys:
            keys = list(dict.fromkeys(sprint_keys))
            if keys:
                placeholders = ",".join("?" for _ in keys)
                sql += f" WHERE sprint_key IN ({placeholders})"
                params.extend(keys)
        sql += " ORDER BY created_at DESC, key"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._decode_ticket(dict(row)) for row in rows]

    def get_ticket(self, key: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT key, sprint_key, title, description, branch_name, assignee, qa_owner,"
                " requires_qa, unit_status, unit_updated_at, unit_updated_by,"
                " qa_status, qa_updated_at, qa_updated_by, created_at, created_by"
                " FROM sprint_tickets WHERE key = ?",
                (key,),
            ).fetchone()
        return self._decode_ticket(dict(row)) if row else None

    def upsert_ticket(self, record: dict) -> None:
        payload = self._normalize_ticket_payload(record)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO sprint_tickets (
                    key, sprint_key, title, description, branch_name,
                    assignee, qa_owner, requires_qa,
                    unit_status, unit_updated_at, unit_updated_by,
                    qa_status, qa_updated_at, qa_updated_by,
                    created_at, created_by
                ) VALUES (
                    :key, :sprint_key, :title, :description, :branch_name,
                    :assignee, :qa_owner, :requires_qa,
                    :unit_status, :unit_updated_at, :unit_updated_by,
                    :qa_status, :qa_updated_at, :qa_updated_by,
                    :created_at, :created_by
                )
                ON CONFLICT(key) DO UPDATE SET
                    sprint_key = excluded.sprint_key,
                    title = excluded.title,
                    description = excluded.description,
                    branch_name = excluded.branch_name,
                    assignee = excluded.assignee,
                    qa_owner = excluded.qa_owner,
                    requires_qa = excluded.requires_qa,
                    unit_status = excluded.unit_status,
                    unit_updated_at = excluded.unit_updated_at,
                    unit_updated_by = excluded.unit_updated_by,
                    qa_status = excluded.qa_status,
                    qa_updated_at = excluded.qa_updated_at,
                    qa_updated_by = excluded.qa_updated_by,
                    created_at = CASE WHEN sprint_tickets.created_at = 0 THEN excluded.created_at ELSE sprint_tickets.created_at END,
                    created_by = CASE WHEN sprint_tickets.created_by IS NULL OR sprint_tickets.created_by = '' THEN excluded.created_by ELSE sprint_tickets.created_by END
                """,
                payload,
            )

    def delete_ticket(self, key: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM sprint_tickets WHERE key = ?", (key,))

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

    def _normalize_role_payload(self, record: dict) -> Dict[str, object]:
        data = {col: record.get(col) for col in ROLE_COLUMNS}
        permissions = record.get("permissions")
        if isinstance(permissions, str):
            try:
                parsed = json.loads(permissions)
            except json.JSONDecodeError:
                parsed = []
        else:
            parsed = list(permissions or [])
        deduped = sorted({str(item) for item in parsed if item})
        data["permissions"] = json.dumps(deduped, ensure_ascii=False)
        data["is_system"] = 1 if record.get("is_system") else 0
        data["created_at"] = int(record.get("created_at") or int(time.time()))
        data["created_by"] = record.get("created_by") or ""
        return data

    def _decode_role(self, record: dict) -> dict:
        raw = dict(record)
        raw["is_system"] = bool(raw.get("is_system"))
        try:
            perms = json.loads(raw.get("permissions") or "[]")
        except json.JSONDecodeError:
            perms = []
        raw["permissions"] = [str(item) for item in perms if item]
        raw["created_at"] = int(raw.get("created_at") or 0)
        return raw

    def _normalize_user_payload(self, record: dict) -> Dict[str, object]:
        data = {col: record.get(col) for col in USER_COLUMNS}
        if not data.get("username"):
            raise ValueError("username is required")
        if not data.get("role_key"):
            raise ValueError("role_key is required")
        data["is_active"] = 1 if record.get("is_active", True) else 0
        data["created_at"] = int(record.get("created_at") or int(time.time()))
        data["created_by"] = record.get("created_by") or ""
        return data

    def _decode_user(self, record: dict) -> dict:
        raw = dict(record)
        raw["is_active"] = bool(raw.get("is_active"))
        raw["created_at"] = int(raw.get("created_at") or 0)
        return raw

    def _normalize_sprint_payload(self, record: dict) -> Dict[str, object]:
        data = {col: record.get(col) for col in SPRINT_COLUMNS}
        if not data.get("key"):
            raise ValueError("sprint key is required")
        if not data.get("name"):
            raise ValueError("sprint name is required")
        if not data.get("version"):
            raise ValueError("version is required")
        data["base_branch"] = record.get("base_branch") or ""
        if not data["base_branch"]:
            raise ValueError("base_branch is required")
        data["status"] = record.get("status") or "planned"
        data["start_date"] = int(record.get("start_date") or 0)
        data["end_date"] = int(record.get("end_date") or 0)
        data["created_at"] = int(record.get("created_at") or int(time.time()))
        data["created_by"] = record.get("created_by") or ""
        return data

    def _decode_sprint(self, record: dict) -> dict:
        raw = dict(record)
        raw["start_date"] = int(raw.get("start_date") or 0)
        raw["end_date"] = int(raw.get("end_date") or 0)
        raw["created_at"] = int(raw.get("created_at") or 0)
        return raw

    def _normalize_ticket_payload(self, record: dict) -> Dict[str, object]:
        data = {col: record.get(col) for col in TICKET_COLUMNS}
        if not data.get("key"):
            raise ValueError("ticket key is required")
        if not data.get("sprint_key"):
            raise ValueError("sprint_key is required")
        if not data.get("title"):
            raise ValueError("ticket title is required")
        data["branch_name"] = record.get("branch_name") or ""
        if not data["branch_name"]:
            raise ValueError("branch_name is required")
        data["requires_qa"] = 1 if record.get("requires_qa", True) else 0
        data["unit_status"] = record.get("unit_status") or "pending"
        data["qa_status"] = record.get("qa_status") or "pending"
        data["unit_updated_at"] = int(record.get("unit_updated_at") or 0)
        data["qa_updated_at"] = int(record.get("qa_updated_at") or 0)
        data["created_at"] = int(record.get("created_at") or int(time.time()))
        data["created_by"] = record.get("created_by") or ""
        return data

    def _decode_ticket(self, record: dict) -> dict:
        raw = dict(record)
        raw["requires_qa"] = bool(raw.get("requires_qa"))
        raw["unit_updated_at"] = int(raw.get("unit_updated_at") or 0)
        raw["qa_updated_at"] = int(raw.get("qa_updated_at") or 0)
        raw["created_at"] = int(raw.get("created_at") or 0)
        return raw

