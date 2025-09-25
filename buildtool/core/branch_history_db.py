from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:  # pragma: no cover - optional dependency for server backends
    from sqlalchemy import Engine, create_engine, text
    from sqlalchemy.exc import IntegrityError
except Exception:  # pragma: no cover - keep optional for pure SQLite installs
    Engine = None  # type: ignore
    create_engine = None  # type: ignore
    text = None  # type: ignore
    IntegrityError = Exception  # type: ignore


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


SPRINT_COLUMNS = [
    "id",
    "branch_key",
    "qa_branch_key",
    "name",
    "version",
    "lead_user",
    "qa_user",
    "description",
    "status",
    "closed_at",
    "closed_by",
    "created_at",
    "created_by",
    "updated_at",
    "updated_by",
]


CARD_COLUMNS = [
    "id",
    "sprint_id",
    "branch_key",
    "title",
    "ticket_id",
    "branch",
    "assignee",
    "qa_assignee",
    "description",
    "unit_tests_url",
    "qa_url",
    "unit_tests_done",
    "qa_done",
    "unit_tests_by",
    "qa_by",
    "unit_tests_at",
    "qa_at",
    "status",
    "branch_created_by",
    "branch_created_at",
    "created_at",
    "created_by",
    "updated_at",
    "updated_by",
]


SPRINT_TABLE_TEMPLATE = """
CREATE TABLE {if_not_exists}{table} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_key TEXT NOT NULL DEFAULT '',
    qa_branch_key TEXT,
    name TEXT NOT NULL DEFAULT '',
    version TEXT NOT NULL DEFAULT '',
    lead_user TEXT,
    qa_user TEXT,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    closed_at INTEGER,
    closed_by TEXT,
    created_at INTEGER NOT NULL DEFAULT 0,
    created_by TEXT,
    updated_at INTEGER NOT NULL DEFAULT 0,
    updated_by TEXT
);
"""


CARD_TABLE_TEMPLATE = """
CREATE TABLE {if_not_exists}{table} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sprint_id INTEGER NOT NULL,
    branch_key TEXT,
    title TEXT NOT NULL DEFAULT '',
    ticket_id TEXT,
    branch TEXT NOT NULL DEFAULT '',
    assignee TEXT,
    qa_assignee TEXT,
    description TEXT,
    unit_tests_url TEXT,
    qa_url TEXT,
    unit_tests_done INTEGER NOT NULL DEFAULT 0,
    qa_done INTEGER NOT NULL DEFAULT 0,
    unit_tests_by TEXT,
    qa_by TEXT,
    unit_tests_at INTEGER,
    qa_at INTEGER,
    status TEXT DEFAULT 'pending',
    branch_created_by TEXT,
    branch_created_at INTEGER,
    created_at INTEGER NOT NULL DEFAULT 0,
    created_by TEXT,
    updated_at INTEGER NOT NULL DEFAULT 0,
    updated_by TEXT,
    FOREIGN KEY(sprint_id) REFERENCES sprints(id) ON DELETE CASCADE
);
"""


@dataclass(slots=True)
class Sprint:
    """Model representing a sprint/version planning entry."""

    id: Optional[int]
    branch_key: str
    name: str
    version: str
    qa_branch_key: Optional[str] = None
    lead_user: Optional[str] = None
    qa_user: Optional[str] = None
    description: str = ""
    status: str = "open"
    closed_at: Optional[int] = None
    closed_by: Optional[str] = None
    created_at: int = 0
    created_by: str = ""
    updated_at: int = 0
    updated_by: str = ""


@dataclass(slots=True)
class Card:
    """Model representing a work card tied to a sprint."""

    id: Optional[int]
    sprint_id: int
    branch_key: Optional[str] = None
    title: str = ""
    ticket_id: str = ""
    branch: str = ""
    assignee: Optional[str] = None
    qa_assignee: Optional[str] = None
    description: str = ""
    unit_tests_url: Optional[str] = None
    qa_url: Optional[str] = None
    unit_tests_done: bool = False
    qa_done: bool = False
    unit_tests_by: Optional[str] = None
    qa_by: Optional[str] = None
    unit_tests_at: Optional[int] = None
    qa_at: Optional[int] = None
    status: str = "pending"
    branch_created_by: Optional[str] = None
    branch_created_at: Optional[int] = None
    created_at: int = 0
    created_by: str = ""
    updated_at: int = 0
    updated_by: str = ""


@dataclass(slots=True)
class User:
    """Application level user."""

    username: str
    display_name: str
    active: bool = True
    email: Optional[str] = None


@dataclass(slots=True)
class Role:
    """Role that can be assigned to users."""

    key: str
    name: str
    description: str = ""


class _NormalizerMixin:
    """Shared normalization helpers for both SQLite and SQL Server backends."""

    @staticmethod
    def _normalize_branch_payload(record: dict) -> Dict[str, Optional[int]]:
        data = {col: record.get(col) for col in BRANCH_COLUMNS}
        data["exists_local"] = 1 if data.get("exists_local") else 0
        data["exists_origin"] = 1 if data.get("exists_origin") else 0
        data["diverged"] = (
            None if data.get("diverged") is None else (1 if data.get("diverged") else 0)
        )
        data["stale_days"] = (
            None if data.get("stale_days") in (None, "") else int(data.get("stale_days") or 0)
        )
        data["created_at"] = int(data.get("created_at") or 0)
        data["last_updated_at"] = int(data.get("last_updated_at") or 0)
        return data

    @staticmethod
    def _normalize_activity_payload(entry: dict) -> Dict[str, Optional[int]]:
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

    @staticmethod
    def _normalize_sprint(payload: dict) -> Dict[str, object]:
        data = {
            "id": payload.get("id"),
            "branch_key": payload.get("branch_key") or "",
            "qa_branch_key": payload.get("qa_branch_key") or None,
            "name": payload.get("name") or "",
            "version": payload.get("version") or "",
            "lead_user": payload.get("lead_user"),
            "qa_user": payload.get("qa_user"),
            "description": payload.get("description") or "",
            "status": (payload.get("status") or "open").lower(),
            "closed_at": int(payload.get("closed_at") or 0) or None,
            "closed_by": payload.get("closed_by") or None,
            "created_at": int(payload.get("created_at") or 0),
            "created_by": payload.get("created_by") or "",
            "updated_at": int(payload.get("updated_at") or 0),
            "updated_by": payload.get("updated_by") or "",
        }
        if data["id"] in ("", None):
            data["id"] = None
        if isinstance(data["qa_branch_key"], str):
            data["qa_branch_key"] = data["qa_branch_key"].strip() or None
        if data["qa_branch_key"] in ("", None):
            data["qa_branch_key"] = None
        return data

    @staticmethod
    def _normalize_card(payload: dict) -> Dict[str, object]:
        data = {
            "id": payload.get("id"),
            "sprint_id": int(payload.get("sprint_id") or 0),
            "branch_key": payload.get("branch_key"),
            "title": payload.get("title") or "",
            "ticket_id": payload.get("ticket_id") or "",
            "branch": payload.get("branch") or "",
            "assignee": payload.get("assignee"),
            "qa_assignee": payload.get("qa_assignee"),
            "description": payload.get("description") or "",
            "unit_tests_url": (payload.get("unit_tests_url") or "").strip() or None,
            "qa_url": (payload.get("qa_url") or "").strip() or None,
            "unit_tests_done": 1 if payload.get("unit_tests_done") else 0,
            "qa_done": 1 if payload.get("qa_done") else 0,
            "unit_tests_by": payload.get("unit_tests_by"),
            "qa_by": payload.get("qa_by"),
            "unit_tests_at": int(payload.get("unit_tests_at") or 0) or None,
            "qa_at": int(payload.get("qa_at") or 0) or None,
            "status": payload.get("status") or "pending",
            "branch_created_by": payload.get("branch_created_by"),
            "branch_created_at": int(payload.get("branch_created_at") or 0) or None,
            "created_at": int(payload.get("created_at") or 0),
            "created_by": payload.get("created_by") or "",
            "updated_at": int(payload.get("updated_at") or 0),
            "updated_by": payload.get("updated_by") or "",
        }
        if data["id"] in ("", None):
            data["id"] = None
        return data

    @staticmethod
    def _normalize_user(payload: dict) -> Dict[str, object]:
        return {
            "username": payload.get("username") or "",
            "display_name": payload.get("display_name") or payload.get("username") or "",
            "email": payload.get("email"),
            "active": 1 if payload.get("active", True) else 0,
        }

    @staticmethod
    def _normalize_role(payload: dict) -> Dict[str, object]:
        return {
            "key": payload.get("key") or "",
            "name": payload.get("name") or payload.get("key") or "",
            "description": payload.get("description") or "",
        }


class _SQLiteBranchHistoryBackend(_NormalizerMixin):
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

            self._apply_migrations(conn)

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

                {sprint_table}

                {card_table}
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    email TEXT,
                    active INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS roles (
                    key TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT
                );

                CREATE TABLE IF NOT EXISTS user_roles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    role_key TEXT NOT NULL,
                    UNIQUE(username, role_key),
                    FOREIGN KEY(username) REFERENCES users(username) ON DELETE CASCADE,
                    FOREIGN KEY(role_key) REFERENCES roles(key) ON DELETE CASCADE
                );
                """.format(
                    sprint_table=SPRINT_TABLE_TEMPLATE.format(
                        if_not_exists="IF NOT EXISTS ", table="sprints"
                    ),
                    card_table=CARD_TABLE_TEMPLATE.format(
                        if_not_exists="IF NOT EXISTS ", table="cards"
                    ),
                )
            )

            self._ensure_indexes(conn)

    def _apply_migrations(self, conn: sqlite3.Connection) -> None:
        self._ensure_activity_log_branch_key(conn)
        self._ensure_sprints_schema(conn)
        self._ensure_cards_schema(conn)

    def _ensure_indexes(self, conn: sqlite3.Connection) -> None:
        """Create or rebuild indexes that may rely on migrated columns."""

        # activity_log indexes depend on the branch_key column being present on
        # legacy installations, so we guard them explicitly instead of relying
        # on the shared DDL script.
        activity_columns = self._table_columns(conn, "activity_log")
        if "branch_key" in activity_columns:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_activity_branch_key ON activity_log(branch_key)"
            )

        if activity_columns:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_activity_ts ON activity_log(ts DESC)"
            )

        # The remaining tables always exist with the required columns when the
        # script above runs, but `CREATE INDEX IF NOT EXISTS` keeps the calls
        # idempotent for repeated initialisations.
        sprints_columns = self._table_columns(conn, "sprints")
        if "branch_key" in sprints_columns:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sprints_branch ON sprints(branch_key)"
            )

        cards_columns = self._table_columns(conn, "cards")
        if "sprint_id" in cards_columns:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cards_sprint ON cards(sprint_id)")
        if "branch" in cards_columns:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cards_branch ON cards(branch)")

    def _ensure_activity_log_branch_key(self, conn: sqlite3.Connection) -> None:
        columns = self._table_columns(conn, "activity_log")
        if not columns:
            return
        if "branch_key" in columns:
            return
        conn.execute("ALTER TABLE activity_log ADD COLUMN branch_key TEXT")
        conn.execute(
            """
            UPDATE activity_log
               SET branch_key = CASE
                   WHEN COALESCE(group_name, '') || COALESCE(project, '') || COALESCE(branch, '') = ''
                       THEN ''
                   ELSE COALESCE(group_name, '') || '/' || COALESCE(project, '') || '/' || COALESCE(branch, '')
               END
             WHERE branch_key IS NULL OR branch_key = ''
            """
        )

    def _ensure_sprints_schema(self, conn: sqlite3.Connection) -> None:
        columns = self._table_columns(conn, "sprints")
        if not columns:
            return
        if set(SPRINT_COLUMNS).issubset(columns):
            return
        defaults = {
            "branch_key": "''",
            "qa_branch_key": "NULL",
            "name": "''",
            "version": "''",
            "lead_user": "NULL",
            "qa_user": "NULL",
            "description": "''",
            "status": "'open'",
            "closed_at": "NULL",
            "closed_by": "NULL",
            "created_at": "0",
            "created_by": "''",
            "updated_at": "0",
            "updated_by": "''",
        }
        self._rebuild_table(
            conn,
            "sprints",
            SPRINT_TABLE_TEMPLATE,
            SPRINT_COLUMNS,
            defaults,
        )

    def _ensure_cards_schema(self, conn: sqlite3.Connection) -> None:
        columns = self._table_columns(conn, "cards")
        if not columns:
            return
        if set(CARD_COLUMNS).issubset(columns):
            return
        defaults = {
            "branch_key": "NULL",
            "assignee": "NULL",
            "qa_assignee": "NULL",
            "description": "''",
            "unit_tests_url": "NULL",
            "qa_url": "NULL",
            "unit_tests_done": "0",
            "qa_done": "0",
            "unit_tests_by": "NULL",
            "qa_by": "NULL",
            "unit_tests_at": "NULL",
            "qa_at": "NULL",
            "status": "'pending'",
            "ticket_id": "NULL",
            "branch_created_by": "NULL",
            "branch_created_at": "NULL",
            "created_at": "0",
            "created_by": "''",
            "updated_at": "0",
            "updated_by": "''",
        }
        self._rebuild_table(
            conn,
            "cards",
            CARD_TABLE_TEMPLATE,
            CARD_COLUMNS,
            defaults,
        )

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> set[str]:
        try:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        except sqlite3.OperationalError:
            return set()
        return {str(row[1]) for row in rows}

    def _rebuild_table(
        self,
        conn: sqlite3.Connection,
        table: str,
        template: str,
        expected_columns: Sequence[str],
        defaults: Dict[str, str],
    ) -> None:
        existing_columns = self._table_columns(conn, table)
        if not existing_columns:
            return
        temp_name = f"__{table}_new"
        conn.execute("PRAGMA foreign_keys = OFF")
        try:
            conn.execute(f"DROP TABLE IF EXISTS {temp_name}")
            conn.executescript(
                template.format(if_not_exists="", table=temp_name)
            )
            dest_cols: List[str] = []
            select_cols: List[str] = []
            for col in expected_columns:
                dest_cols.append(col)
                if col in existing_columns:
                    select_cols.append(col)
                else:
                    select_cols.append(defaults.get(col, "NULL"))
            conn.execute(
                f"INSERT INTO {temp_name} ({', '.join(dest_cols)}) "
                f"SELECT {', '.join(select_cols)} FROM {table}"
            )
            conn.execute(f"DROP TABLE {table}")
            conn.execute(f"ALTER TABLE {temp_name} RENAME TO {table}")
        finally:
            conn.execute("PRAGMA foreign_keys = ON")

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

    # ------------------------------------------------------------------
    # sprints & cards
    def fetch_sprints(self, *, branch_keys: Optional[Sequence[str]] = None) -> List[dict]:
        sql = "SELECT * FROM sprints"
        params: List[str] = []
        if branch_keys:
            keys = [key for key in branch_keys if key]
            if keys:
                placeholders = ",".join("?" for _ in keys)
                sql += f" WHERE branch_key IN ({placeholders})"
                params.extend(keys)
        sql += " ORDER BY created_at DESC, id DESC"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def fetch_sprint(self, sprint_id: int) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM sprints WHERE id = ?",
                (int(sprint_id),),
            ).fetchone()
        return dict(row) if row else None

    def fetch_sprint_by_branch_key(self, branch_key: str) -> Optional[dict]:
        key = (branch_key or "").strip()
        if not key:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM sprints WHERE branch_key = ? OR qa_branch_key = ?",
                (key, key),
            ).fetchone()
        return dict(row) if row else None

    def upsert_sprint(self, payload: dict) -> int:
        data = self._normalize_sprint(payload)
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sprints (
                    id, branch_key, qa_branch_key, name, version, lead_user, qa_user, description,
                    status, closed_at, closed_by, created_at, created_by, updated_at, updated_by
                ) VALUES (
                    :id, :branch_key, :qa_branch_key, :name, :version, :lead_user, :qa_user, :description,
                    :status, :closed_at, :closed_by, :created_at, :created_by, :updated_at, :updated_by
                )
                ON CONFLICT(id) DO UPDATE SET
                    branch_key = excluded.branch_key,
                    qa_branch_key = excluded.qa_branch_key,
                    name = excluded.name,
                    version = excluded.version,
                    lead_user = excluded.lead_user,
                    qa_user = excluded.qa_user,
                    description = excluded.description,
                    status = excluded.status,
                    closed_at = excluded.closed_at,
                    closed_by = excluded.closed_by,
                    created_at = excluded.created_at,
                    created_by = excluded.created_by,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
                """,
                data,
            )
            if data.get("id"):
                return int(data["id"])
            return int(cursor.lastrowid)

    def delete_sprint(self, sprint_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM sprints WHERE id = ?", (int(sprint_id),))

    def fetch_cards(
        self,
        *,
        sprint_ids: Optional[Sequence[int]] = None,
        branches: Optional[Sequence[str]] = None,
    ) -> List[dict]:
        sql = "SELECT * FROM cards"
        params: List[object] = []
        clauses: List[str] = []
        if sprint_ids:
            ids = [int(x) for x in sprint_ids if x is not None]
            if ids:
                placeholders = ",".join("?" for _ in ids)
                clauses.append(f"sprint_id IN ({placeholders})")
                params.extend(ids)
        if branches:
            names = [b for b in branches if b]
            if names:
                placeholders = ",".join("?" for _ in names)
                clauses.append(f"branch IN ({placeholders})")
                params.extend(names)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def upsert_card(self, payload: dict) -> int:
        data = self._normalize_card(payload)
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO cards (
                    id, sprint_id, branch_key, title, ticket_id, branch, assignee, qa_assignee, description,
                    unit_tests_url, qa_url, unit_tests_done, qa_done, unit_tests_by, qa_by, unit_tests_at, qa_at, status,
                    branch_created_by, branch_created_at, created_at, created_by, updated_at, updated_by
                ) VALUES (
                    :id, :sprint_id, :branch_key, :title, :ticket_id, :branch, :assignee, :qa_assignee, :description,
                    :unit_tests_url, :qa_url, :unit_tests_done, :qa_done, :unit_tests_by, :qa_by, :unit_tests_at, :qa_at, :status,
                    :branch_created_by, :branch_created_at, :created_at, :created_by, :updated_at, :updated_by
                )
                ON CONFLICT(id) DO UPDATE SET
                    sprint_id = excluded.sprint_id,
                    branch_key = excluded.branch_key,
                    title = excluded.title,
                    ticket_id = excluded.ticket_id,
                    branch = excluded.branch,
                    assignee = excluded.assignee,
                    qa_assignee = excluded.qa_assignee,
                    description = excluded.description,
                    unit_tests_url = excluded.unit_tests_url,
                    qa_url = excluded.qa_url,
                    unit_tests_done = excluded.unit_tests_done,
                    qa_done = excluded.qa_done,
                    unit_tests_by = excluded.unit_tests_by,
                    qa_by = excluded.qa_by,
                    unit_tests_at = excluded.unit_tests_at,
                    qa_at = excluded.qa_at,
                    status = excluded.status,
                    branch_created_by = excluded.branch_created_by,
                    branch_created_at = excluded.branch_created_at,
                    created_at = excluded.created_at,
                    created_by = excluded.created_by,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
                """,
                data,
            )
            if data.get("id"):
                return int(data["id"])
            return int(cursor.lastrowid)

    def delete_card(self, card_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM cards WHERE id = ?", (int(card_id),))

    # ------------------------------------------------------------------
    # users & roles
    def fetch_users(self) -> List[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT username, display_name, email, active FROM users ORDER BY display_name"
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_user(self, payload: dict) -> None:
        data = self._normalize_user(payload)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users (username, display_name, email, active)
                VALUES (:username, :display_name, :email, :active)
                ON CONFLICT(username) DO UPDATE SET
                    display_name = excluded.display_name,
                    email = excluded.email,
                    active = excluded.active
                """,
                data,
            )

    def delete_user(self, username: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM users WHERE username = ?", (username,))

    def fetch_roles(self) -> List[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT key, name, description FROM roles ORDER BY name"
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_role(self, payload: dict) -> None:
        data = self._normalize_role(payload)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO roles (key, name, description)
                VALUES (:key, :name, :description)
                ON CONFLICT(key) DO UPDATE SET
                    name = excluded.name,
                    description = excluded.description
                """,
                data,
            )

    def delete_role(self, role_key: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM roles WHERE key = ?", (role_key,))

    def fetch_user_roles(self, username: Optional[str] = None) -> List[dict]:
        sql = "SELECT username, role_key FROM user_roles"
        params: List[str] = []
        if username:
            sql += " WHERE username = ?"
            params.append(username)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def set_user_roles(self, username: str, roles: Sequence[str]) -> None:
        normalized = [(username, role) for role in roles if role]
        with self._connect() as conn:
            conn.execute("DELETE FROM user_roles WHERE username = ?", (username,))
            if normalized:
                conn.executemany(
                    "INSERT OR IGNORE INTO user_roles (username, role_key) VALUES (?, ?)",
                    normalized,
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


class _SQLServerBranchHistoryBackend(_NormalizerMixin):
    """Persistence backend backed by SQL Server using SQLAlchemy."""

    def __init__(self, connection_url: str):
        if not create_engine or not text:
            raise RuntimeError(
                "El backend SQL Server requiere SQLAlchemy y un driver compatible "
                "con SQL Server instalado (por ejemplo, pyodbc, python-tds o pymssql)."
            )
        self.connection_url = connection_url
        self._engine: Engine = create_engine(
            connection_url,
            future=True,
            pool_pre_ping=True,
        )
        self._ensure_schema()

    # ------------------------------------------------------------------
    # helpers
    @staticmethod
    def _quote(identifier: str) -> str:
        return f"[{identifier}]"

    @contextmanager
    def _connect(self):
        with self._engine.begin() as conn:
            yield conn

    def _ensure_schema(self) -> None:
        statements = [
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'branches')
            BEGIN
                CREATE TABLE branches (
                    [key] NVARCHAR(255) NOT NULL PRIMARY KEY,
                    branch NVARCHAR(255) NOT NULL,
                    group_name NVARCHAR(255) NULL,
                    project NVARCHAR(255) NULL,
                    created_at BIGINT NOT NULL DEFAULT 0,
                    created_by NVARCHAR(255) NULL,
                    exists_local BIT NOT NULL DEFAULT 0,
                    exists_origin BIT NOT NULL DEFAULT 0,
                    merge_status NVARCHAR(255) NULL,
                    diverged BIT NULL,
                    stale_days INT NULL,
                    last_action NVARCHAR(255) NULL,
                    last_updated_at BIGINT NOT NULL DEFAULT 0,
                    last_updated_by NVARCHAR(255) NULL
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'activity_log')
            BEGIN
                CREATE TABLE activity_log (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    ts BIGINT NOT NULL,
                    [user] NVARCHAR(255) NULL,
                    group_name NVARCHAR(255) NULL,
                    project NVARCHAR(255) NULL,
                    branch NVARCHAR(255) NULL,
                    action NVARCHAR(255) NULL,
                    result NVARCHAR(255) NULL,
                    message NVARCHAR(MAX) NULL,
                    branch_key NVARCHAR(512) NULL,
                    CONSTRAINT uq_activity_log UNIQUE(ts, [user], group_name, project, branch, action, result, message)
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'sprints')
            BEGIN
                CREATE TABLE sprints (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    branch_key NVARCHAR(512) NOT NULL DEFAULT '',
                    qa_branch_key NVARCHAR(512) NULL,
                    name NVARCHAR(255) NOT NULL DEFAULT '',
                    version NVARCHAR(255) NOT NULL DEFAULT '',
                    lead_user NVARCHAR(255) NULL,
                    qa_user NVARCHAR(255) NULL,
                    description NVARCHAR(MAX) NULL,
                    status NVARCHAR(32) NOT NULL DEFAULT 'open',
                    closed_at BIGINT NULL,
                    closed_by NVARCHAR(255) NULL,
                    created_at BIGINT NOT NULL DEFAULT 0,
                    created_by NVARCHAR(255) NULL,
                    updated_at BIGINT NOT NULL DEFAULT 0,
                    updated_by NVARCHAR(255) NULL
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'cards')
            BEGIN
                CREATE TABLE cards (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    sprint_id INT NOT NULL,
                    branch_key NVARCHAR(512) NULL,
                    title NVARCHAR(255) NOT NULL DEFAULT '',
                    ticket_id NVARCHAR(255) NULL,
                    branch NVARCHAR(255) NOT NULL DEFAULT '',
                    assignee NVARCHAR(255) NULL,
                    qa_assignee NVARCHAR(255) NULL,
                    description NVARCHAR(MAX) NULL,
                    unit_tests_url NVARCHAR(512) NULL,
                    qa_url NVARCHAR(512) NULL,
                    unit_tests_done BIT NOT NULL DEFAULT 0,
                    qa_done BIT NOT NULL DEFAULT 0,
                    unit_tests_by NVARCHAR(255) NULL,
                    qa_by NVARCHAR(255) NULL,
                    unit_tests_at BIGINT NULL,
                    qa_at BIGINT NULL,
                    status NVARCHAR(32) NULL DEFAULT 'pending',
                    branch_created_by NVARCHAR(255) NULL,
                    branch_created_at BIGINT NULL,
                    created_at BIGINT NOT NULL DEFAULT 0,
                    created_by NVARCHAR(255) NULL,
                    updated_at BIGINT NOT NULL DEFAULT 0,
                    updated_by NVARCHAR(255) NULL,
                    CONSTRAINT fk_cards_sprint FOREIGN KEY (sprint_id) REFERENCES sprints(id) ON DELETE CASCADE
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'users')
            BEGIN
                CREATE TABLE users (
                    username NVARCHAR(255) NOT NULL PRIMARY KEY,
                    display_name NVARCHAR(255) NOT NULL,
                    email NVARCHAR(255) NULL,
                    active BIT NOT NULL DEFAULT 1
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'roles')
            BEGIN
                CREATE TABLE roles (
                    [key] NVARCHAR(255) NOT NULL PRIMARY KEY,
                    name NVARCHAR(255) NOT NULL,
                    description NVARCHAR(MAX) NULL
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'user_roles')
            BEGIN
                CREATE TABLE user_roles (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    username NVARCHAR(255) NOT NULL,
                    role_key NVARCHAR(255) NOT NULL,
                    CONSTRAINT uq_user_roles UNIQUE(username, role_key),
                    CONSTRAINT fk_user_roles_user FOREIGN KEY (username) REFERENCES users(username) ON DELETE CASCADE,
                    CONSTRAINT fk_user_roles_role FOREIGN KEY (role_key) REFERENCES roles([key]) ON DELETE CASCADE
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_activity_branch_key')
                CREATE INDEX idx_activity_branch_key ON activity_log(branch_key);
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_activity_ts')
                CREATE INDEX idx_activity_ts ON activity_log(ts DESC);
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_sprints_branch')
                CREATE INDEX idx_sprints_branch ON sprints(branch_key);
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_cards_sprint')
                CREATE INDEX idx_cards_sprint ON cards(sprint_id);
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_cards_branch')
                CREATE INDEX idx_cards_branch ON cards(branch);
            """,
        ]
        with self._engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))

    def _fetch_all(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[dict]:
        params = params or {}
        with self._connect() as conn:
            result = conn.execute(text(sql), params)
            return [dict(row._mapping) for row in result]

    def _execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        params = params or {}
        with self._connect() as conn:
            conn.execute(text(sql), params)

    # ------------------------------------------------------------------
    # branches
    def fetch_branches(self, *, filter_origin: Optional[bool] = None) -> List[dict]:
        sql = "SELECT [key] AS key, branch, group_name, project, created_at, created_by, exists_local, exists_origin, merge_status, diverged, stale_days, last_action, last_updated_at, last_updated_by FROM branches"
        params: Dict[str, Any] = {}
        if filter_origin is not None:
            sql += " WHERE exists_origin = :origin"
            params["origin"] = 1 if filter_origin else 0
        sql += " ORDER BY [key]"
        return self._fetch_all(sql, params)

    def replace_branches(self, records: Iterable[dict]) -> None:
        payload = [self._normalize_branch_payload(record) for record in records]
        if not payload:
            return
        update_sql = text(
            """
            UPDATE branches
               SET branch = :branch,
                   group_name = :group_name,
                   project = :project,
                   created_at = :created_at,
                   created_by = :created_by,
                   exists_local = :exists_local,
                   exists_origin = :exists_origin,
                   merge_status = :merge_status,
                   diverged = :diverged,
                   stale_days = :stale_days,
                   last_action = :last_action,
                   last_updated_at = :last_updated_at,
                   last_updated_by = :last_updated_by
             WHERE [key] = :key
            """
        )
        insert_sql = text(
            """
            INSERT INTO branches (
                [key], branch, group_name, project, created_at, created_by,
                exists_local, exists_origin, merge_status, diverged, stale_days,
                last_action, last_updated_at, last_updated_by
            ) VALUES (
                :key, :branch, :group_name, :project, :created_at, :created_by,
                :exists_local, :exists_origin, :merge_status, :diverged, :stale_days,
                :last_action, :last_updated_at, :last_updated_by
            )
            """
        )
        with self._connect() as conn:
            for row in payload:
                result = conn.execute(update_sql, row)
                if result.rowcount:
                    continue
                conn.execute(insert_sql, row)

    def delete_branch(self, key: str) -> None:
        self._execute("DELETE FROM branches WHERE [key] = :key", {"key": key})

    # ------------------------------------------------------------------
    # activity log
    def fetch_activity(self, *, branch_keys: Optional[Iterable[str]] = None) -> List[dict]:
        sql = (
            "SELECT ts, [user] AS user, group_name, project, branch, action, result, message, branch_key "
            "FROM activity_log"
        )
        params: Dict[str, Any] = {}
        if branch_keys:
            keys = [key for key in dict.fromkeys(branch_keys) if key]
            if keys:
                clauses = ", ".join(f":bk{i}" for i in range(len(keys)))
                sql += f" WHERE branch_key IN ({clauses})"
                params.update({f"bk{i}": value for i, value in enumerate(keys)})
        sql += " ORDER BY ts DESC, id DESC"
        return self._fetch_all(sql, params)

    def append_activity(self, entries: Iterable[dict]) -> None:
        payload = [self._normalize_activity_payload(entry) for entry in entries]
        if not payload:
            return
        insert_sql = text(
            """
            INSERT INTO activity_log (
                ts, [user], group_name, project, branch, action, result, message, branch_key
            ) VALUES (
                :ts, :user, :group_name, :project, :branch, :action, :result, :message, :branch_key
            )
            """
        )
        with self._connect() as conn:
            for row in payload:
                try:
                    conn.execute(insert_sql, row)
                except IntegrityError:
                    continue

    def prune_activity(self, valid_keys: Iterable[str]) -> None:
        keys = [key for key in dict.fromkeys(valid_keys) if key]
        if not keys:
            self._execute("DELETE FROM activity_log")
            return
        placeholders = ", ".join(f":bk{i}" for i in range(len(keys)))
        params = {f"bk{i}": key for i, key in enumerate(keys)}
        self._execute(
            f"DELETE FROM activity_log WHERE branch_key NOT IN ({placeholders})",
            params,
        )

    # ------------------------------------------------------------------
    # sprints & cards
    def fetch_sprints(self, *, branch_keys: Optional[Sequence[str]] = None) -> List[dict]:
        sql = "SELECT * FROM sprints"
        params: Dict[str, Any] = {}
        if branch_keys:
            keys = [key for key in branch_keys if key]
            if keys:
                placeholders = ", ".join(f":bk{i}" for i in range(len(keys)))
                sql += f" WHERE branch_key IN ({placeholders})"
                params.update({f"bk{i}": value for i, value in enumerate(keys)})
        sql += " ORDER BY created_at DESC, id DESC"
        return self._fetch_all(sql, params)

    def fetch_sprint(self, sprint_id: int) -> Optional[dict]:
        rows = self._fetch_all(
            "SELECT * FROM sprints WHERE id = :id",
            {"id": int(sprint_id)},
        )
        return rows[0] if rows else None

    def fetch_sprint_by_branch_key(self, branch_key: str) -> Optional[dict]:
        key = (branch_key or "").strip()
        if not key:
            return None
        rows = self._fetch_all(
            "SELECT * FROM sprints WHERE branch_key = :key OR qa_branch_key = :key",
            {"key": key},
        )
        return rows[0] if rows else None

    def _insert_identity(self, conn, table: str, columns: Sequence[str], data: Dict[str, Any]) -> None:
        col_list = ", ".join(self._quote(col) for col in columns)
        placeholders = ", ".join(f":{col}" for col in columns)
        conn.execute(text(f"SET IDENTITY_INSERT {table} ON"))
        try:
            conn.execute(text(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"), data)
        finally:
            conn.execute(text(f"SET IDENTITY_INSERT {table} OFF"))

    def upsert_sprint(self, payload: dict) -> int:
        data = self._normalize_sprint(payload)
        with self._connect() as conn:
            if data.get("id"):
                result = conn.execute(
                    text(
                        """
                        UPDATE sprints
                           SET branch_key = :branch_key,
                               qa_branch_key = :qa_branch_key,
                               name = :name,
                               version = :version,
                               lead_user = :lead_user,
                               qa_user = :qa_user,
                               description = :description,
                               status = :status,
                               closed_at = :closed_at,
                               closed_by = :closed_by,
                               created_at = :created_at,
                               created_by = :created_by,
                               updated_at = :updated_at,
                               updated_by = :updated_by
                         WHERE id = :id
                        """
                    ),
                    data,
                )
                if not result.rowcount:
                    self._insert_identity(
                        conn,
                        "sprints",
                        [
                            "id",
                            "branch_key",
                            "qa_branch_key",
                            "name",
                            "version",
                            "lead_user",
                            "qa_user",
                            "description",
                            "status",
                            "closed_at",
                            "closed_by",
                            "created_at",
                            "created_by",
                            "updated_at",
                            "updated_by",
                        ],
                        data,
                    )
            else:
                result = conn.execute(
                    text(
                        """
                        INSERT INTO sprints (
                            branch_key, qa_branch_key, name, version, lead_user, qa_user,
                            description, status, closed_at, closed_by, created_at, created_by,
                            updated_at, updated_by
                        ) OUTPUT INSERTED.id VALUES (
                            :branch_key, :qa_branch_key, :name, :version, :lead_user, :qa_user,
                            :description, :status, :closed_at, :closed_by, :created_at, :created_by,
                            :updated_at, :updated_by
                        )
                        """
                    ),
                    data,
                )
                data["id"] = int(result.scalar_one())
        return int(data.get("id") or 0)

    def delete_sprint(self, sprint_id: int) -> None:
        self._execute("DELETE FROM sprints WHERE id = :id", {"id": int(sprint_id)})

    def fetch_cards(
        self,
        *,
        sprint_ids: Optional[Sequence[int]] = None,
        branches: Optional[Sequence[str]] = None,
    ) -> List[dict]:
        sql = "SELECT * FROM cards"
        params: Dict[str, Any] = {}
        clauses: List[str] = []
        if sprint_ids:
            ids = [int(x) for x in sprint_ids if x is not None]
            if ids:
                placeholders = ", ".join(f":sid{i}" for i in range(len(ids)))
                clauses.append(f"sprint_id IN ({placeholders})")
                params.update({f"sid{i}": value for i, value in enumerate(ids)})
        if branches:
            names = [b for b in branches if b]
            if names:
                placeholders = ", ".join(f":br{i}" for i in range(len(names)))
                clauses.append(f"branch IN ({placeholders})")
                params.update({f"br{i}": value for i, value in enumerate(names)})
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC"
        return self._fetch_all(sql, params)

    def fetch_card(self, card_id: int) -> Optional[dict]:
        rows = self._fetch_all("SELECT * FROM cards WHERE id = :id", {"id": int(card_id)})
        return rows[0] if rows else None

    def upsert_card(self, payload: dict) -> int:
        data = self._normalize_card(payload)
        with self._connect() as conn:
            if data.get("id"):
                result = conn.execute(
                    text(
                        """
                        UPDATE cards
                           SET sprint_id = :sprint_id,
                               branch_key = :branch_key,
                               title = :title,
                               ticket_id = :ticket_id,
                               branch = :branch,
                               assignee = :assignee,
                               qa_assignee = :qa_assignee,
                               description = :description,
                               unit_tests_url = :unit_tests_url,
                               qa_url = :qa_url,
                               unit_tests_done = :unit_tests_done,
                               qa_done = :qa_done,
                               unit_tests_by = :unit_tests_by,
                               qa_by = :qa_by,
                               unit_tests_at = :unit_tests_at,
                               qa_at = :qa_at,
                               status = :status,
                               branch_created_by = :branch_created_by,
                               branch_created_at = :branch_created_at,
                               created_at = :created_at,
                               created_by = :created_by,
                               updated_at = :updated_at,
                               updated_by = :updated_by
                         WHERE id = :id
                        """
                    ),
                    data,
                )
                if not result.rowcount:
                    self._insert_identity(
                        conn,
                        "cards",
                        [
                            "id",
                            "sprint_id",
                            "branch_key",
                            "title",
                            "ticket_id",
                            "branch",
                            "assignee",
                            "qa_assignee",
                            "description",
                            "unit_tests_url",
                            "qa_url",
                            "unit_tests_done",
                            "qa_done",
                            "unit_tests_by",
                            "qa_by",
                            "unit_tests_at",
                            "qa_at",
                            "status",
                            "branch_created_by",
                            "branch_created_at",
                            "created_at",
                            "created_by",
                            "updated_at",
                            "updated_by",
                        ],
                        data,
                    )
            else:
                result = conn.execute(
                    text(
                        """
                        INSERT INTO cards (
                            sprint_id, branch_key, title, ticket_id, branch, assignee, qa_assignee, description,
                            unit_tests_url, qa_url, unit_tests_done, qa_done, unit_tests_by, qa_by, unit_tests_at, qa_at, status,
                            branch_created_by, branch_created_at, created_at, created_by, updated_at, updated_by
                        ) OUTPUT INSERTED.id VALUES (
                            :sprint_id, :branch_key, :title, :ticket_id, :branch, :assignee, :qa_assignee, :description,
                            :unit_tests_url, :qa_url, :unit_tests_done, :qa_done, :unit_tests_by, :qa_by, :unit_tests_at, :qa_at, :status,
                            :branch_created_by, :branch_created_at, :created_at, :created_by, :updated_at, :updated_by
                        )
                        """
                    ),
                    data,
                )
                data["id"] = int(result.scalar_one())
        return int(data.get("id") or 0)

    def delete_card(self, card_id: int) -> None:
        self._execute("DELETE FROM cards WHERE id = :id", {"id": int(card_id)})

    # ------------------------------------------------------------------
    # users & roles
    def fetch_users(self) -> List[dict]:
        return self._fetch_all(
            "SELECT username, display_name, email, active FROM users ORDER BY display_name"
        )

    def upsert_user(self, payload: dict) -> None:
        data = self._normalize_user(payload)
        with self._connect() as conn:
            result = conn.execute(
                text(
                    """
                    UPDATE users
                       SET display_name = :display_name,
                           email = :email,
                           active = :active
                     WHERE username = :username
                    """
                ),
                data,
            )
            if not result.rowcount:
                conn.execute(
                    text(
                        """
                        INSERT INTO users (username, display_name, email, active)
                        VALUES (:username, :display_name, :email, :active)
                        """
                    ),
                    data,
                )

    def delete_user(self, username: str) -> None:
        self._execute("DELETE FROM users WHERE username = :username", {"username": username})

    def fetch_roles(self) -> List[dict]:
        return self._fetch_all(
            "SELECT [key] AS key, name, description FROM roles ORDER BY name"
        )

    def upsert_role(self, payload: dict) -> None:
        data = self._normalize_role(payload)
        with self._connect() as conn:
            result = conn.execute(
                text(
                    """
                    UPDATE roles
                       SET name = :name,
                           description = :description
                     WHERE [key] = :key
                    """
                ),
                data,
            )
            if not result.rowcount:
                conn.execute(
                    text(
                        """
                        INSERT INTO roles ([key], name, description)
                        VALUES (:key, :name, :description)
                        """
                    ),
                    data,
                )

    def delete_role(self, role_key: str) -> None:
        self._execute("DELETE FROM roles WHERE [key] = :key", {"key": role_key})

    def fetch_user_roles(self, username: Optional[str] = None) -> List[dict]:
        sql = "SELECT username, role_key FROM user_roles"
        params: Dict[str, Any] = {}
        if username:
            sql += " WHERE username = :username"
            params["username"] = username
        return self._fetch_all(sql, params)

    def set_user_roles(self, username: str, roles: Sequence[str]) -> None:
        normalized = [role for role in dict.fromkeys(roles) if role]
        with self._connect() as conn:
            conn.execute(text("DELETE FROM user_roles WHERE username = :username"), {"username": username})
            if normalized:
                insert_sql = text(
                    """
                    INSERT INTO user_roles (username, role_key) VALUES (:username, :role_key)
                    """
                )
                for role in normalized:
                    try:
                        conn.execute(insert_sql, {"username": username, "role_key": role})
                    except IntegrityError:
                        continue


class BranchHistoryDB:
    """Facade selecting the appropriate backend based on configuration."""

    def __init__(self, path: Path, *, connection_url: Optional[str] = None):
        if connection_url is None:
            raw_url = os.environ.get("FORGEBUILD_BRANCH_HISTORY_URL", "")
        else:
            raw_url = connection_url
        url = (raw_url or "").strip()
        if url:
            self._backend = _SQLServerBranchHistoryBackend(url)
        else:
            self._backend = _SQLiteBranchHistoryBackend(path)

    def __getattr__(self, item: str):  # pragma: no cover - simple delegation
        return getattr(self._backend, item)
