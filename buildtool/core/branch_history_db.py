from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
import logging
import os
import queue
import sqlite3
import threading
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import urlparse, parse_qs, unquote

try:  # pragma: no cover - optional dependency when SQL Server is not enabled
    import pyodbc  # type: ignore
except Exception:  # pragma: no cover - defer errors until the backend is used
    pyodbc = None


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


# ---------------------------------------------------------------------------
# shared metadata

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


DEFAULT_SQLITE_TIMEOUT = 30.0
DEFAULT_SQLSERVER_POOL_SIZE = 5


class _BaseBackend(ABC):
    """Abstract backend that provides DB-agnostic helpers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    @abstractmethod
    def ensure_schema(self) -> None:
        """Ensure tables and indexes exist for the backend."""

    @contextmanager
    @abstractmethod
    def cursor(self) -> Iterator[Tuple[object, object]]:
        """Yield a (connection, cursor) pair ready to execute statements."""

    @abstractmethod
    def table_columns(self, table: str) -> set[str]:
        """Return the column names for the given table."""

    @abstractmethod
    def rebuild_table(
        self,
        table: str,
        template: str,
        expected_columns: Sequence[str],
        defaults: Dict[str, str],
    ) -> None:
        """Rebuild table ensuring expected columns exist."""

    @abstractmethod
    def execute_script(self, sql: str) -> None:
        """Execute a multi-statement SQL script."""

    @staticmethod
    def _row_to_dict(cursor: object, row: object) -> dict:
        raise NotImplementedError

    def fetch_all(self, sql: str, params: Sequence[object] | None = None) -> List[dict]:
        with self.cursor() as (_, cur):
            cur.execute(sql, params or [])
            rows = cur.fetchall()
            description = getattr(cur, "description", None)
        return [self._row_to_dict(description, row) for row in rows]

    def fetch_one(self, sql: str, params: Sequence[object] | None = None) -> Optional[dict]:
        with self.cursor() as (_, cur):
            cur.execute(sql, params or [])
            row = cur.fetchone()
            description = getattr(cur, "description", None)
        if not row:
            return None
        return self._row_to_dict(description, row)

    def execute(self, sql: str, params: Sequence[object] | None = None) -> int:
        with self.cursor() as (_, cur):
            cur.execute(sql, params or [])
            rowcount = getattr(cur, "rowcount", -1)
        return rowcount if rowcount is not None else -1

    def executemany(self, sql: str, params: Iterable[Sequence[object]]) -> None:
        with self.cursor() as (_, cur):
            cur.executemany(sql, list(params))


class _SQLiteBackend(_BaseBackend):
    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._timeout = DEFAULT_SQLITE_TIMEOUT
        self.ensure_schema()

    @contextmanager
    def cursor(self) -> Iterator[Tuple[sqlite3.Connection, sqlite3.Cursor]]:
        conn = sqlite3.connect(self.path, timeout=self._timeout)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            cur = conn.cursor()
            yield conn, cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def ensure_schema(self) -> None:
        with sqlite3.connect(self.path, timeout=self._timeout) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
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

    def execute_script(self, sql: str) -> None:
        with sqlite3.connect(self.path, timeout=self._timeout) as conn:
            conn.executescript(sql)

    def _apply_migrations(self, conn: sqlite3.Connection) -> None:
        self._ensure_activity_log_branch_key(conn)
        self._ensure_sprints_schema(conn)
        self._ensure_cards_schema(conn)

    def _ensure_indexes(self, conn: sqlite3.Connection) -> None:
        activity_columns = self._table_columns(conn, "activity_log")
        if "branch_key" in activity_columns:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_activity_branch_key ON activity_log(branch_key)"
            )

        if activity_columns:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_activity_ts ON activity_log(ts DESC)"
            )

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
        if not columns or "branch_key" in columns:
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
        self.rebuild_table("sprints", SPRINT_TABLE_TEMPLATE, SPRINT_COLUMNS, defaults)

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
        self.rebuild_table("cards", CARD_TABLE_TEMPLATE, CARD_COLUMNS, defaults)

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> set[str]:
        try:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        except sqlite3.OperationalError:
            return set()
        return {str(row[1]) for row in rows}

    def table_columns(self, table: str) -> set[str]:
        with sqlite3.connect(self.path, timeout=self._timeout) as conn:
            return self._table_columns(conn, table)

    def rebuild_table(
        self,
        table: str,
        template: str,
        expected_columns: Sequence[str],
        defaults: Dict[str, str],
    ) -> None:
        with sqlite3.connect(self.path, timeout=self._timeout) as conn:
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

    def _row_to_dict(self, cursor_desc: object, row: object) -> dict:
        return dict(row)


class _SqlServerBackend(_BaseBackend):
    def __init__(self, connection_string: str, *, pool_size: int = DEFAULT_SQLSERVER_POOL_SIZE) -> None:
        if pyodbc is None:  # pragma: no cover - validated at runtime
            raise RuntimeError(
                "El backend de SQL Server requiere la dependencia opcional 'pyodbc'."
            )
        super().__init__()
        self.connection_string = connection_string
        self.pool_size = max(1, pool_size)
        self._pool: "queue.LifoQueue[pyodbc.Connection]" = queue.LifoQueue()
        self.ensure_schema()

    def _get_connection(self) -> "pyodbc.Connection":
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            conn = pyodbc.connect(self.connection_string, autocommit=False)
            conn.add_output_converter(-155, lambda value: value)
            return conn

    def _release_connection(self, conn: "pyodbc.Connection") -> None:
        try:
            if self._pool.qsize() < self.pool_size:
                self._pool.put_nowait(conn)
            else:
                conn.close()
        except Exception:
            conn.close()

    @contextmanager
    def cursor(self) -> Iterator[Tuple["pyodbc.Connection", "pyodbc.Cursor"]]:
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            yield conn, cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._release_connection(conn)

    def ensure_schema(self) -> None:
        statements = [
            """
            IF OBJECT_ID('branches', 'U') IS NULL
            BEGIN
                CREATE TABLE branches (
                    key NVARCHAR(255) NOT NULL PRIMARY KEY,
                    branch NVARCHAR(255) NOT NULL,
                    group_name NVARCHAR(255) NULL,
                    project NVARCHAR(255) NULL,
                    created_at BIGINT NOT NULL DEFAULT 0,
                    created_by NVARCHAR(255) NULL,
                    exists_local BIT NOT NULL DEFAULT 0,
                    exists_origin BIT NOT NULL DEFAULT 0,
                    merge_status NVARCHAR(50) NULL,
                    diverged BIT NULL,
                    stale_days INT NULL,
                    last_action NVARCHAR(50) NULL,
                    last_updated_at BIGINT NOT NULL DEFAULT 0,
                    last_updated_by NVARCHAR(255) NULL
                )
            END
            """,
            """
            IF OBJECT_ID('activity_log', 'U') IS NULL
            BEGIN
                CREATE TABLE activity_log (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    ts BIGINT NOT NULL,
                    [user] NVARCHAR(255) NULL,
                    group_name NVARCHAR(255) NULL,
                    project NVARCHAR(255) NULL,
                    branch NVARCHAR(255) NULL,
                    action NVARCHAR(50) NULL,
                    result NVARCHAR(50) NULL,
                    message NVARCHAR(MAX) NULL,
                    branch_key NVARCHAR(767) NULL
                )
                CREATE UNIQUE INDEX uq_activity_log_event
                    ON activity_log(ts, [user], group_name, project, branch, action, result, message)
            END
            """,
            """
            IF OBJECT_ID('sprints', 'U') IS NULL
            BEGIN
                CREATE TABLE sprints (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    branch_key NVARCHAR(767) NOT NULL DEFAULT '',
                    qa_branch_key NVARCHAR(767) NULL,
                    name NVARCHAR(255) NOT NULL DEFAULT '',
                    version NVARCHAR(50) NOT NULL DEFAULT '',
                    lead_user NVARCHAR(255) NULL,
                    qa_user NVARCHAR(255) NULL,
                    description NVARCHAR(MAX) NULL,
                    status NVARCHAR(50) NOT NULL DEFAULT 'open',
                    closed_at BIGINT NULL,
                    closed_by NVARCHAR(255) NULL,
                    created_at BIGINT NOT NULL DEFAULT 0,
                    created_by NVARCHAR(255) NULL,
                    updated_at BIGINT NOT NULL DEFAULT 0,
                    updated_by NVARCHAR(255) NULL
                )
            END
            """,
            """
            IF OBJECT_ID('cards', 'U') IS NULL
            BEGIN
                CREATE TABLE cards (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    sprint_id INT NOT NULL,
                    branch_key NVARCHAR(767) NULL,
                    title NVARCHAR(255) NOT NULL DEFAULT '',
                    ticket_id NVARCHAR(255) NULL,
                    branch NVARCHAR(255) NOT NULL DEFAULT '',
                    assignee NVARCHAR(255) NULL,
                    qa_assignee NVARCHAR(255) NULL,
                    description NVARCHAR(MAX) NULL,
                    unit_tests_url NVARCHAR(1024) NULL,
                    qa_url NVARCHAR(1024) NULL,
                    unit_tests_done BIT NOT NULL DEFAULT 0,
                    qa_done BIT NOT NULL DEFAULT 0,
                    unit_tests_by NVARCHAR(255) NULL,
                    qa_by NVARCHAR(255) NULL,
                    unit_tests_at BIGINT NULL,
                    qa_at BIGINT NULL,
                    status NVARCHAR(50) NULL,
                    branch_created_by NVARCHAR(255) NULL,
                    branch_created_at BIGINT NULL,
                    created_at BIGINT NOT NULL DEFAULT 0,
                    created_by NVARCHAR(255) NULL,
                    updated_at BIGINT NOT NULL DEFAULT 0,
                    updated_by NVARCHAR(255) NULL,
                    CONSTRAINT fk_cards_sprint FOREIGN KEY (sprint_id) REFERENCES sprints(id) ON DELETE CASCADE
                )
                CREATE INDEX idx_cards_sprint ON cards(sprint_id)
                CREATE INDEX idx_cards_branch ON cards(branch)
            END
            """,
            """
            IF OBJECT_ID('users', 'U') IS NULL
            BEGIN
                CREATE TABLE users (
                    username NVARCHAR(255) NOT NULL PRIMARY KEY,
                    display_name NVARCHAR(255) NOT NULL,
                    email NVARCHAR(255) NULL,
                    active BIT NOT NULL DEFAULT 1
                )
            END
            """,
            """
            IF OBJECT_ID('roles', 'U') IS NULL
            BEGIN
                CREATE TABLE roles (
                    [key] NVARCHAR(255) NOT NULL PRIMARY KEY,
                    name NVARCHAR(255) NOT NULL,
                    description NVARCHAR(MAX) NULL
                )
            END
            """,
            """
            IF OBJECT_ID('user_roles', 'U') IS NULL
            BEGIN
                CREATE TABLE user_roles (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    username NVARCHAR(255) NOT NULL,
                    role_key NVARCHAR(255) NOT NULL,
                    CONSTRAINT uq_user_roles UNIQUE(username, role_key),
                    CONSTRAINT fk_user_roles_user FOREIGN KEY(username) REFERENCES users(username) ON DELETE CASCADE,
                    CONSTRAINT fk_user_roles_role FOREIGN KEY(role_key) REFERENCES roles([key]) ON DELETE CASCADE
                )
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_activity_branch_key')
            BEGIN
                CREATE INDEX idx_activity_branch_key ON activity_log(branch_key)
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_activity_ts')
            BEGIN
                CREATE INDEX idx_activity_ts ON activity_log(ts DESC)
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_sprints_branch')
            BEGIN
                CREATE INDEX idx_sprints_branch ON sprints(branch_key)
            END
            """,
        ]
        with self.cursor() as (_, cur):
            for stmt in statements:
                cur.execute(stmt)

    def execute_script(self, sql: str) -> None:
        statements = [s for s in sql.split(";") if s.strip()]
        with self.cursor() as (_, cur):
            for stmt in statements:
                cur.execute(stmt)

    def table_columns(self, table: str) -> set[str]:
        query = (
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"
        )
        with self.cursor() as (_, cur):
            cur.execute(query, (table,))
            rows = cur.fetchall()
        return {str(row[0]) for row in rows}

    def rebuild_table(
        self,
        table: str,
        template: str,
        expected_columns: Sequence[str],
        defaults: Dict[str, str],
    ) -> None:
        raise NotImplementedError(
            "Las migraciones automáticas no están soportadas en SQL Server; use el script de migración."
        )

    def _row_to_dict(self, cursor_desc: object, row: object) -> dict:
        if not cursor_desc:
            return {}
        columns = [col[0] for col in cursor_desc]
        return {col: row[idx] for idx, col in enumerate(columns)}


class BranchHistoryDB:
    """Persistence layer capable of working with SQLite or SQL Server."""

    def __init__(self, path: Path, *, connection_url: Optional[str] = None):
        self.path = Path(path)
        self._backend = self._resolve_backend(self.path, connection_url)

    @staticmethod
    def _resolve_backend(path: Path, url_override: Optional[str]) -> _BaseBackend:
        url = (url_override or os.environ.get("BRANCH_HISTORY_URL", "")).strip()
        if not url:
            return _SQLiteBackend(path)
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        if scheme in {"sqlite", "file"}:
            target_path = unquote(parsed.path or "")
            if scheme == "file" and target_path.startswith("//"):
                target_path = target_path[1:]
            if target_path:
                sqlite_path = Path(target_path)
            else:
                sqlite_path = path
            return _SQLiteBackend(sqlite_path)
        if scheme.startswith("mssql"):
            if parsed.scheme.lower().startswith("mssql+pyodbc"):
                query = parse_qs(parsed.query)
                if "odbc_connect" in query:
                    conn_str = unquote(query["odbc_connect"][0])
                else:
                    # Build minimal ODBC connection string from components
                    netloc = parsed.netloc or ""
                    user, password, host = None, None, None
                    if "@" in netloc:
                        creds, host = netloc.split("@", 1)
                        if ":" in creds:
                            user, password = creds.split(":", 1)
                        else:
                            user = creds
                    else:
                        host = netloc
                    database = parsed.path.lstrip("/")
                    parts = []
                    driver = query.get("driver", ["ODBC Driver 17 for SQL Server"])[0]
                    parts.append(f"Driver={{{driver}}}")
                    if host:
                        parts.append(f"Server={host}")
                    if database:
                        parts.append(f"Database={database}")
                    if user:
                        parts.append(f"UID={user}")
                    if password:
                        parts.append(f"PWD={password}")
                    conn_str = ";".join(parts)
            else:
                conn_str = unquote(url.split("://", 1)[1])
            return _SqlServerBackend(conn_str)
        raise ValueError(
            f"Esquema de conexión no soportado para BRANCH_HISTORY_URL: {parsed.scheme}"
        )

    # ------------------------------------------------------------------
    # branches
    def fetch_branches(self, *, filter_origin: bool = False) -> List[dict]:
        query = "SELECT * FROM branches"
        params: List[object] = []
        if filter_origin:
            query += " WHERE exists_origin = ?"
            params.append(1)
        return self._backend.fetch_all(query, params)

    def replace_branches(self, records: Iterable[dict]) -> None:
        payload = [self._normalize_branch_payload(rec) for rec in records]
        self._backend.execute("DELETE FROM branches")
        if not payload:
            return
        placeholders = ",".join("?" for _ in BRANCH_COLUMNS)
        sql = (
            "INSERT INTO branches (" + ", ".join(BRANCH_COLUMNS) + ") VALUES ("
            + placeholders + ")"
        )
        values = [tuple(item[col] for col in BRANCH_COLUMNS) for item in payload]
        self._backend.executemany(sql, values)

    def _update_then_insert(
        self,
        table: str,
        payload: Dict[str, object],
        key_column: str,
        columns: Sequence[str],
        *,
        return_identity: bool = False,
    ) -> Optional[int]:
        update_columns = [col for col in columns if col != key_column]
        set_clause = ", ".join(f"{col} = ?" for col in update_columns)
        update_sql = f"UPDATE {table} SET {set_clause} WHERE {key_column} = ?"
        update_params = [payload[col] for col in update_columns]
        update_params.append(payload[key_column])
        rowcount = self._backend.execute(update_sql, update_params)
        if rowcount:
            return None
        insert_sql = (
            f"INSERT INTO {table} (" + ", ".join(columns) + ") VALUES ("
            + ",".join("?" for _ in columns)
            + ")"
        )
        insert_params = [payload[col] for col in columns]
        if not return_identity:
            self._backend.execute(insert_sql, insert_params)
            return None
        with self._backend.cursor() as (_, cur):
            cur.execute(insert_sql, insert_params)
            if isinstance(self._backend, _SQLiteBackend):
                return int(cur.lastrowid or 0)
            if isinstance(self._backend, _SqlServerBackend):
                cur.execute("SELECT CAST(SCOPE_IDENTITY() AS BIGINT)")
                row = cur.fetchone()
                if row:
                    return int(row[0])
        return None

    def upsert_branch(self, record: dict) -> None:
        payload = self._normalize_branch_payload(record)
        self._update_then_insert("branches", payload, "key", BRANCH_COLUMNS)

    def delete_branch(self, key: str) -> None:
        self._backend.execute("DELETE FROM branches WHERE key = ?", (key,))

    # ------------------------------------------------------------------
    # activity log
    def fetch_activity(self, *, branch_keys: Optional[Iterable[str]] = None) -> List[dict]:
        sql = (
            "SELECT ts, user, group_name, project, branch, action, result, message, branch_key "
            "FROM activity_log"
        )
        params: List[object] = []
        if branch_keys:
            keys = [key for key in dict.fromkeys(branch_keys) if key]
            if keys:
                placeholders = ",".join("?" for _ in keys)
                sql += f" WHERE branch_key IN ({placeholders})"
                params.extend(keys)
        sql += " ORDER BY ts DESC, id DESC"
        return self._backend.fetch_all(sql, params)

    def _is_unique_violation(self, exc: Exception) -> bool:
        if isinstance(exc, sqlite3.IntegrityError):
            return "UNIQUE" in str(exc).upper()
        if pyodbc is not None and isinstance(exc, pyodbc.IntegrityError):  # type: ignore[attr-defined]
            return True
        return False

    def append_activity(self, entries: Iterable[dict]) -> None:
        payload = [self._normalize_activity_payload(entry) for entry in entries]
        if not payload:
            return
        columns = [
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
        sql = (
            "INSERT INTO activity_log (" + ", ".join(columns) + ") VALUES ("
            + ",".join("?" for _ in columns)
            + ")"
        )
        for item in payload:
            params = [item.get(col) for col in columns]
            try:
                self._backend.execute(sql, params)
            except Exception as exc:
                if not self._is_unique_violation(exc):
                    raise

    # ------------------------------------------------------------------
    # sprints & cards
    def fetch_sprints(self, *, branch_keys: Optional[Sequence[str]] = None) -> List[dict]:
        sql = "SELECT * FROM sprints"
        params: List[object] = []
        if branch_keys:
            keys = [key for key in branch_keys if key]
            if keys:
                placeholders = ",".join("?" for _ in keys)
                sql += f" WHERE branch_key IN ({placeholders})"
                params.extend(keys)
        sql += " ORDER BY id DESC"
        return self._backend.fetch_all(sql, params)

    def fetch_sprint(self, sprint_id: int) -> Optional[dict]:
        if sprint_id is None:
            return None
        return self._backend.fetch_one(
            "SELECT * FROM sprints WHERE id = ?", (int(sprint_id),)
        )

    def fetch_sprint_by_branch_key(self, branch_key: str) -> Optional[dict]:
        key = (branch_key or "").strip()
        if not key:
            return None
        return self._backend.fetch_one(
            "SELECT * FROM sprints WHERE branch_key = ? OR qa_branch_key = ?",
            (key, key),
        )

    def upsert_sprint(self, payload: dict) -> int:
        data = self._normalize_sprint(payload)
        identifier = data.get("id")
        columns = [
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
        new_id = self._update_then_insert(
            "sprints", data, "id", columns, return_identity=True
        )
        if identifier:
            return int(identifier)
        if new_id:
            return int(new_id)
        raise RuntimeError("No se pudo obtener el identificador del sprint insertado")

    def delete_sprint(self, sprint_id: int) -> None:
        self._backend.execute("DELETE FROM sprints WHERE id = ?", (int(sprint_id),))

    def fetch_cards(
        self,
        *,
        sprint_ids: Optional[Iterable[int]] = None,
        branches: Optional[Iterable[str]] = None,
    ) -> List[dict]:
        sql = "SELECT * FROM cards"
        params: List[object] = []
        conditions: List[str] = []
        if sprint_ids:
            ids = [int(sid) for sid in sprint_ids if sid is not None]
            if ids:
                placeholders = ",".join("?" for _ in ids)
                conditions.append(f"sprint_id IN ({placeholders})")
                params.extend(ids)
        if branches:
            branch_list = [branch for branch in branches if branch]
            if branch_list:
                placeholders = ",".join("?" for _ in branch_list)
                conditions.append(f"branch IN ({placeholders})")
                params.extend(branch_list)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY created_at DESC, id DESC"
        return self._backend.fetch_all(sql, params)

    def upsert_card(self, payload: dict) -> int:
        data = self._normalize_card(payload)
        identifier = data.get("id")
        columns = [
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
        new_id = self._update_then_insert(
            "cards", data, "id", columns, return_identity=True
        )
        if identifier:
            return int(identifier)
        if new_id:
            return int(new_id)
        raise RuntimeError("No se pudo obtener el identificador de la tarjeta insertada")

    def delete_card(self, card_id: int) -> None:
        self._backend.execute("DELETE FROM cards WHERE id = ?", (int(card_id),))

    # ------------------------------------------------------------------
    # users & roles
    def fetch_users(self) -> List[dict]:
        return self._backend.fetch_all(
            "SELECT username, display_name, email, active FROM users ORDER BY display_name"
        )

    def upsert_user(self, payload: dict) -> None:
        data = self._normalize_user(payload)
        updated = self._backend.execute(
            "UPDATE users SET display_name = ?, email = ?, active = ? WHERE username = ?",
            [data["display_name"], data.get("email"), int(bool(data.get("active"))), data["username"]],
        )
        if not updated:
            self._backend.execute(
                "INSERT INTO users (username, display_name, email, active) VALUES (?, ?, ?, ?)",
                [
                    data["username"],
                    data["display_name"],
                    data.get("email"),
                    int(bool(data.get("active"))),
                ],
            )

    def delete_user(self, username: str) -> None:
        self._backend.execute("DELETE FROM users WHERE username = ?", (username,))

    def fetch_roles(self) -> List[dict]:
        return self._backend.fetch_all(
            "SELECT key, name, description FROM roles ORDER BY name"
        )

    def upsert_role(self, payload: dict) -> None:
        data = self._normalize_role(payload)
        updated = self._backend.execute(
            "UPDATE roles SET name = ?, description = ? WHERE key = ?",
            [data["name"], data.get("description"), data["key"]],
        )
        if not updated:
            self._backend.execute(
                "INSERT INTO roles (key, name, description) VALUES (?, ?, ?)",
                [data["key"], data["name"], data.get("description")],
            )

    def delete_role(self, role_key: str) -> None:
        self._backend.execute("DELETE FROM roles WHERE key = ?", (role_key,))

    def fetch_user_roles(self, username: Optional[str] = None) -> List[dict]:
        sql = "SELECT username, role_key FROM user_roles"
        params: List[object] = []
        if username:
            sql += " WHERE username = ?"
            params.append(username)
        return self._backend.fetch_all(sql, params)

    def set_user_roles(self, username: str, roles: Sequence[str]) -> None:
        normalized = [role for role in roles if role]
        self._backend.execute("DELETE FROM user_roles WHERE username = ?", (username,))
        if not normalized:
            return
        sql = "INSERT INTO user_roles (username, role_key) VALUES (?, ?)"
        for role in normalized:
            try:
                self._backend.execute(sql, (username, role))
            except Exception as exc:
                if not self._is_unique_violation(exc):
                    raise

    def prune_activity(self, valid_keys: Iterable[str]) -> None:
        keys = [key for key in dict.fromkeys(valid_keys) if key]
        if not keys:
            self._backend.execute("DELETE FROM activity_log")
            return
        placeholders = ",".join("?" for _ in keys)
        sql = f"DELETE FROM activity_log WHERE branch_key NOT IN ({placeholders})"
        self._backend.execute(sql, keys)

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

    def _normalize_sprint(self, payload: dict) -> Dict[str, object]:
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

    def _normalize_card(self, payload: dict) -> Dict[str, object]:
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

    def _normalize_user(self, payload: dict) -> Dict[str, object]:
        return {
            "username": payload.get("username") or "",
            "display_name": payload.get("display_name") or payload.get("username") or "",
            "email": payload.get("email"),
            "active": 1 if payload.get("active", True) else 0,
        }

    def _normalize_role(self, payload: dict) -> Dict[str, object]:
        return {
            "key": payload.get("key") or "",
            "name": payload.get("name") or payload.get("key") or "",
            "description": payload.get("description") or "",
        }

