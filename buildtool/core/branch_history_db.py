from __future__ import annotations

from dataclasses import dataclass
import importlib
import logging
import os
import sqlite3
import threading
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from queue import Empty, Full, Queue
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

_SQLSERVER_IMPORT_ERRORS: List[str] = []
_SQLSERVER_DRIVER: Optional[str] = None
tds = None

for _candidate in ("pytds", "pymssql", "tds"):
    try:  # pragma: no cover - optional dependency, exercised in integration flows
        _module = importlib.import_module(_candidate)
    except Exception as exc:  # pragma: no cover - tests run without SQL Server driver
        _SQLSERVER_IMPORT_ERRORS.append(f"{_candidate}: {exc}")
        continue

    if not hasattr(_module, "connect"):
        _SQLSERVER_IMPORT_ERRORS.append(
            f"{_candidate}: el módulo no expone la función 'connect' esperada"
        )
        continue

    tds = _module
    _SQLSERVER_DRIVER = _candidate
    break

del _candidate


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


class BranchHistoryBackend(ABC):
    """Interface implemented by all persistence backends."""

    @abstractmethod
    def ensure_schema(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def fetch_branches(self, *, filter_origin: bool = False) -> List[dict]:
        raise NotImplementedError

    @abstractmethod
    def replace_branches(self, records: Iterable[dict]) -> None:
        raise NotImplementedError

    @abstractmethod
    def upsert_branch(self, record: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_branch(self, key: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def append_activity(self, entries: Iterable[dict]) -> None:
        raise NotImplementedError

    @abstractmethod
    def fetch_activity(
        self,
        *,
        branch_keys: Optional[Iterable[str]] = None,
        limit: Optional[int] = None,
    ) -> List[dict]:
        raise NotImplementedError

    @abstractmethod
    def prune_activity(self, valid_keys: Iterable[str]) -> None:
        raise NotImplementedError

    @abstractmethod
    def fetch_sprints(self, branch_keys: Optional[Sequence[str]] = None) -> List[dict]:
        raise NotImplementedError

    @abstractmethod
    def fetch_sprint(self, sprint_id: int) -> Optional[dict]:
        raise NotImplementedError

    @abstractmethod
    def fetch_sprint_by_branch_key(self, branch_key: str) -> Optional[dict]:
        raise NotImplementedError

    @abstractmethod
    def upsert_sprint(self, payload: dict) -> int:
        raise NotImplementedError

    @abstractmethod
    def delete_sprint(self, sprint_id: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def fetch_cards(
        self,
        *,
        sprint_ids: Optional[Sequence[int]] = None,
        branches: Optional[Sequence[str]] = None,
    ) -> List[dict]:
        raise NotImplementedError

    @abstractmethod
    def upsert_card(self, payload: dict) -> int:
        raise NotImplementedError

    @abstractmethod
    def delete_card(self, card_id: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def fetch_users(self) -> List[dict]:
        raise NotImplementedError

    @abstractmethod
    def upsert_user(self, payload: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_user(self, username: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def fetch_roles(self) -> List[dict]:
        raise NotImplementedError

    @abstractmethod
    def upsert_role(self, payload: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_role(self, role_key: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def fetch_user_roles(self, username: Optional[str] = None) -> List[dict]:
        raise NotImplementedError

    @abstractmethod
    def set_user_roles(self, username: str, roles: Sequence[str]) -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class BranchHistorySettings:
    """Resolved configuration for the branch history repository."""

    backend: str
    sqlite_path: Optional[Path]
    sqlserver_url: Optional[str]
    pool_size: int

    @classmethod
    def resolve(cls, default_path: Optional[Path]) -> "BranchHistorySettings":
        backend = os.environ.get("FORGEBUILD_BRANCH_HISTORY_BACKEND", "").strip().lower()
        url = os.environ.get("FORGEBUILD_BRANCH_HISTORY_URL", "").strip() or None
        sqlite_override = os.environ.get("FORGEBUILD_BRANCH_HISTORY_PATH", "").strip() or None
        pool_size_raw = os.environ.get("FORGEBUILD_BRANCH_HISTORY_POOL_SIZE", "").strip()
        pool_size = 5
        if pool_size_raw.isdigit():
            pool_size = max(1, int(pool_size_raw))
        elif pool_size_raw:
            try:
                pool_size = max(1, int(float(pool_size_raw)))
            except (TypeError, ValueError):
                logging.warning("Valor inválido para FORGEBUILD_BRANCH_HISTORY_POOL_SIZE: %s", pool_size_raw)

        if not backend:
            backend = "sqlserver" if url else "sqlite"
        if backend not in {"sqlite", "sqlserver"}:
            logging.warning("Backend de historial desconocido '%s', se usará SQLite", backend)
            backend = "sqlite"

        sqlite_path = Path(sqlite_override) if sqlite_override else default_path
        if backend == "sqlite" and sqlite_path is None:
            raise ValueError("Se requiere una ruta SQLite para inicializar el historial de ramas")
        if backend == "sqlserver" and not url:
            raise ValueError("FORGEBUILD_BRANCH_HISTORY_URL es obligatorio para el backend SQL Server")

        return cls(backend=backend, sqlite_path=sqlite_path, sqlserver_url=url, pool_size=pool_size)


def _parse_sqlserver_url(url: str) -> Dict[str, Any]:
    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if "+" in scheme:
        base, _, driver = scheme.partition("+")
        if base in {"mssql", "sqlserver", "tds"} and driver in {"pytds", "pymssql"}:
            scheme = base
        else:
            raise ValueError(f"Esquema de URL no soportado para SQL Server: {parsed.scheme}")

    if scheme not in {"mssql", "sqlserver", "tds"}:
        raise ValueError(f"Esquema de URL no soportado para SQL Server: {parsed.scheme}")

    if not parsed.hostname:
        raise ValueError("La URL de SQL Server debe incluir un host")

    params = {k.lower(): v[-1] for k, v in parse_qs(parsed.query, keep_blank_values=True).items() if v}
    user = parsed.username or params.get("user") or params.get("uid")
    password = parsed.password or params.get("password") or params.get("pwd")
    database = parsed.path.lstrip("/") if parsed.path else params.get("database") or params.get("db")

    if not user:
        raise ValueError("La URL de SQL Server debe incluir el usuario de conexión")
    if password is None:
        password = ""
    if not database:
        raise ValueError("La URL de SQL Server debe incluir la base de datos")

    connect_kwargs: Dict[str, Any] = {
        "server": parsed.hostname,
        "port": parsed.port or 1433,
        "user": user,
        "password": password,
        "database": database,
        "autocommit": False,
        "use_mars": True,
        "appname": params.get("appname", "ForgeBuild"),
    }

    if "timeout" in params:
        try:
            connect_kwargs["timeout"] = int(params["timeout"])
        except (TypeError, ValueError):
            logging.warning("Timeout inválido en la cadena de conexión SQL Server: %s", params["timeout"])

    if params.get("login_timeout"):
        try:
            connect_kwargs["login_timeout"] = int(params["login_timeout"])
        except (TypeError, ValueError):
            logging.warning(
                "login_timeout inválido en la cadena de conexión SQL Server: %s",
                params["login_timeout"],
            )

    if params.get("encrypt"):
        connect_kwargs["encrypt"] = params["encrypt"].lower() in {"1", "true", "yes"}

    if params.get("trustservercertificate"):
        connect_kwargs["trust_server_certificate"] = params["trustservercertificate"].lower() in {"1", "true", "yes"}

    return connect_kwargs


def _rows_to_dicts(cursor, rows: Sequence[Any]) -> List[dict]:
    """Convert DB-API rows into dictionaries."""

    if not rows:
        return []
    first = rows[0]
    if isinstance(first, dict):
        return [dict(row) for row in rows]
    columns = [col[0] for col in cursor.description] if cursor.description else []
    result: List[dict] = []
    for row in rows:
        if isinstance(row, dict):
            result.append(dict(row))
        else:
            result.append({col: row[idx] for idx, col in enumerate(columns)})
    return result


class _SqlServerConnectionPool:
    """Very small helper to reuse python-tds connections."""

    def __init__(self, connect_kwargs: Mapping[str, Any], size: int) -> None:
        if tds is None:  # pragma: no cover - validated during runtime configuration
            detail = "; ".join(_SQLSERVER_IMPORT_ERRORS)
            if detail:
                detail = f" Detalles: {detail}"
            raise RuntimeError(
                "No se encontró un cliente SQL Server compatible (python-tds o pymssql)."
                + detail
            )
        self._connect_kwargs = dict(connect_kwargs)
        self._pool: "Queue[Any]" = Queue(maxsize=max(1, size))
        self._lock = threading.Lock()

    def _create(self):
        return tds.connect(**self._connect_kwargs)

    def acquire(self):
        try:
            conn = self._pool.get_nowait()
        except Empty:
            conn = self._create()
        else:
            if getattr(conn, "closed", False):
                conn = self._create()
        return conn

    def release(self, conn) -> None:
        if conn is None:
            return
        if getattr(conn, "closed", False):
            return
        try:
            self._pool.put_nowait(conn)
        except Full:
            try:
                conn.close()
            except Exception:
                pass

class SQLiteBranchHistoryBackend(BranchHistoryBackend):
    """SQLite persistence for branch index and activity log."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.ensure_schema()

    # ------------------------------------------------------------------
    # basic helpers
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def ensure_schema(self) -> None:
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
                    "SQLiteBranchHistoryBackend journal_mode WAL unavailable for %s; falling back to DELETE",
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



class SqlServerBranchHistoryBackend(BranchHistoryBackend):
    """SQL Server persistence backed by python-tds/pymssql."""

    def __init__(self, url: str, *, pool_size: int = 5) -> None:
        self._connect_kwargs = _parse_sqlserver_url(url)
        self._pool = _SqlServerConnectionPool(self._connect_kwargs, pool_size)
        self.ensure_schema()

    @contextmanager
    def _connection(self):
        conn = self._pool.acquire()
        try:
            yield conn
            try:
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            self._pool.release(conn)

    # ------------------------------------------------------------------
    # schema helpers
    def ensure_schema(self) -> None:
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                self._create_tables(cursor)
                self._ensure_indexes(cursor)
            finally:
                cursor.close()

    def _create_tables(self, cursor) -> None:
        statements = [
            """
            IF OBJECT_ID('branches', 'U') IS NULL
            BEGIN
                CREATE TABLE branches (
                    [key] NVARCHAR(512) NOT NULL PRIMARY KEY,
                    [branch] NVARCHAR(255) NOT NULL,
                    [group_name] NVARCHAR(255) NULL,
                    [project] NVARCHAR(255) NULL,
                    [created_at] BIGINT NOT NULL DEFAULT 0,
                    [created_by] NVARCHAR(255) NULL,
                    [exists_local] BIT NOT NULL DEFAULT 0,
                    [exists_origin] BIT NOT NULL DEFAULT 0,
                    [merge_status] NVARCHAR(255) NULL,
                    [diverged] BIT NULL,
                    [stale_days] INT NULL,
                    [last_action] NVARCHAR(255) NULL,
                    [last_updated_at] BIGINT NOT NULL DEFAULT 0,
                    [last_updated_by] NVARCHAR(255) NULL
                )
            END
            """,
            """
            IF OBJECT_ID('activity_log', 'U') IS NULL
            BEGIN
                CREATE TABLE activity_log (
                    [id] INT IDENTITY(1,1) PRIMARY KEY,
                    [ts] BIGINT NOT NULL,
                    [user] NVARCHAR(255) NULL,
                    [group_name] NVARCHAR(255) NULL,
                    [project] NVARCHAR(255) NULL,
                    [branch] NVARCHAR(255) NULL,
                    [action] NVARCHAR(255) NULL,
                    [result] NVARCHAR(255) NULL,
                    [message] NVARCHAR(MAX) NULL,
                    [branch_key] NVARCHAR(512) NULL
                )
            END
            """,
            """
            IF OBJECT_ID('sprints', 'U') IS NULL
            BEGIN
                CREATE TABLE sprints (
                    [id] INT IDENTITY(1,1) PRIMARY KEY,
                    [branch_key] NVARCHAR(512) NOT NULL DEFAULT '',
                    [qa_branch_key] NVARCHAR(512) NULL,
                    [name] NVARCHAR(255) NOT NULL DEFAULT '',
                    [version] NVARCHAR(255) NOT NULL DEFAULT '',
                    [lead_user] NVARCHAR(255) NULL,
                    [qa_user] NVARCHAR(255) NULL,
                    [description] NVARCHAR(MAX) NULL,
                    [status] NVARCHAR(64) NOT NULL DEFAULT 'open',
                    [closed_at] BIGINT NULL,
                    [closed_by] NVARCHAR(255) NULL,
                    [created_at] BIGINT NOT NULL DEFAULT 0,
                    [created_by] NVARCHAR(255) NULL,
                    [updated_at] BIGINT NOT NULL DEFAULT 0,
                    [updated_by] NVARCHAR(255) NULL
                )
            END
            """,
            """
            IF OBJECT_ID('cards', 'U') IS NULL
            BEGIN
                CREATE TABLE cards (
                    [id] INT IDENTITY(1,1) PRIMARY KEY,
                    [sprint_id] INT NOT NULL,
                    [branch_key] NVARCHAR(512) NULL,
                    [title] NVARCHAR(255) NOT NULL DEFAULT '',
                    [ticket_id] NVARCHAR(255) NULL,
                    [branch] NVARCHAR(255) NOT NULL DEFAULT '',
                    [assignee] NVARCHAR(255) NULL,
                    [qa_assignee] NVARCHAR(255) NULL,
                    [description] NVARCHAR(MAX) NULL,
                    [unit_tests_url] NVARCHAR(1024) NULL,
                    [qa_url] NVARCHAR(1024) NULL,
                    [unit_tests_done] BIT NOT NULL DEFAULT 0,
                    [qa_done] BIT NOT NULL DEFAULT 0,
                    [unit_tests_by] NVARCHAR(255) NULL,
                    [qa_by] NVARCHAR(255) NULL,
                    [unit_tests_at] BIGINT NULL,
                    [qa_at] BIGINT NULL,
                    [status] NVARCHAR(64) NOT NULL DEFAULT 'pending',
                    [branch_created_by] NVARCHAR(255) NULL,
                    [branch_created_at] BIGINT NULL,
                    [created_at] BIGINT NOT NULL DEFAULT 0,
                    [created_by] NVARCHAR(255) NULL,
                    [updated_at] BIGINT NOT NULL DEFAULT 0,
                    [updated_by] NVARCHAR(255) NULL,
                    CONSTRAINT fk_cards_sprint FOREIGN KEY ([sprint_id]) REFERENCES sprints([id]) ON DELETE CASCADE
                )
            END
            """,
            """
            IF OBJECT_ID('users', 'U') IS NULL
            BEGIN
                CREATE TABLE users (
                    [username] NVARCHAR(255) NOT NULL PRIMARY KEY,
                    [display_name] NVARCHAR(255) NOT NULL,
                    [email] NVARCHAR(255) NULL,
                    [active] BIT NOT NULL DEFAULT 1
                )
            END
            """,
            """
            IF OBJECT_ID('roles', 'U') IS NULL
            BEGIN
                CREATE TABLE roles (
                    [key] NVARCHAR(255) NOT NULL PRIMARY KEY,
                    [name] NVARCHAR(255) NOT NULL,
                    [description] NVARCHAR(MAX) NULL
                )
            END
            """,
            """
            IF OBJECT_ID('user_roles', 'U') IS NULL
            BEGIN
                CREATE TABLE user_roles (
                    [id] INT IDENTITY(1,1) PRIMARY KEY,
                    [username] NVARCHAR(255) NOT NULL,
                    [role_key] NVARCHAR(255) NOT NULL,
                    CONSTRAINT uq_user_roles UNIQUE ([username], [role_key]),
                    CONSTRAINT fk_user_roles_user FOREIGN KEY ([username]) REFERENCES users([username]) ON DELETE CASCADE,
                    CONSTRAINT fk_user_roles_role FOREIGN KEY ([role_key]) REFERENCES roles([key]) ON DELETE CASCADE
                )
            END
            """,
        ]
        for stmt in statements:
            cursor.execute(stmt)

    def _ensure_indexes(self, cursor) -> None:
        statements = [
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_activity_branch_key' AND object_id = OBJECT_ID('activity_log'))
            BEGIN
                CREATE INDEX idx_activity_branch_key ON activity_log([branch_key])
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_activity_ts' AND object_id = OBJECT_ID('activity_log'))
            BEGIN
                CREATE INDEX idx_activity_ts ON activity_log([ts] DESC)
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_sprints_branch' AND object_id = OBJECT_ID('sprints'))
            BEGIN
                CREATE INDEX idx_sprints_branch ON sprints([branch_key])
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_cards_sprint' AND object_id = OBJECT_ID('cards'))
            BEGIN
                CREATE INDEX idx_cards_sprint ON cards([sprint_id])
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_cards_branch' AND object_id = OBJECT_ID('cards'))
            BEGIN
                CREATE INDEX idx_cards_branch ON cards([branch])
            END
            """,
        ]
        for stmt in statements:
            cursor.execute(stmt)

    # ------------------------------------------------------------------
    # branches
    def fetch_branches(self, *, filter_origin: bool = False) -> List[dict]:
        sql = (
            "SELECT [key] AS key, [branch], [group_name], [project], [created_at], [created_by], "
            "[exists_local], [exists_origin], [merge_status], [diverged], [stale_days], "
            "[last_action], [last_updated_at], [last_updated_by] FROM branches"
        )
        if filter_origin:
            sql += " WHERE [exists_origin] = 1"
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                rows = cursor.fetchall()
                result = _rows_to_dicts(cursor, rows)
            finally:
                cursor.close()
        return result

    def replace_branches(self, records: Iterable[dict]) -> None:
        payload = [self._normalize_branch_payload(rec) for rec in records]
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DELETE FROM branches")
                for row in payload:
                    cursor.execute(
                        """
                        INSERT INTO branches (
                            [key], [branch], [group_name], [project], [created_at], [created_by],
                            [exists_local], [exists_origin], [merge_status], [diverged],
                            [stale_days], [last_action], [last_updated_at], [last_updated_by]
                        ) VALUES (
                            @key, @branch, @group_name, @project, @created_at, @created_by,
                            @exists_local, @exists_origin, @merge_status, @diverged,
                            @stale_days, @last_action, @last_updated_at, @last_updated_by
                        )
                        """,
                        row,
                    )
            finally:
                cursor.close()

    def upsert_branch(self, record: dict) -> None:
        data = self._normalize_branch_payload(record)
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    UPDATE branches SET
                        [branch] = @branch,
                        [group_name] = @group_name,
                        [project] = @project,
                        [created_at] = @created_at,
                        [created_by] = @created_by,
                        [exists_local] = @exists_local,
                        [exists_origin] = @exists_origin,
                        [merge_status] = @merge_status,
                        [diverged] = @diverged,
                        [stale_days] = @stale_days,
                        [last_action] = @last_action,
                        [last_updated_at] = @last_updated_at,
                        [last_updated_by] = @last_updated_by
                    WHERE [key] = @key
                    """,
                    data,
                )
                if cursor.rowcount:
                    return
                cursor.execute(
                    """
                    INSERT INTO branches (
                        [key], [branch], [group_name], [project], [created_at], [created_by],
                        [exists_local], [exists_origin], [merge_status], [diverged],
                        [stale_days], [last_action], [last_updated_at], [last_updated_by]
                    ) VALUES (
                        @key, @branch, @group_name, @project, @created_at, @created_by,
                        @exists_local, @exists_origin, @merge_status, @diverged,
                        @stale_days, @last_action, @last_updated_at, @last_updated_by
                    )
                    """,
                    data,
                )
            finally:
                cursor.close()

    def delete_branch(self, key: str) -> None:
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DELETE FROM branches WHERE [key] = @key", {"key": key})
            finally:
                cursor.close()

    # ------------------------------------------------------------------
    # activity
    def append_activity(self, entries: Iterable[dict]) -> None:
        rows = [self._normalize_activity_payload(item) for item in entries]
        if not rows:
            return
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                for row in rows:
                    cursor.execute(
                        """
                        IF NOT EXISTS (
                            SELECT 1 FROM activity_log
                            WHERE [ts] = @ts
                              AND ISNULL([user], '') = ISNULL(@user, '')
                              AND ISNULL([group_name], '') = ISNULL(@group_name, '')
                              AND ISNULL([project], '') = ISNULL(@project, '')
                              AND ISNULL([branch], '') = ISNULL(@branch, '')
                              AND ISNULL([action], '') = ISNULL(@action, '')
                              AND ISNULL([result], '') = ISNULL(@result, '')
                              AND ISNULL([message], '') = ISNULL(@message, '')
                        )
                        BEGIN
                            INSERT INTO activity_log (
                                [ts], [user], [group_name], [project], [branch], [action], [result], [message], [branch_key]
                            ) VALUES (
                                @ts, @user, @group_name, @project, @branch, @action, @result, @message, @branch_key
                            )
                        END
                        """,
                        row,
                    )
            finally:
                cursor.close()

    def fetch_activity(
        self,
        *,
        branch_keys: Optional[Iterable[str]] = None,
        limit: Optional[int] = None,
    ) -> List[dict]:
        where = []
        params: Dict[str, Any] = {}
        if branch_keys:
            keys = list(dict.fromkeys(branch_keys))
            placeholders = []
            for idx, key in enumerate(keys):
                name = f"bk{idx}"
                placeholders.append(f"@{name}")
                params[name] = key
            where.append(f"[branch_key] IN ({', '.join(placeholders)})")
        top_clause = f"TOP {int(limit)} " if limit else ""
        sql = (
            f"SELECT {top_clause}[id], [ts], [user], [group_name], [project], [branch], [action], [result], [message], [branch_key] "
            "FROM activity_log"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY [ts] DESC, [id] DESC"
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                result = _rows_to_dicts(cursor, rows)
            finally:
                cursor.close()
        return result

    def prune_activity(self, valid_keys: Iterable[str]) -> None:
        keys = list(dict.fromkeys(valid_keys))
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                if not keys:
                    cursor.execute("DELETE FROM activity_log")
                else:
                    placeholders = []
                    params: Dict[str, Any] = {}
                    for idx, key in enumerate(keys):
                        name = f"bk{idx}"
                        placeholders.append(f"@{name}")
                        params[name] = key
                    cursor.execute(
                        f"DELETE FROM activity_log WHERE [branch_key] NOT IN ({', '.join(placeholders)})",
                        params,
                    )
            finally:
                cursor.close()

    # ------------------------------------------------------------------
    # sprints
    def fetch_sprints(self, branch_keys: Optional[Sequence[str]] = None) -> List[dict]:
        sql = (
            "SELECT [id], [branch_key], [qa_branch_key], [name], [version], [lead_user], [qa_user], [description], [status], "
            "[closed_at], [closed_by], [created_at], [created_by], [updated_at], [updated_by] FROM sprints"
        )
        params: Dict[str, Any] = {}
        if branch_keys:
            keys = list(dict.fromkeys(branch_keys))
            placeholders = []
            for idx, key in enumerate(keys):
                name = f"bk{idx}"
                placeholders.append(f"@{name}")
                params[name] = key
            sql += f" WHERE [branch_key] IN ({', '.join(placeholders)})"
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                result = _rows_to_dicts(cursor, rows)
            finally:
                cursor.close()
        return result

    def fetch_sprint(self, sprint_id: int) -> Optional[dict]:
        sql = (
            "SELECT TOP 1 [id], [branch_key], [qa_branch_key], [name], [version], [lead_user], [qa_user], [description], [status], "
            "[closed_at], [closed_by], [created_at], [created_by], [updated_at], [updated_by] FROM sprints WHERE [id] = @id"
        )
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, {"id": int(sprint_id)})
                row = cursor.fetchone()
                if not row:
                    return None
                result = _rows_to_dicts(cursor, [row])[0]
            finally:
                cursor.close()
        return result

    def fetch_sprint_by_branch_key(self, branch_key: str) -> Optional[dict]:
        sql = (
            "SELECT TOP 1 [id], [branch_key], [qa_branch_key], [name], [version], [lead_user], [qa_user], [description], [status], "
            "[closed_at], [closed_by], [created_at], [created_by], [updated_at], [updated_by] FROM sprints WHERE [branch_key] = @branch_key"
        )
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, {"branch_key": branch_key})
                row = cursor.fetchone()
                if not row:
                    return None
                result = _rows_to_dicts(cursor, [row])[0]
            finally:
                cursor.close()
        return result

    def upsert_sprint(self, payload: dict) -> int:
        data = self._normalize_sprint(payload)
        sprint_id = data.get("id")
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                if sprint_id:
                    cursor.execute(
                        """
                        UPDATE sprints SET
                            [branch_key] = @branch_key,
                            [qa_branch_key] = @qa_branch_key,
                            [name] = @name,
                            [version] = @version,
                            [lead_user] = @lead_user,
                            [qa_user] = @qa_user,
                            [description] = @description,
                            [status] = @status,
                            [closed_at] = @closed_at,
                            [closed_by] = @closed_by,
                            [created_at] = @created_at,
                            [created_by] = @created_by,
                            [updated_at] = @updated_at,
                            [updated_by] = @updated_by
                        WHERE [id] = @id
                        """,
                        data,
                    )
                    if cursor.rowcount:
                        return int(sprint_id)
                    cursor.execute("SET IDENTITY_INSERT sprints ON")
                    try:
                        cursor.execute(
                            """
                            INSERT INTO sprints (
                                [id], [branch_key], [qa_branch_key], [name], [version], [lead_user], [qa_user], [description],
                                [status], [closed_at], [closed_by], [created_at], [created_by], [updated_at], [updated_by]
                            ) VALUES (
                                @id, @branch_key, @qa_branch_key, @name, @version, @lead_user, @qa_user, @description,
                                @status, @closed_at, @closed_by, @created_at, @created_by, @updated_at, @updated_by
                            )
                            """,
                            data,
                        )
                    finally:
                        cursor.execute("SET IDENTITY_INSERT sprints OFF")
                    return int(sprint_id)
                insert_data = {k: v for k, v in data.items() if k != "id"}
                cursor.execute(
                    """
                    INSERT INTO sprints (
                        [branch_key], [qa_branch_key], [name], [version], [lead_user], [qa_user], [description],
                        [status], [closed_at], [closed_by], [created_at], [created_by], [updated_at], [updated_by]
                    ) OUTPUT INSERTED.id VALUES (
                        @branch_key, @qa_branch_key, @name, @version, @lead_user, @qa_user, @description,
                        @status, @closed_at, @closed_by, @created_at, @created_by, @updated_at, @updated_by
                    )
                    """,
                    insert_data,
                )
                new_id_row = cursor.fetchone()
                return int(new_id_row["id"] if isinstance(new_id_row, dict) else new_id_row[0])
            finally:
                cursor.close()

    def delete_sprint(self, sprint_id: int) -> None:
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DELETE FROM sprints WHERE [id] = @id", {"id": int(sprint_id)})
            finally:
                cursor.close()

    # ------------------------------------------------------------------
    # cards
    def fetch_cards(
        self,
        *,
        sprint_ids: Optional[Sequence[int]] = None,
        branches: Optional[Sequence[str]] = None,
    ) -> List[dict]:
        sql = (
            "SELECT [id], [sprint_id], [branch_key], [title], [ticket_id], [branch], [assignee], [qa_assignee], [description], [unit_tests_url], [qa_url], "
            "[unit_tests_done], [qa_done], [unit_tests_by], [qa_by], [unit_tests_at], [qa_at], [status], [branch_created_by], [branch_created_at], [created_at], [created_by], [updated_at], [updated_by] "
            "FROM cards"
        )
        clauses = []
        params: Dict[str, Any] = {}
        if sprint_ids:
            placeholders = []
            for idx, sprint_id in enumerate(dict.fromkeys(int(s) for s in sprint_ids)):
                name = f"sid{idx}"
                placeholders.append(f"@{name}")
                params[name] = int(sprint_id)
            clauses.append(f"[sprint_id] IN ({', '.join(placeholders)})")
        if branches:
            placeholders = []
            for idx, branch in enumerate(dict.fromkeys(branches)):
                name = f"br{idx}"
                placeholders.append(f"@{name}")
                params[name] = branch
            clauses.append(f"[branch] IN ({', '.join(placeholders)})")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                result = _rows_to_dicts(cursor, rows)
            finally:
                cursor.close()
        return result

    def upsert_card(self, payload: dict) -> int:
        data = self._normalize_card(payload)
        card_id = data.get("id")
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                if card_id:
                    cursor.execute(
                        """
                        UPDATE cards SET
                            [sprint_id] = @sprint_id,
                            [branch_key] = @branch_key,
                            [title] = @title,
                            [ticket_id] = @ticket_id,
                            [branch] = @branch,
                            [assignee] = @assignee,
                            [qa_assignee] = @qa_assignee,
                            [description] = @description,
                            [unit_tests_url] = @unit_tests_url,
                            [qa_url] = @qa_url,
                            [unit_tests_done] = @unit_tests_done,
                            [qa_done] = @qa_done,
                            [unit_tests_by] = @unit_tests_by,
                            [qa_by] = @qa_by,
                            [unit_tests_at] = @unit_tests_at,
                            [qa_at] = @qa_at,
                            [status] = @status,
                            [branch_created_by] = @branch_created_by,
                            [branch_created_at] = @branch_created_at,
                            [created_at] = @created_at,
                            [created_by] = @created_by,
                            [updated_at] = @updated_at,
                            [updated_by] = @updated_by
                        WHERE [id] = @id
                        """,
                        data,
                    )
                    if cursor.rowcount:
                        return int(card_id)
                    cursor.execute("SET IDENTITY_INSERT cards ON")
                    try:
                        cursor.execute(
                            """
                            INSERT INTO cards (
                                [id], [sprint_id], [branch_key], [title], [ticket_id], [branch], [assignee], [qa_assignee], [description],
                                [unit_tests_url], [qa_url], [unit_tests_done], [qa_done], [unit_tests_by], [qa_by], [unit_tests_at],
                                [qa_at], [status], [branch_created_by], [branch_created_at], [created_at], [created_by], [updated_at], [updated_by]
                            ) VALUES (
                                @id, @sprint_id, @branch_key, @title, @ticket_id, @branch, @assignee, @qa_assignee, @description,
                                @unit_tests_url, @qa_url, @unit_tests_done, @qa_done, @unit_tests_by, @qa_by, @unit_tests_at,
                                @qa_at, @status, @branch_created_by, @branch_created_at, @created_at, @created_by, @updated_at, @updated_by
                            )
                            """,
                            data,
                        )
                    finally:
                        cursor.execute("SET IDENTITY_INSERT cards OFF")
                    return int(card_id)
                insert_data = {k: v for k, v in data.items() if k != "id"}
                cursor.execute(
                    """
                    INSERT INTO cards (
                        [sprint_id], [branch_key], [title], [ticket_id], [branch], [assignee], [qa_assignee], [description],
                        [unit_tests_url], [qa_url], [unit_tests_done], [qa_done], [unit_tests_by], [qa_by], [unit_tests_at],
                        [qa_at], [status], [branch_created_by], [branch_created_at], [created_at], [created_by], [updated_at], [updated_by]
                    ) OUTPUT INSERTED.id VALUES (
                        @sprint_id, @branch_key, @title, @ticket_id, @branch, @assignee, @qa_assignee, @description,
                        @unit_tests_url, @qa_url, @unit_tests_done, @qa_done, @unit_tests_by, @qa_by, @unit_tests_at,
                        @qa_at, @status, @branch_created_by, @branch_created_at, @created_at, @created_by, @updated_at, @updated_by
                    )
                    """,
                    insert_data,
                )
                new_id_row = cursor.fetchone()
                return int(new_id_row["id"] if isinstance(new_id_row, dict) else new_id_row[0])
            finally:
                cursor.close()

    def delete_card(self, card_id: int) -> None:
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DELETE FROM cards WHERE [id] = @id", {"id": int(card_id)})
            finally:
                cursor.close()

    # ------------------------------------------------------------------
    # users & roles
    def fetch_users(self) -> List[dict]:
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT [username], [display_name], [email], [active] FROM users ORDER BY [display_name]"
                )
                rows = cursor.fetchall()
                result = _rows_to_dicts(cursor, rows)
            finally:
                cursor.close()
        return result

    def upsert_user(self, payload: dict) -> None:
        data = self._normalize_user(payload)
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    UPDATE users SET
                        [display_name] = @display_name,
                        [email] = @email,
                        [active] = @active
                    WHERE [username] = @username
                    """,
                    data,
                )
                if cursor.rowcount:
                    return
                cursor.execute(
                    """
                    INSERT INTO users ([username], [display_name], [email], [active])
                    VALUES (@username, @display_name, @email, @active)
                    """,
                    data,
                )
            finally:
                cursor.close()

    def delete_user(self, username: str) -> None:
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DELETE FROM users WHERE [username] = @username", {"username": username})
            finally:
                cursor.close()

    def fetch_roles(self) -> List[dict]:
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT [key] AS key, [name], [description] FROM roles ORDER BY [name]")
                rows = cursor.fetchall()
                data = _rows_to_dicts(cursor, rows)
            finally:
                cursor.close()
        return data

    def upsert_role(self, payload: dict) -> None:
        data = self._normalize_role(payload)
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    UPDATE roles SET
                        [name] = @name,
                        [description] = @description
                    WHERE [key] = @key
                    """,
                    data,
                )
                if cursor.rowcount:
                    return
                cursor.execute(
                    """
                    INSERT INTO roles ([key], [name], [description])
                    VALUES (@key, @name, @description)
                    """,
                    data,
                )
            finally:
                cursor.close()

    def delete_role(self, role_key: str) -> None:
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DELETE FROM roles WHERE [key] = @key", {"key": role_key})
            finally:
                cursor.close()

    def fetch_user_roles(self, username: Optional[str] = None) -> List[dict]:
        sql = "SELECT [username], [role_key] FROM user_roles"
        params: Dict[str, Any] = {}
        if username:
            sql += " WHERE [username] = @username"
            params["username"] = username
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                result = _rows_to_dicts(cursor, rows)
            finally:
                cursor.close()
        return result

    def set_user_roles(self, username: str, roles: Sequence[str]) -> None:
        normalized = [role for role in roles if role]
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DELETE FROM user_roles WHERE [username] = @username", {"username": username})
                for role_key in normalized:
                    cursor.execute(
                        """
                        IF NOT EXISTS (SELECT 1 FROM user_roles WHERE [username] = @username AND [role_key] = @role_key)
                        BEGIN
                            INSERT INTO user_roles ([username], [role_key]) VALUES (@username, @role_key)
                        END
                        """,
                        {"username": username, "role_key": role_key},
                    )
            finally:
                cursor.close()

    # ------------------------------------------------------------------
    # normalization helpers
    def _normalize_branch_payload(self, record: dict) -> Dict[str, Any]:
        data = {col: record.get(col) for col in BRANCH_COLUMNS}
        data["exists_local"] = 1 if data.get("exists_local") else 0
        data["exists_origin"] = 1 if data.get("exists_origin") else 0
        data["diverged"] = None if data.get("diverged") is None else (1 if data.get("diverged") else 0)
        data["stale_days"] = None if data.get("stale_days") in (None, "") else int(data.get("stale_days") or 0)
        data["created_at"] = int(data.get("created_at") or 0)
        data["last_updated_at"] = int(data.get("last_updated_at") or 0)
        return data

    def _normalize_activity_payload(self, entry: dict) -> Dict[str, Any]:
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

    def _normalize_sprint(self, payload: dict) -> Dict[str, Any]:
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

    def _normalize_card(self, payload: dict) -> Dict[str, Any]:
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

    def _normalize_user(self, payload: dict) -> Dict[str, Any]:
        return {
            "username": payload.get("username") or "",
            "display_name": payload.get("display_name") or payload.get("username") or "",
            "email": payload.get("email"),
            "active": 1 if payload.get("active", True) else 0,
        }

    def _normalize_role(self, payload: dict) -> Dict[str, Any]:
        return {
            "key": payload.get("key") or "",
            "name": payload.get("name") or payload.get("key") or "",
            "description": payload.get("description") or "",
        }



class BranchHistoryDB:
    """Facade that selects an appropriate backend based on configuration."""

    def __init__(
        self,
        path: Optional[Path] = None,
        *,
        backend: Optional[str] = None,
        url: Optional[str] = None,
        pool_size: Optional[int] = None,
    ) -> None:
        default_path = Path(path) if path is not None else None
        overrides_provided = any(value is not None for value in (backend, url, pool_size))

        settings: Optional[BranchHistorySettings]
        if not overrides_provided:
            settings = BranchHistorySettings.resolve(default_path)
        else:
            try:
                settings = BranchHistorySettings.resolve(default_path)
            except ValueError:
                settings = None

        resolved_backend = backend or (settings.backend if settings else None)
        if not resolved_backend:
            resolved_backend = "sqlserver" if (url or (settings and settings.sqlserver_url)) else "sqlite"

        backend_name = resolved_backend.strip().lower()
        if backend_name not in {"sqlite", "sqlserver"}:
            raise ValueError(f"Backend de historial no soportado: {backend_name}")

        if backend_name == "sqlite":
            sqlite_path = Path(path) if path is not None else (settings.sqlite_path if settings else None)
            if url and not path:
                sqlite_path = Path(url)
            if sqlite_path is None:
                raise ValueError("No se pudo determinar la ruta SQLite para el historial")
            self._backend: BranchHistoryBackend = SQLiteBranchHistoryBackend(Path(sqlite_path))
        else:
            connection_url = url or (settings.sqlserver_url if settings else None)
            if not connection_url:
                raise ValueError("Se requiere FORGEBUILD_BRANCH_HISTORY_URL para SQL Server")
            size = pool_size or (settings.pool_size if settings else 5)
            self._backend = SqlServerBranchHistoryBackend(connection_url, pool_size=size)

        self.backend_name = backend_name

    def __getattr__(self, item):  # pragma: no cover - dynamic delegation
        return getattr(self._backend, item)
