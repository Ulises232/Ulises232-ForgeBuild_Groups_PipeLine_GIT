from __future__ import annotations

from dataclasses import dataclass
import logging
import os
import queue
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlparse

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    def load_dotenv() -> bool:
        return False

try:
    import pymssql
except ImportError:  # pragma: no cover - optional dependency for MSSQL backend
    pymssql = None


load_dotenv()


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
    "group_name",
    "name",
    "version",
    "lead_user",
    "qa_user",
    "company_id",
    "company_sequence",
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
    "group_name",
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
    "company_id",
    "incidence_type_id",
    "closed_at",
    "closed_by",
    "branch_created_by",
    "branch_created_at",
    "created_at",
    "created_by",
    "updated_at",
    "updated_by",
]


COMPANY_COLUMNS = [
    "id",
    "name",
    "group_name",
    "next_sprint_number",
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
    group_name TEXT,
    name TEXT NOT NULL DEFAULT '',
    version TEXT NOT NULL DEFAULT '',
    lead_user TEXT,
    qa_user TEXT,
    company_id INTEGER,
    company_sequence INTEGER,
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
    group_name TEXT,
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
    company_id INTEGER,
    incidence_type_id INTEGER,
    closed_at INTEGER,
    closed_by TEXT,
    branch_created_by TEXT,
    branch_created_at INTEGER,
    created_at INTEGER NOT NULL DEFAULT 0,
    created_by TEXT,
    updated_at INTEGER NOT NULL DEFAULT 0,
    updated_by TEXT,
    FOREIGN KEY(sprint_id) REFERENCES sprints(id) ON DELETE CASCADE
);
"""


def _normalize_branch_payload(record: dict) -> Dict[str, Optional[int]]:
    data = {col: record.get(col) for col in BRANCH_COLUMNS}
    data["exists_local"] = 1 if data.get("exists_local") else 0
    data["exists_origin"] = 1 if data.get("exists_origin") else 0
    data["diverged"] = None if data.get("diverged") is None else (1 if data.get("diverged") else 0)
    data["stale_days"] = None if data.get("stale_days") in (None, "") else int(data.get("stale_days") or 0)
    data["created_at"] = int(data.get("created_at") or 0)
    data["last_updated_at"] = int(data.get("last_updated_at") or 0)
    return data


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


def _normalize_sprint(payload: dict) -> Dict[str, object]:
    data = {
        "id": payload.get("id"),
        "branch_key": payload.get("branch_key") or "",
        "qa_branch_key": payload.get("qa_branch_key") or None,
        "group_name": payload.get("group_name") or None,
        "name": payload.get("name") or "",
        "version": payload.get("version") or "",
        "lead_user": payload.get("lead_user"),
        "qa_user": payload.get("qa_user"),
        "company_id": payload.get("company_id"),
        "company_sequence": payload.get("company_sequence"),
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
    if isinstance(data["group_name"], str):
        data["group_name"] = data["group_name"].strip() or None
    company_id = data.get("company_id")
    try:
        data["company_id"] = int(company_id) if company_id not in (None, "") else None
    except (TypeError, ValueError):
        data["company_id"] = None
    sequence = data.get("company_sequence")
    try:
        data["company_sequence"] = int(sequence) if sequence not in (None, "") else None
    except (TypeError, ValueError):
        data["company_sequence"] = None
    return data


def _normalize_card(payload: dict) -> Dict[str, object]:
    raw_sprint = payload.get("sprint_id")
    sprint_id: Optional[int]
    try:
        sprint_id = int(raw_sprint) if raw_sprint not in (None, "") else None
    except (TypeError, ValueError):
        sprint_id = None
    if sprint_id == 0:
        sprint_id = None

    data = {
        "id": payload.get("id"),
        "sprint_id": sprint_id,
        "branch_key": payload.get("branch_key"),
        "title": payload.get("title") or "",
        "ticket_id": payload.get("ticket_id") or "",
        "branch": payload.get("branch") or "",
        "group_name": payload.get("group_name") or None,
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
        "company_id": payload.get("company_id"),
        "incidence_type_id": payload.get("incidence_type_id"),
        "closed_at": int(payload.get("closed_at") or 0) or None,
        "closed_by": payload.get("closed_by"),
        "branch_created_by": payload.get("branch_created_by"),
        "branch_created_at": int(payload.get("branch_created_at") or 0) or None,
        "created_at": int(payload.get("created_at") or 0),
        "created_by": payload.get("created_by") or "",
        "updated_at": int(payload.get("updated_at") or 0),
        "updated_by": payload.get("updated_by") or "",
    }
    if data["id"] in ("", None):
        data["id"] = None
    if isinstance(data["group_name"], str):
        data["group_name"] = data["group_name"].strip() or None
    company_id = data.get("company_id")
    try:
        data["company_id"] = int(company_id) if company_id not in (None, "") else None
    except (TypeError, ValueError):
        data["company_id"] = None
    incidence_id = data.get("incidence_type_id")
    try:
        data["incidence_type_id"] = (
            int(incidence_id) if incidence_id not in (None, "") else None
        )
    except (TypeError, ValueError):
        data["incidence_type_id"] = None
    return data


def _normalize_user(payload: dict) -> Dict[str, object]:
    data = {
        "username": payload.get("username") or "",
        "display_name": payload.get("display_name") or payload.get("username") or "",
        "email": payload.get("email"),
        "active": 1 if payload.get("active", True) else 0,
    }
    if "require_password_reset" in payload:
        data["require_password_reset"] = 1 if payload.get("require_password_reset") else 0
    return data


def _normalize_role(payload: dict) -> Dict[str, object]:
    return {
        "key": payload.get("key") or "",
        "name": payload.get("name") or payload.get("key") or "",
        "description": payload.get("description") or "",
    }


def _normalize_company(payload: dict) -> Dict[str, object]:
    data = {
        "id": payload.get("id"),
        "name": (payload.get("name") or "").strip(),
        "group_name": (payload.get("group_name") or "").strip() or None,
        "next_sprint_number": payload.get("next_sprint_number"),
        "created_at": int(payload.get("created_at") or 0),
        "created_by": payload.get("created_by"),
        "updated_at": int(payload.get("updated_at") or 0),
        "updated_by": payload.get("updated_by"),
    }
    if data["id"] in ("", None):
        data["id"] = None
    sequence = data.get("next_sprint_number")
    try:
        data["next_sprint_number"] = int(sequence) if sequence not in (None, "") else 1
    except (TypeError, ValueError):
        data["next_sprint_number"] = 1
    if data["next_sprint_number"] <= 0:
        data["next_sprint_number"] = 1
    return data


def _normalize_incidence_type(payload: dict) -> Dict[str, object]:
    data = {
        "id": payload.get("id"),
        "name": (payload.get("name") or "").strip(),
        "icon": payload.get("icon"),
        "created_at": int(payload.get("created_at") or 0),
        "created_by": payload.get("created_by"),
        "updated_at": int(payload.get("updated_at") or 0),
        "updated_by": payload.get("updated_by"),
    }
    if data["id"] in ("", None):
        data["id"] = None
    icon_value = data.get("icon")
    if icon_value is not None and not isinstance(icon_value, (bytes, bytearray)):
        data["icon"] = None
    return data


@dataclass(slots=True)
class Sprint:
    """Model representing a sprint/version planning entry."""

    id: Optional[int]
    branch_key: str
    name: str
    version: str
    group_name: Optional[str] = None
    qa_branch_key: Optional[str] = None
    lead_user: Optional[str] = None
    qa_user: Optional[str] = None
    company_id: Optional[int] = None
    company_sequence: Optional[int] = None
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
    sprint_id: Optional[int] = None
    branch_key: Optional[str] = None
    title: str = ""
    ticket_id: str = ""
    branch: str = ""
    group_name: Optional[str] = None
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
    company_id: Optional[int] = None
    incidence_type_id: Optional[int] = None
    closed_at: Optional[int] = None
    closed_by: Optional[str] = None
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
    has_password: bool = False
    require_password_reset: bool = False
    password_changed_at: Optional[int] = None
    active_since: Optional[int] = None


@dataclass(slots=True)
class Role:
    """Role that can be assigned to users."""

    key: str
    name: str
    description: str = ""


@dataclass(slots=True)
class Company:
    """Catalog entry representing a company."""

    id: Optional[int]
    name: str
    group_name: Optional[str] = None
    next_sprint_number: int = 1
    created_at: int = 0
    created_by: Optional[str] = None
    updated_at: int = 0
    updated_by: Optional[str] = None


@dataclass(slots=True)
class IncidenceType:
    """Catalog entry describing an incident type."""

    id: Optional[int]
    name: str
    icon: Optional[bytes] = None
    created_at: int = 0
    created_by: Optional[str] = None
    updated_at: int = 0
    updated_by: Optional[str] = None


def _parse_sqlserver_dsn(url: str) -> Tuple[Dict[str, Optional[str]], Dict[str, str]]:
    parsed = urlparse(url)
    if parsed.scheme.lower() not in {"mssql", "sqlserver", "tds"}:
        raise ValueError(f"Esquema de conexión no soportado para SQL Server: {parsed.scheme}")
    username = parsed.username or ""
    password = parsed.password or ""
    server = parsed.hostname or "localhost"
    port = parsed.port or 1433
    database = parsed.path.lstrip("/") or None
    query = dict(parse_qsl(parsed.query))
    return (
        {
            "server": server,
            "user": username or query.get("user"),
            "password": password or query.get("password"),
            "database": database or query.get("database") or query.get("db"),
            "port": port,
        },
        query,
    )


class _SqlServerConnectionPool:
    """Pequeño pool de conexiones reutilizables para SQL Server."""

    def __init__(self, url: str, *, max_size: int = 5):
        if pymssql is None:
            raise RuntimeError(
                "pymssql es requerido para el backend SQL Server pero no está instalado."
            )
        self._config, self._options = _parse_sqlserver_dsn(url)
        self._max_size = max(1, int(max_size or 1))
        self._pool: "queue.Queue[pymssql.Connection]" = queue.Queue()
        self._lock = threading.Lock()
        self._created = 0

    def _create_connection(self) -> "pymssql.Connection":
        conn = pymssql.connect(
            server=self._config["server"],
            user=self._config["user"],
            password=self._config["password"],
            database=self._config["database"],
            port=self._config["port"],
            charset=self._options.get("charset", "utf8"),
            login_timeout=int(self._options.get("login_timeout", 10)),
            timeout=int(self._options.get("timeout", 30)),
            as_dict=True,
        )
        return conn

    def _acquire(self) -> "pymssql.Connection":
        try:
            conn = self._pool.get_nowait()
        except queue.Empty:
            with self._lock:
                if self._created < self._max_size:
                    conn = self._create_connection()
                    self._created += 1
                    return conn
            conn = self._pool.get()
        return conn

    def _release(self, conn: "pymssql.Connection") -> None:
        try:
            if conn.closed:
                raise AttributeError
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            with self._lock:
                self._created = max(0, self._created - 1)
            return

        try:
            self._pool.put_nowait(conn)
        except queue.Full:
            conn.close()
            with self._lock:
                self._created = max(0, self._created - 1)

    @contextmanager
    def connection(self) -> Iterator["pymssql.Connection"]:
        conn = self._acquire()
        try:
            yield conn
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            self._release(conn)



class _SqlServerBranchHistory:
    """Implementación de persistencia sobre SQL Server 2019."""

    def __init__(self, url: str, *, pool_size: int = 5):
        self._pool = _SqlServerConnectionPool(url, max_size=pool_size)
        self._ensure_schema()

    @contextmanager
    def _connect(self) -> Iterator["pymssql.Connection"]:
        with self._pool.connection() as conn:
            yield conn

    def _system_username(self) -> str:
        for env_key in ("BRANCH_HISTORY_USERNAME", "USERNAME", "USER"):
            value = os.environ.get(env_key)
            if value:
                trimmed = value.strip()
                if trimmed:
                    return trimmed
        return "system"

    # ------------------------------------------------------------------
    # inicialización
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
                    merge_status NVARCHAR(64) NULL,
                    diverged BIT NULL,
                    stale_days INT NULL,
                    last_action NVARCHAR(64) NULL,
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
                    action NVARCHAR(64) NULL,
                    result NVARCHAR(64) NULL,
                    message NVARCHAR(1024) NULL,
                    branch_key NVARCHAR(512) NULL,
                    CONSTRAINT uq_activity UNIQUE(ts, [user], group_name, project, branch, action, result, message)
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
                    version NVARCHAR(128) NOT NULL DEFAULT '',
                    lead_user NVARCHAR(255) NULL,
                    qa_user NVARCHAR(255) NULL,
                    company_id INT NULL,
                    company_sequence INT NULL,
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
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'sprint_groups')
            BEGIN
                CREATE TABLE sprint_groups (
                    sprint_id INT NOT NULL PRIMARY KEY,
                    group_name NVARCHAR(255) NOT NULL,
                    CONSTRAINT fk_sprint_groups_sprint FOREIGN KEY (sprint_id) REFERENCES sprints(id) ON DELETE CASCADE
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'cards')
            BEGIN
                CREATE TABLE cards (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    sprint_id INT NULL,
                    branch_key NVARCHAR(512) NULL,
                    title NVARCHAR(255) NOT NULL DEFAULT '',
                    ticket_id NVARCHAR(128) NULL,
                    branch NVARCHAR(255) NOT NULL DEFAULT '',
                    group_name NVARCHAR(255) NULL,
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
                    status NVARCHAR(32) NOT NULL DEFAULT 'pending',
                    company_id INT NULL,
                    incidence_type_id INT NULL,
                    closed_at BIGINT NULL,
                    closed_by NVARCHAR(255) NULL,
                    branch_created_by NVARCHAR(255) NULL,
                    branch_created_at BIGINT NULL,
                    created_at BIGINT NOT NULL DEFAULT 0,
                    created_by NVARCHAR(255) NULL,
                    updated_at BIGINT NOT NULL DEFAULT 0,
                    updated_by NVARCHAR(255) NULL,
                    CONSTRAINT fk_cards_sprint FOREIGN KEY (sprint_id) REFERENCES sprints(id) ON DELETE SET NULL
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'catalog_incidence_types')
            BEGIN
                CREATE TABLE catalog_incidence_types (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(255) NOT NULL UNIQUE,
                    icon VARBINARY(MAX) NULL,
                    created_at BIGINT NOT NULL DEFAULT 0,
                    created_by NVARCHAR(255) NULL,
                    updated_at BIGINT NOT NULL DEFAULT 0,
                    updated_by NVARCHAR(255) NULL
                );
            END
            """,
            """
            IF EXISTS (
                SELECT 1
                FROM sys.columns
                WHERE object_id = OBJECT_ID('cards')
                  AND name = 'sprint_id'
                  AND is_nullable = 0
            )
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM sys.foreign_keys
                    WHERE name = 'fk_cards_sprint'
                      AND parent_object_id = OBJECT_ID('cards')
                )
                    ALTER TABLE cards DROP CONSTRAINT fk_cards_sprint;
                ALTER TABLE cards ALTER COLUMN sprint_id INT NULL;
                IF NOT EXISTS (
                    SELECT 1
                    FROM sys.foreign_keys
                    WHERE name = 'fk_cards_sprint'
                      AND parent_object_id = OBJECT_ID('cards')
                )
                    ALTER TABLE cards
                        ADD CONSTRAINT fk_cards_sprint FOREIGN KEY (sprint_id) REFERENCES sprints(id) ON DELETE SET NULL;
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'card_sprint_links')
            BEGIN
                CREATE TABLE card_sprint_links (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    card_id INT NOT NULL,
                    sprint_id INT NULL,
                    assigned_at BIGINT NOT NULL DEFAULT 0,
                    assigned_by NVARCHAR(255) NULL,
                    unassigned_at BIGINT NULL,
                    unassigned_by NVARCHAR(255) NULL,
                    CONSTRAINT fk_card_sprint_card FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE,
                    CONSTRAINT fk_card_sprint_sprint FOREIGN KEY (sprint_id) REFERENCES sprints(id)
                );
            END
            """,
            """
            IF EXISTS (
                SELECT 1
                FROM sys.foreign_keys
                WHERE name = 'fk_card_sprint_sprint'
                  AND parent_object_id = OBJECT_ID('card_sprint_links')
                  AND delete_referential_action <> 0
            )
            BEGIN
                ALTER TABLE card_sprint_links DROP CONSTRAINT fk_card_sprint_sprint;
                ALTER TABLE card_sprint_links ALTER COLUMN sprint_id INT NULL;
                ALTER TABLE card_sprint_links
                    ADD CONSTRAINT fk_card_sprint_sprint FOREIGN KEY (sprint_id) REFERENCES sprints(id);
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'card_company_links')
            BEGIN
                CREATE TABLE card_company_links (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    card_id INT NOT NULL,
                    company_id INT NOT NULL,
                    linked_at BIGINT NOT NULL DEFAULT 0,
                    linked_by NVARCHAR(255) NULL,
                    unlinked_at BIGINT NULL,
                    unlinked_by NVARCHAR(255) NULL,
                    CONSTRAINT fk_card_company_card FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE,
                    CONSTRAINT fk_card_company_company FOREIGN KEY (company_id) REFERENCES catalog_companies(id) ON DELETE CASCADE
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'card_branch_links')
            BEGIN
                CREATE TABLE card_branch_links (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    card_id INT NOT NULL,
                    branch_key NVARCHAR(512) NOT NULL,
                    linked_at BIGINT NOT NULL DEFAULT 0,
                    linked_by NVARCHAR(255) NULL,
                    unlinked_at BIGINT NULL,
                    unlinked_by NVARCHAR(255) NULL,
                    CONSTRAINT fk_card_branch_card FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
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
                    active BIT NOT NULL DEFAULT 1,
                    password_hash NVARCHAR(512) NULL,
                    password_salt NVARCHAR(512) NULL,
                    password_algo NVARCHAR(128) NULL,
                    password_changed_at BIGINT NULL,
                    require_password_reset BIT NOT NULL DEFAULT 0,
                    active_since BIGINT NULL
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'catalog_companies')
            BEGIN
                CREATE TABLE catalog_companies (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(255) NOT NULL UNIQUE,
                    group_name NVARCHAR(255) NULL,
                    created_at BIGINT NOT NULL DEFAULT 0,
                    created_by NVARCHAR(255) NULL,
                    updated_at BIGINT NOT NULL DEFAULT 0,
                    updated_by NVARCHAR(255) NULL
                );
            END
            """,
            """
            IF COL_LENGTH('users', 'password_hash') IS NULL
            BEGIN
                ALTER TABLE users ADD password_hash NVARCHAR(512) NULL;
            END
            """,
            """
            IF COL_LENGTH('users', 'password_salt') IS NULL
            BEGIN
                ALTER TABLE users ADD password_salt NVARCHAR(512) NULL;
            END
            """,
            """
            IF COL_LENGTH('users', 'password_algo') IS NULL
            BEGIN
                ALTER TABLE users ADD password_algo NVARCHAR(128) NULL;
            END
            """,
            """
            IF COL_LENGTH('users', 'password_changed_at') IS NULL
            BEGIN
                ALTER TABLE users ADD password_changed_at BIGINT NULL;
            END
            """,
            """
            IF COL_LENGTH('users', 'require_password_reset') IS NULL
            BEGIN
                ALTER TABLE users ADD require_password_reset BIT NULL;
            END
            """,
            """
            IF COL_LENGTH('users', 'require_password_reset') IS NOT NULL
            BEGIN
                UPDATE users
                   SET require_password_reset = 0
                 WHERE require_password_reset IS NULL;
            END
            """,
            """
            IF COL_LENGTH('users', 'require_password_reset') IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1
                      FROM sys.default_constraints dc
                      JOIN sys.columns c
                        ON c.object_id = dc.parent_object_id
                       AND c.column_id = dc.parent_column_id
                     WHERE dc.parent_object_id = OBJECT_ID('users')
                       AND c.name = 'require_password_reset'
                )
            BEGIN
                ALTER TABLE users
                ADD CONSTRAINT DF_users_require_password_reset DEFAULT (0) FOR require_password_reset;
            END
            """,
            """
            IF COL_LENGTH('users', 'require_password_reset') IS NOT NULL
            BEGIN
                ALTER TABLE users ALTER COLUMN require_password_reset BIT NOT NULL;
            END
            """,
            """
            IF COL_LENGTH('users', 'active_since') IS NULL
            BEGIN
                ALTER TABLE users ADD active_since BIGINT NULL;
            END
            """,
            """
            IF COL_LENGTH('sprints', 'company_id') IS NULL
            BEGIN
                ALTER TABLE sprints ADD company_id INT NULL;
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'sprint_groups')
            BEGIN
                CREATE TABLE sprint_groups (
                    sprint_id INT NOT NULL PRIMARY KEY,
                    group_name NVARCHAR(255) NOT NULL,
                    CONSTRAINT fk_sprint_groups_sprint FOREIGN KEY (sprint_id) REFERENCES sprints(id) ON DELETE CASCADE
                );
            END
            IF COL_LENGTH('sprints', 'group_name') IS NOT NULL
            BEGIN
                INSERT INTO sprint_groups (sprint_id, group_name)
                SELECT id, group_name
                FROM sprints
                WHERE group_name IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM sprint_groups WHERE sprint_groups.sprint_id = sprints.id
                  );
                ALTER TABLE sprints DROP COLUMN group_name;
            END
            """,
            """
            IF COL_LENGTH('sprints', 'company_sequence') IS NULL
            BEGIN
                ALTER TABLE sprints ADD company_sequence INT NULL;
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'roles')
            BEGIN
                CREATE TABLE roles (
                    [key] NVARCHAR(128) NOT NULL PRIMARY KEY,
                    name NVARCHAR(255) NOT NULL,
                    description NVARCHAR(512) NULL
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'user_roles')
            BEGIN
                CREATE TABLE user_roles (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    username NVARCHAR(255) NOT NULL,
                    role_key NVARCHAR(128) NOT NULL,
                    CONSTRAINT uq_user_roles UNIQUE(username, role_key),
                    CONSTRAINT fk_user_roles_user FOREIGN KEY (username) REFERENCES users(username) ON DELETE CASCADE,
                    CONSTRAINT fk_user_roles_role FOREIGN KEY (role_key) REFERENCES roles([key]) ON DELETE CASCADE
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'branch_local_users')
            BEGIN
                CREATE TABLE branch_local_users (
                    branch_key NVARCHAR(255) NOT NULL,
                    username NVARCHAR(255) NOT NULL,
                    state NVARCHAR(32) NOT NULL DEFAULT 'absent',
                    location NVARCHAR(1024) NULL,
                    updated_at BIGINT NOT NULL DEFAULT 0,
                    CONSTRAINT pk_branch_local_users PRIMARY KEY (branch_key, username),
                    CONSTRAINT fk_branch_local_users_branch FOREIGN KEY (branch_key) REFERENCES branches([key]) ON DELETE CASCADE
                );
            END
            """,
            """
            IF COL_LENGTH('branch_local_users', 'branch_key') IS NOT NULL
                AND COL_LENGTH('branch_local_users', 'branch_key') <> 255
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM sys.indexes
                    WHERE name = 'idx_branch_local_users_username'
                        AND object_id = OBJECT_ID('branch_local_users')
                )
                BEGIN
                    DROP INDEX idx_branch_local_users_username ON branch_local_users;
                END
                IF EXISTS (
                    SELECT 1 FROM sys.indexes
                    WHERE name = 'idx_branch_local_users_state'
                        AND object_id = OBJECT_ID('branch_local_users')
                )
                BEGIN
                    DROP INDEX idx_branch_local_users_state ON branch_local_users;
                END
                IF EXISTS (
                    SELECT 1 FROM sys.foreign_keys
                    WHERE name = 'fk_branch_local_users_branch'
                        AND parent_object_id = OBJECT_ID('branch_local_users')
                )
                BEGIN
                    ALTER TABLE branch_local_users DROP CONSTRAINT fk_branch_local_users_branch;
                END
                IF EXISTS (
                    SELECT 1 FROM sys.key_constraints
                    WHERE name = 'pk_branch_local_users'
                        AND parent_object_id = OBJECT_ID('branch_local_users')
                )
                BEGIN
                    ALTER TABLE branch_local_users DROP CONSTRAINT pk_branch_local_users;
                END
                ALTER TABLE branch_local_users ALTER COLUMN branch_key NVARCHAR(255) NOT NULL;
                ALTER TABLE branch_local_users ADD CONSTRAINT pk_branch_local_users PRIMARY KEY (branch_key, username);
                ALTER TABLE branch_local_users WITH CHECK ADD CONSTRAINT fk_branch_local_users_branch FOREIGN KEY (branch_key) REFERENCES branches([key]) ON DELETE CASCADE;
            END
            """,
            """
            IF NOT EXISTS (
                SELECT name FROM sys.indexes WHERE name = 'idx_activity_branch_key'
                    AND object_id = OBJECT_ID('activity_log')
            )
            BEGIN
                CREATE INDEX idx_activity_branch_key ON activity_log(branch_key);
            END
            """,
            """
            IF NOT EXISTS (
                SELECT name FROM sys.indexes WHERE name = 'idx_activity_ts'
                    AND object_id = OBJECT_ID('activity_log')
            )
            BEGIN
                CREATE INDEX idx_activity_ts ON activity_log(ts DESC, id DESC);
            END
            """,
            """
            IF NOT EXISTS (
                SELECT name FROM sys.indexes WHERE name = 'idx_sprints_branch'
                    AND object_id = OBJECT_ID('sprints')
            )
            BEGIN
                CREATE INDEX idx_sprints_branch ON sprints(branch_key);
            END
            """,
            """
            IF NOT EXISTS (
                SELECT name FROM sys.indexes WHERE name = 'idx_cards_sprint'
                    AND object_id = OBJECT_ID('cards')
            )
            BEGIN
                CREATE INDEX idx_cards_sprint ON cards(sprint_id);
            END
            """,
            """
            IF NOT EXISTS (
                SELECT name FROM sys.indexes WHERE name = 'idx_cards_branch'
                    AND object_id = OBJECT_ID('cards')
            )
            BEGIN
                CREATE INDEX idx_cards_branch ON cards(branch);
            END
            """,
            """
            IF NOT EXISTS (
                SELECT name FROM sys.indexes WHERE name = 'idx_card_sprint_active'
                    AND object_id = OBJECT_ID('card_sprint_links')
            )
            BEGIN
                CREATE INDEX idx_card_sprint_active ON card_sprint_links(card_id, unassigned_at);
            END
            """,
            """
            IF NOT EXISTS (
                SELECT name FROM sys.indexes WHERE name = 'idx_card_sprint_by_sprint'
                    AND object_id = OBJECT_ID('card_sprint_links')
            )
            BEGIN
                CREATE INDEX idx_card_sprint_by_sprint ON card_sprint_links(sprint_id, unassigned_at);
            END
            """,
            """
            IF NOT EXISTS (
                SELECT name FROM sys.indexes WHERE name = 'idx_card_company_active'
                    AND object_id = OBJECT_ID('card_company_links')
            )
            BEGIN
                CREATE INDEX idx_card_company_active ON card_company_links(card_id, unlinked_at);
            END
            """,
            """
            IF NOT EXISTS (
                SELECT name FROM sys.indexes WHERE name = 'idx_card_branch_active'
                    AND object_id = OBJECT_ID('card_branch_links')
            )
            BEGIN
                CREATE INDEX idx_card_branch_active ON card_branch_links(card_id, unlinked_at);
            END
            """,
            """
            IF COL_LENGTH('cards', 'group_name') IS NULL
            BEGIN
                ALTER TABLE cards ADD group_name NVARCHAR(255) NULL;
            END
            """,
            """
            IF COL_LENGTH('cards', 'company_id') IS NULL
            BEGIN
                ALTER TABLE cards ADD company_id INT NULL;
            END
            """,
            """
            IF COL_LENGTH('cards', 'incidence_type_id') IS NULL
            BEGIN
                ALTER TABLE cards ADD incidence_type_id INT NULL;
            END
            """,
            """
            IF COL_LENGTH('cards', 'closed_at') IS NULL
            BEGIN
                ALTER TABLE cards ADD closed_at BIGINT NULL;
            END
            """,
            """
            IF COL_LENGTH('cards', 'closed_by') IS NULL
            BEGIN
                ALTER TABLE cards ADD closed_by NVARCHAR(255) NULL;
            END
            """,
            """
            IF COL_LENGTH('catalog_companies', 'next_sprint_number') IS NULL
            BEGIN
                ALTER TABLE catalog_companies ADD next_sprint_number INT NOT NULL DEFAULT 1;
            END
            """,
            """
            IF COL_LENGTH('catalog_companies', 'next_sprint_number') IS NOT NULL
            BEGIN
                UPDATE catalog_companies
                   SET next_sprint_number = 1
                 WHERE next_sprint_number IS NULL OR next_sprint_number <= 0;
            END
            """,
            """
            IF COL_LENGTH('catalog_incidence_types', 'color') IS NOT NULL
            BEGIN
                ALTER TABLE catalog_incidence_types DROP COLUMN color;
            END
            """,
            """
            IF COL_LENGTH('catalog_incidence_types', 'icon') IS NULL
            BEGIN
                ALTER TABLE catalog_incidence_types ADD icon VARBINARY(MAX) NULL;
            END
            """,
            """
            IF COL_LENGTH('catalog_incidence_types', 'created_at') IS NULL
            BEGIN
                ALTER TABLE catalog_incidence_types ADD created_at BIGINT NOT NULL DEFAULT 0;
            END
            """,
            """
            IF COL_LENGTH('catalog_incidence_types', 'created_by') IS NULL
            BEGIN
                ALTER TABLE catalog_incidence_types ADD created_by NVARCHAR(255) NULL;
            END
            """,
            """
            IF COL_LENGTH('catalog_incidence_types', 'updated_at') IS NULL
            BEGIN
                ALTER TABLE catalog_incidence_types ADD updated_at BIGINT NOT NULL DEFAULT 0;
            END
            """,
            """
            IF COL_LENGTH('catalog_incidence_types', 'updated_by') IS NULL
            BEGIN
                ALTER TABLE catalog_incidence_types ADD updated_by NVARCHAR(255) NULL;
            END
            """,
            """
            IF NOT EXISTS (
                SELECT name FROM sys.indexes WHERE name = 'idx_branch_local_users_username'
                    AND object_id = OBJECT_ID('branch_local_users')
            )
            BEGIN
                CREATE INDEX idx_branch_local_users_username ON branch_local_users(username);
            END
            """,
            """
            IF NOT EXISTS (
                SELECT name FROM sys.indexes WHERE name = 'idx_branch_local_users_state'
                    AND object_id = OBJECT_ID('branch_local_users')
            )
            BEGIN
                CREATE INDEX idx_branch_local_users_state ON branch_local_users(state);
            END
            """,
        ]

        with self._connect() as conn:
            cursor = conn.cursor()
            for stmt in statements:
                try:
                    cursor.execute(stmt)
                except pymssql.ProgrammingError as exc:
                    logging.debug("Error al aplicar esquema en SQL Server: %s", exc)
                    if "IF NOT EXISTS" in stmt:
                        continue
                    raise

    # ------------------------------------------------------------------
    # helpers
    def _execute_upsert_branch(self, cursor: "pymssql.Cursor", data: Dict[str, object]) -> None:
        update_sql = """
            UPDATE branches
               SET branch=%s, group_name=%s, project=%s, created_at=%s, created_by=%s,
                   exists_local=%s, exists_origin=%s, merge_status=%s, diverged=%s,
                   stale_days=%s, last_action=%s, last_updated_at=%s, last_updated_by=%s
             WHERE [key]=%s
        """
        params = (
            data.get("branch"),
            data.get("group_name"),
            data.get("project"),
            data.get("created_at"),
            data.get("created_by"),
            data.get("exists_local"),
            data.get("exists_origin"),
            data.get("merge_status"),
            data.get("diverged"),
            data.get("stale_days"),
            data.get("last_action"),
            data.get("last_updated_at"),
            data.get("last_updated_by"),
            data.get("key"),
        )
        cursor.execute(update_sql, params)
        if cursor.rowcount:
            return
        insert_sql = """
            INSERT INTO branches (
                [key], branch, group_name, project, created_at, created_by,
                exists_local, exists_origin, merge_status, diverged, stale_days,
                last_action, last_updated_at, last_updated_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            insert_sql,
            (
                data.get("key"),
                data.get("branch"),
                data.get("group_name"),
                data.get("project"),
                data.get("created_at"),
                data.get("created_by"),
                data.get("exists_local"),
                data.get("exists_origin"),
                data.get("merge_status"),
                data.get("diverged"),
                data.get("stale_days"),
                data.get("last_action"),
                data.get("last_updated_at"),
                data.get("last_updated_by"),
            ),
        )

    def _execute_upsert_branch_local_user(
        self,
        cursor: "pymssql.Cursor",
        data: Dict[str, object],
    ) -> None:
        update_sql = """
            UPDATE branch_local_users
               SET state=%s, location=%s, updated_at=%s
             WHERE branch_key=%s AND username=%s
        """
        params = (
            data.get("state"),
            data.get("location"),
            data.get("updated_at"),
            data.get("branch_key"),
            data.get("username"),
        )
        cursor.execute(update_sql, params)
        if cursor.rowcount:
            return
        insert_sql = """
            INSERT INTO branch_local_users (
                branch_key, username, state, location, updated_at
            ) VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(
            insert_sql,
            (
                data.get("branch_key"),
                data.get("username"),
                data.get("state"),
                data.get("location"),
                data.get("updated_at"),
            ),
        )

    def _insert_ignore_activity(self, cursor: "pymssql.Cursor", data: Dict[str, object]) -> None:
        sql = """
            INSERT INTO activity_log (
                ts, [user], group_name, project, branch, action,
                result, message, branch_key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            data.get("ts"),
            data.get("user"),
            data.get("group_name"),
            data.get("project"),
            data.get("branch"),
            data.get("action"),
            data.get("result"),
            data.get("message"),
            data.get("branch_key"),
        )
        try:
            cursor.execute(sql, params)
        except pymssql.IntegrityError:
            pass

    def _execute_upsert_generic(
        self,
        cursor: "pymssql.Cursor",
        table: str,
        key_column: str,
        data: Dict[str, object],
        columns: Sequence[str],
    ) -> object:
        quoted_table = self._quote_identifier(table)
        quoted_key = self._quote_identifier(key_column)
        setters = ", ".join(
            f"{self._quote_identifier(col)}=%s" for col in columns if col != key_column
        )
        update_sql = f"UPDATE {quoted_table} SET {setters} WHERE {quoted_key}=%s"
        update_params = [data.get(col) for col in columns if col != key_column]
        update_params.append(data.get(key_column))
        cursor.execute(update_sql, tuple(update_params))
        if cursor.rowcount:
            if key_column == "id" and data.get("id") is None:
                cursor.execute("SELECT SCOPE_IDENTITY() AS id")
                row = cursor.fetchone()
                return int(row["id"]) if row and row.get("id") is not None else 0
            if key_column == "id":
                return int(data.get(key_column) or 0)
            return data.get(key_column)

        insert_columns = list(columns)
        if key_column == "id" and not data.get("id"):
            insert_columns = [col for col in columns if col != "id"]
        placeholders = ",".join("%s" for _ in insert_columns)
        quoted_insert_cols = [self._quote_identifier(col) for col in insert_columns]
        insert_sql = (
            f"INSERT INTO {quoted_table} ({', '.join(quoted_insert_cols)}) VALUES ({placeholders})"
        )
        cursor.execute(insert_sql, tuple(data.get(col) for col in insert_columns))
        if key_column == "id":
            cursor.execute("SELECT SCOPE_IDENTITY() AS id")
            row = cursor.fetchone()
            return int(row["id"]) if row and row.get("id") is not None else 0
        return data.get(key_column)

    def _sync_card_links(
        self,
        cursor: "pymssql.Cursor",
        card_id: int,
        data: Dict[str, object],
    ) -> None:
        username = data.get("updated_by") or data.get("created_by")
        timestamp = int(data.get("updated_at") or data.get("created_at") or time.time())
        sprint_id = data.get("sprint_id") or None
        if sprint_id in (0, ""):
            sprint_id = None
        company_id = data.get("company_id") or None
        if company_id in (0, ""):
            company_id = None
        branch_key = (data.get("branch_key") or "").strip() or None
        self._sync_card_sprint_link(cursor, card_id, sprint_id, username, timestamp)
        self._sync_card_company_link(cursor, card_id, company_id, username, timestamp)
        self._sync_card_branch_link(cursor, card_id, branch_key, username, timestamp)

    def _sync_card_sprint_link(
        self,
        cursor: "pymssql.Cursor",
        card_id: int,
        sprint_id: Optional[int],
        username: Optional[str],
        timestamp: int,
    ) -> None:
        cursor.execute(
            "SELECT id, sprint_id FROM card_sprint_links WHERE card_id=%s AND unassigned_at IS NULL",
            (int(card_id),),
        )
        rows = cursor.fetchall() or []
        active_id: Optional[int] = None
        for row in rows:
            linked_id = int(row.get("id"))
            current = int(row.get("sprint_id") or 0) or None
            if sprint_id is not None and current == sprint_id and active_id is None:
                active_id = linked_id
                continue
            cursor.execute(
                "UPDATE card_sprint_links SET unassigned_at=%s, unassigned_by=%s WHERE id=%s",
                (timestamp, username, linked_id),
            )
        if sprint_id is None or active_id is not None:
            return
        cursor.execute(
            """
            INSERT INTO card_sprint_links (
                card_id, sprint_id, assigned_at, assigned_by
            ) VALUES (%s, %s, %s, %s)
            """,
            (int(card_id), int(sprint_id), timestamp, username),
        )

    def _sync_card_company_link(
        self,
        cursor: "pymssql.Cursor",
        card_id: int,
        company_id: Optional[int],
        username: Optional[str],
        timestamp: int,
    ) -> None:
        cursor.execute(
            "SELECT id, company_id FROM card_company_links WHERE card_id=%s AND unlinked_at IS NULL",
            (int(card_id),),
        )
        rows = cursor.fetchall() or []
        active_id: Optional[int] = None
        for row in rows:
            linked_id = int(row.get("id"))
            current = int(row.get("company_id") or 0) or None
            if company_id is not None and current == company_id and active_id is None:
                active_id = linked_id
                continue
            cursor.execute(
                "UPDATE card_company_links SET unlinked_at=%s, unlinked_by=%s WHERE id=%s",
                (timestamp, username, linked_id),
            )
        if company_id is None or active_id is not None:
            return
        cursor.execute(
            """
            INSERT INTO card_company_links (
                card_id, company_id, linked_at, linked_by
            ) VALUES (%s, %s, %s, %s)
            """,
            (int(card_id), int(company_id), timestamp, username),
        )

    def _sync_card_branch_link(
        self,
        cursor: "pymssql.Cursor",
        card_id: int,
        branch_key: Optional[str],
        username: Optional[str],
        timestamp: int,
    ) -> None:
        cursor.execute(
            "SELECT id, branch_key FROM card_branch_links WHERE card_id=%s AND unlinked_at IS NULL",
            (int(card_id),),
        )
        rows = cursor.fetchall() or []
        active_id: Optional[int] = None
        normalized_branch = (branch_key or "").strip() or None
        for row in rows:
            linked_id = int(row.get("id"))
            current_branch = (row.get("branch_key") or "").strip()
            if normalized_branch and current_branch == normalized_branch and active_id is None:
                active_id = linked_id
                continue
            cursor.execute(
                "UPDATE card_branch_links SET unlinked_at=%s, unlinked_by=%s WHERE id=%s",
                (timestamp, username, linked_id),
            )
        if not normalized_branch or active_id is not None:
            return
        cursor.execute(
            """
            INSERT INTO card_branch_links (
                card_id, branch_key, linked_at, linked_by
            ) VALUES (%s, %s, %s, %s)
            """,
            (int(card_id), normalized_branch, timestamp, username),
        )

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        identifier = identifier.strip()
        if identifier.startswith("[") and identifier.endswith("]"):
            return identifier
        return f"[{identifier}]"

    # ------------------------------------------------------------------
    # API pública
    def replace_branches(self, records: Iterable[dict]) -> None:
        payload = [_normalize_branch_payload(rec) for rec in records]
        if not payload:
            return
        with self._connect() as conn:
            cursor = conn.cursor()
            for data in payload:
                self._execute_upsert_branch(cursor, data)

    def upsert_branch(self, record: dict) -> None:
        data = _normalize_branch_payload(record)
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute_upsert_branch(cursor, data)

    def delete_branch(self, key: str) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM branches WHERE [key]=%s", (key,))

    def fetch_branches(
        self,
        *,
        filter_origin: bool = False,
        username: Optional[str] = None,
    ) -> List[dict]:
        sql = (
            "SELECT b.[key], b.branch, b.group_name, b.project, b.created_at, b.created_by,"
            " b.exists_local, b.exists_origin, b.merge_status, b.diverged, b.stale_days,"
            " b.last_action, b.last_updated_at, b.last_updated_by,"
            " u.state AS local_state, u.location AS local_location, u.updated_at AS local_updated_at"
            " FROM branches AS b"
        )
        params: List[object] = []
        if username:
            sql += " LEFT JOIN branch_local_users AS u ON u.branch_key = b.[key] AND u.username = %s"
            params.append(username)
        else:
            sql += " LEFT JOIN branch_local_users AS u ON u.branch_key = b.[key]"
        where_clauses: List[str] = []
        if filter_origin:
            where_clauses.append("b.exists_origin = 1")
        if username:
            where_clauses.append(
                "(b.exists_origin = 1 OR u.username IS NOT NULL OR b.created_by = %s OR b.last_updated_by = %s)"
            )
            params.extend([username, username])
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY b.last_updated_at DESC, b.[key]"
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def fetch_branch_local_users(
        self,
        *,
        branch_keys: Optional[Sequence[str]] = None,
        username: Optional[str] = None,
    ) -> List[dict]:
        sql = (
            "SELECT branch_key, username, state, location, updated_at"
            " FROM branch_local_users"
        )
        params: List[object] = []
        conditions: List[str] = []
        keys = [key for key in (branch_keys or []) if key]
        if keys:
            placeholders = ",".join("%s" for _ in keys)
            conditions.append(f"branch_key IN ({placeholders})")
            params.extend(keys)
        if username:
            conditions.append("username=%s")
            params.append(username)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def upsert_branch_local_user(
        self,
        branch_key: str,
        username: str,
        state: str,
        location: Optional[str],
        updated_at: int,
    ) -> None:
        data = {
            "branch_key": branch_key,
            "username": username,
            "state": state,
            "location": location,
            "updated_at": updated_at,
        }
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute_upsert_branch_local_user(cursor, data)

    def delete_branch_local_user(self, branch_key: str, username: str) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM branch_local_users WHERE branch_key=%s AND username=%s",
                (branch_key, username),
            )

    def fetch_activity(self, *, branch_keys: Optional[Iterable[str]] = None) -> List[dict]:
        sql = "SELECT ts, [user] AS [user], group_name, project, branch, action, result, message, branch_key FROM activity_log"
        params: List[str] = []
        keys = [key for key in (branch_keys or []) if key]
        if keys:
            placeholders = ",".join("%s" for _ in keys)
            sql += f" WHERE branch_key IN ({placeholders})"
            params.extend(keys)
        sql += " ORDER BY ts DESC, id DESC"
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def append_activity(self, entries: Iterable[dict]) -> None:
        payload = [_normalize_activity_payload(entry) for entry in entries]
        if not payload:
            return
        with self._connect() as conn:
            cursor = conn.cursor()
            for data in payload:
                self._insert_ignore_activity(cursor, data)

    def fetch_sprints(self, *, branch_keys: Optional[Sequence[str]] = None) -> List[dict]:
        sql = "SELECT s.*, g.group_name FROM sprints AS s LEFT JOIN sprint_groups AS g ON g.sprint_id = s.id"
        params: List[str] = []
        keys = [key for key in (branch_keys or []) if key]
        if keys:
            placeholders = ",".join("%s" for _ in keys)
            sql += f" WHERE s.branch_key IN ({placeholders})"
            params.extend(keys)
        sql += " ORDER BY s.created_at DESC, s.id DESC"
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def fetch_sprint(self, sprint_id: int) -> Optional[dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT s.*, g.group_name FROM sprints AS s LEFT JOIN sprint_groups AS g ON g.sprint_id = s.id WHERE s.id=%s",
                (int(sprint_id),),
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    def fetch_sprint_by_branch_key(self, branch_key: str) -> Optional[dict]:
        key = (branch_key or "").strip()
        if not key:
            return None
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT s.*, g.group_name FROM sprints AS s LEFT JOIN sprint_groups AS g ON g.sprint_id = s.id WHERE s.branch_key=%s OR s.qa_branch_key=%s",
                (key, key),
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    def upsert_sprint(self, payload: dict) -> int:
        data = _normalize_sprint(payload)
        group_name = data.pop("group_name", None)
        columns = [
            "id",
            "branch_key",
            "qa_branch_key",
            "name",
            "version",
            "lead_user",
            "qa_user",
            "company_id",
            "description",
            "status",
            "closed_at",
            "closed_by",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
        ]
        with self._connect() as conn:
            cursor = conn.cursor()
            sprint_id = self._execute_upsert_generic(cursor, "sprints", "id", data, columns)
            self._update_sprint_group(cursor, sprint_id, group_name)
            return sprint_id

    def _update_sprint_group(self, cursor, sprint_id: int, group_name: Optional[str]) -> None:
        if sprint_id is None:
            return
        if group_name:
            cursor.execute(
                """
                IF EXISTS (SELECT 1 FROM sprint_groups WHERE sprint_id=%s)
                BEGIN
                    UPDATE sprint_groups SET group_name=%s WHERE sprint_id=%s;
                END
                ELSE
                BEGIN
                    INSERT INTO sprint_groups (sprint_id, group_name) VALUES (%s, %s);
                END
                """,
                (
                    int(sprint_id),
                    group_name,
                    int(sprint_id),
                    int(sprint_id),
                    group_name,
                ),
            )
        else:
            cursor.execute(
                "DELETE FROM sprint_groups WHERE sprint_id=%s",
                (int(sprint_id),),
            )

    def delete_sprint(self, sprint_id: int) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            timestamp = int(time.time())
            username = self._system_username()
            cursor.execute(
                """
                UPDATE card_sprint_links
                   SET unassigned_at=%s,
                       unassigned_by=%s
                 WHERE sprint_id=%s
                   AND unassigned_at IS NULL
                """,
                (timestamp, username, int(sprint_id)),
            )
            cursor.execute(
                "UPDATE card_sprint_links SET sprint_id=NULL WHERE sprint_id=%s",
                (int(sprint_id),),
            )
            cursor.execute(
                "UPDATE cards SET sprint_id=NULL WHERE sprint_id=%s",
                (int(sprint_id),),
            )
            cursor.execute("DELETE FROM sprints WHERE id=%s", (int(sprint_id),))

    def fetch_cards(
        self,
        *,
        sprint_ids: Optional[Sequence[int]] = None,
        branches: Optional[Sequence[str]] = None,
        company_ids: Optional[Sequence[int]] = None,
        group_names: Optional[Sequence[str]] = None,
        statuses: Optional[Sequence[str]] = None,
        include_closed: bool = True,
        without_sprint: bool = False,
    ) -> List[dict]:
        sql = "SELECT * FROM cards"
        params: List[object] = []
        clauses: List[str] = []
        ids = [int(x) for x in (sprint_ids or []) if x is not None]
        if ids:
            placeholders = ",".join("%s" for _ in ids)
            clauses.append(f"sprint_id IN ({placeholders})")
            params.extend(ids)
        names = [b for b in (branches or []) if b]
        if names:
            placeholders = ",".join("%s" for _ in names)
            clauses.append(f"branch IN ({placeholders})")
            params.extend(names)
        companies = [int(cid) for cid in (company_ids or []) if cid not in (None, "")]
        if companies:
            placeholders = ",".join("%s" for _ in companies)
            clauses.append(f"company_id IN ({placeholders})")
            params.extend(companies)
        groups = [(g or "").strip() for g in (group_names or []) if g is not None]
        groups = [g for g in groups if g]
        if groups:
            placeholders = ",".join("%s" for _ in groups)
            clauses.append(f"group_name IN ({placeholders})")
            params.extend(groups)
        status_list = [(s or "").lower() for s in (statuses or []) if s]
        status_list = [s for s in status_list if s]
        if status_list:
            placeholders = ",".join("%s" for _ in status_list)
            clauses.append(f"LOWER(status) IN ({placeholders})")
            params.extend(status_list)
        if not include_closed:
            clauses.append("LOWER(status) <> 'terminated'")
        if without_sprint:
            clauses.append("sprint_id IS NULL")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC"
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def fetch_card(self, card_id: int) -> Optional[dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM cards WHERE id=%s", (int(card_id),))
            row = cursor.fetchone()
        return dict(row) if row else None

    def upsert_card(self, payload: dict) -> int:
        data = _normalize_card(payload)
        columns = [
            "id",
            "sprint_id",
            "branch_key",
            "title",
            "ticket_id",
            "branch",
            "group_name",
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
            "company_id",
            "incidence_type_id",
            "closed_at",
            "closed_by",
            "branch_created_by",
            "branch_created_at",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
        ]
        with self._connect() as conn:
            cursor = conn.cursor()
            card_id = int(self._execute_upsert_generic(cursor, "cards", "id", data, columns) or 0)
            if card_id <= 0:
                card_id = int(data.get("id") or 0)
            if card_id:
                self._sync_card_links(cursor, card_id, data)
            return card_id

    def delete_card(self, card_id: int) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM cards WHERE id=%s", (int(card_id),))

    def assign_cards_to_sprint(self, sprint_id: int, card_ids: Sequence[int]) -> None:
        ids = [int(cid) for cid in card_ids if cid not in (None, "")]
        if not ids:
            return
        placeholders = ",".join("%s" for _ in ids)
        params: List[object] = [int(sprint_id)]
        params.extend(ids)
        sql = (
            "UPDATE cards SET sprint_id=%s WHERE id IN ("
            + placeholders
            + ") AND (status IS NULL OR LOWER(status) <> 'terminated')"
        )
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))

    def fetch_users(self) -> List[dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    username,
                    display_name,
                    email,
                    active,
                    require_password_reset,
                    password_changed_at,
                    active_since,
                    CASE
                        WHEN password_hash IS NULL OR password_hash = '' THEN 0
                        ELSE 1
                    END AS has_password
                FROM users
                ORDER BY display_name
                """
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def fetch_user(self, username: str) -> Optional[dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    username,
                    display_name,
                    email,
                    active,
                    require_password_reset,
                    password_changed_at,
                    active_since,
                    password_hash,
                    password_salt,
                    password_algo
                FROM users
                WHERE username=%s
                """,
                (username,),
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    def upsert_user(self, payload: dict) -> None:
        data = _normalize_user(payload)
        columns = ["username", "display_name", "email", "active"]
        if "require_password_reset" in payload:
            columns.append("require_password_reset")
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute_upsert_generic(
                cursor,
                "users",
                "username",
                data,
                columns,
            )

    def update_user_profile(self, username: str, display_name: Optional[str], email: Optional[str]) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE users
                SET display_name=%s, email=%s
                WHERE username=%s
                """,
                (display_name, email, username),
            )

    def set_user_active(self, username: str, active: bool, *, timestamp: Optional[int] = None) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            if active:
                cursor.execute(
                    """
                    UPDATE users
                    SET active = 1,
                        active_since = CASE WHEN %s IS NOT NULL THEN %s ELSE active_since END
                    WHERE username=%s
                    """,
                    (timestamp, timestamp, username),
                )
            else:
                cursor.execute(
                    """
                    UPDATE users
                    SET active = 0
                    WHERE username=%s
                    """,
                    (username,),
                )

    def update_user_password(
        self,
        username: str,
        *,
        password_hash: Optional[str],
        password_salt: Optional[str],
        password_algo: Optional[str],
        password_changed_at: Optional[int],
        require_password_reset: bool,
    ) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE users
                SET password_hash=%s,
                    password_salt=%s,
                    password_algo=%s,
                    password_changed_at=%s,
                    require_password_reset=%s
                WHERE username=%s
                """,
                (
                    password_hash,
                    password_salt,
                    password_algo,
                    password_changed_at,
                    1 if require_password_reset else 0,
                    username,
                ),
            )

    def mark_password_reset(self, username: str, require_password_reset: bool) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET require_password_reset=%s WHERE username=%s",
                (1 if require_password_reset else 0, username),
            )

    def delete_user(self, username: str) -> None:
        # Soft delete: mark as inactive
        self.set_user_active(username, False)

    def fetch_roles(self) -> List[dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT [key], name, description FROM roles ORDER BY name")
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def upsert_role(self, payload: dict) -> None:
        data = _normalize_role(payload)
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute_upsert_generic(
                cursor,
                "roles",
                "key",
                data,
                ["key", "name", "description"],
            )

    def delete_role(self, role_key: str) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM roles WHERE [key]=%s", (role_key,))

    def fetch_user_roles(self, username: Optional[str] = None) -> List[dict]:
        sql = "SELECT username, role_key FROM user_roles"
        params: List[str] = []
        if username:
            sql += " WHERE username=%s"
            params.append(username)
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def set_user_roles(self, username: str, roles: Sequence[str]) -> None:
        normalized = [(username, role) for role in roles if role]
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM user_roles WHERE username=%s", (username,))
            for entry in normalized:
                try:
                    cursor.execute(
                        "INSERT INTO user_roles (username, role_key) VALUES (%s, %s)",
                        entry,
                    )
                except pymssql.IntegrityError:
                    pass

    def fetch_companies(self) -> List[dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM catalog_companies ORDER BY name")
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def fetch_company(self, company_id: int) -> Optional[dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM catalog_companies WHERE id=%s",
                (int(company_id),),
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    def upsert_company(self, payload: dict) -> int:
        data = _normalize_company(payload)
        columns = [
            "id",
            "name",
            "group_name",
            "next_sprint_number",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
        ]
        with self._connect() as conn:
            cursor = conn.cursor()
            return self._execute_upsert_generic(
                cursor,
                "catalog_companies",
                "id",
                data,
                columns,
            )

    def delete_company(self, company_id: int) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM catalog_companies WHERE id=%s",
                (int(company_id),),
            )

    def fetch_incidence_types(self) -> List[dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM catalog_incidence_types ORDER BY name")
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def fetch_incidence_type(self, type_id: int) -> Optional[dict]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM catalog_incidence_types WHERE id=%s",
                (int(type_id),),
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    def upsert_incidence_type(self, payload: dict) -> int:
        data = _normalize_incidence_type(payload)
        columns = [
            "id",
            "name",
            "icon",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
        ]
        with self._connect() as conn:
            cursor = conn.cursor()
            return int(
                self._execute_upsert_generic(
                    cursor,
                    "catalog_incidence_types",
                    "id",
                    data,
                    columns,
                )
            )

    def delete_incidence_type(self, type_id: int) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM catalog_incidence_types WHERE id=%s",
                (int(type_id),),
            )

    def prune_activity(self, valid_keys: Iterable[str]) -> None:
        keys = [key for key in valid_keys if key]
        with self._connect() as conn:
            cursor = conn.cursor()
            if not keys:
                cursor.execute("DELETE FROM activity_log")
                return
            placeholders = ",".join("%s" for _ in keys)
            sql = f"DELETE FROM activity_log WHERE branch_key NOT IN ({placeholders})"
            cursor.execute(sql, tuple(keys))


class BranchHistoryRepo:
    """Fachada que gestiona la conexión hacia SQL Server."""

    def __init__(
        self,
        path: Optional[Path] = None,
        *,
        backend: Optional[str] = None,
        url: Optional[str] = None,
        pool_size: int = 5,
    ) -> None:
        url = url or os.environ.get("BRANCH_HISTORY_DB_URL")
        if backend and backend.lower() != "sqlserver":
            raise ValueError("Solo se admite el backend 'sqlserver' para BranchHistoryDB.")
        if not url:
            raise ValueError(
                "Se requiere una URL de conexión (BRANCH_HISTORY_DB_URL) para usar SQL Server."
            )
        self._backend = _SqlServerBranchHistory(url, pool_size=pool_size)

    @property
    def backend_name(self) -> str:
        return "sqlserver"

    def __getattr__(self, item):  # pragma: no cover - delegado trivial
        return getattr(self._backend, item)


BranchHistoryDB = BranchHistoryRepo
