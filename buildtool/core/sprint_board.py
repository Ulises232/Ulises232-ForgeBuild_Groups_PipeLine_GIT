"""Persistence helpers for sprint/version cards linked to the branch history DB."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable, List, Optional

from .branch_history_db import BranchHistoryDB

# ---------------------------------------------------------------------------
# Constants and utilities

STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"
STATUS_REJECTED = "rejected"

_VALID_STATUSES = {STATUS_PENDING, STATUS_APPROVED, STATUS_REJECTED}


def compose_sprint_key(
    version_branch: str,
    group_name: Optional[str],
    project_name: Optional[str],
    sprint_name: str,
) -> str:
    """Generate a deterministic identifier for a sprint."""

    parts: List[str] = [version_branch.strip()]
    if group_name:
        parts.append(group_name.strip())
    if project_name:
        parts.append(project_name.strip())
    parts.append(sprint_name.strip())
    return "::".join(part for part in parts if part)


# ---------------------------------------------------------------------------
# Data models


@dataclass
class SprintCard:
    key: str
    title: str
    branch_name: str
    assignee: Optional[str]
    unit_status: str
    qa_status: str
    is_qa_branch: bool
    unit_checked_by: Optional[str]
    qa_checked_by: Optional[str]

    def ready_for_merge(self) -> bool:
        if self.unit_status != STATUS_APPROVED:
            return False
        if self.is_qa_branch:
            return True
        return self.qa_status == STATUS_APPROVED


@dataclass
class SprintRecord:
    key: str
    name: str
    version_branch: str
    group_name: Optional[str]
    project_name: Optional[str]
    cards: List[SprintCard]


# ---------------------------------------------------------------------------
# Store


class SprintBoardStore:
    """High-level CRUD helpers over the sprint/QA workflow tables."""

    def __init__(self, db: BranchHistoryDB):
        self._db = db

    # -- roles & users --------------------------------------------------
    def upsert_role(self, name: str, *, description: str = "") -> None:
        name = name.strip()
        if not name:
            raise ValueError("role name cannot be empty")
        with self._db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sprint_roles (name, description)
                VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE SET description = excluded.description
                """,
                (name, description.strip()),
            )

    def upsert_user(
        self,
        username: str,
        *,
        display_name: Optional[str] = None,
        role_name: Optional[str] = None,
    ) -> None:
        username = username.strip()
        if not username:
            raise ValueError("username cannot be empty")
        now = int(time.time())
        with self._db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sprint_users (username, display_name, role_name, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    display_name = excluded.display_name,
                    role_name = excluded.role_name,
                    updated_at = excluded.updated_at
                """,
                (username, display_name, role_name, now, now),
            )

    def list_users(self) -> List[dict]:
        with self._db.connect() as conn:
            rows = conn.execute(
                "SELECT username, display_name, role_name FROM sprint_users ORDER BY username"
            ).fetchall()
        return [dict(row) for row in rows]

    # -- sprints --------------------------------------------------------
    def create_sprint(
        self,
        *,
        name: str,
        version_branch: str,
        group_name: Optional[str] = None,
        project_name: Optional[str] = None,
        created_by: Optional[str] = None,
        key: Optional[str] = None,
    ) -> SprintRecord:
        sprint_name = name.strip()
        version = version_branch.strip()
        if not sprint_name or not version:
            raise ValueError("sprint name and version_branch are required")
        sprint_key = key.strip() if key else compose_sprint_key(version, group_name, project_name, sprint_name)
        now = int(time.time())
        with self._db.connect() as conn:
            created_by_id = self._resolve_user_id(conn, created_by) if created_by else None
            conn.execute(
                """
                INSERT INTO sprints (
                    key, name, version_branch, group_name, project_name,
                    created_at, created_by, updated_at, updated_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    name = excluded.name,
                    version_branch = excluded.version_branch,
                    group_name = excluded.group_name,
                    project_name = excluded.project_name,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
                """,
                (
                    sprint_key,
                    sprint_name,
                    version,
                    group_name,
                    project_name,
                    now,
                    created_by_id,
                    now,
                    created_by_id,
                ),
            )
        return self.get_sprint(sprint_key)

    def list_sprints(
        self,
        *,
        group_name: Optional[str] = None,
        project_name: Optional[str] = None,
    ) -> List[SprintRecord]:
        filters: List[str] = []
        params: List[str] = []
        if group_name:
            filters.append("group_name = ?")
            params.append(group_name)
        if project_name:
            filters.append("project_name = ?")
            params.append(project_name)

        sql = "SELECT * FROM sprints"
        if filters:
            sql += " WHERE " + " AND ".join(filters)
        sql += " ORDER BY created_at DESC, id DESC"

        with self._db.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._row_to_sprint(conn, row) for row in rows]

    def get_sprint(self, key: str) -> SprintRecord:
        with self._db.connect() as conn:
            row = conn.execute("SELECT * FROM sprints WHERE key = ?", (key,)).fetchone()
            if not row:
                raise KeyError(f"sprint '{key}' not found")
            return self._row_to_sprint(conn, row)

    # -- cards ----------------------------------------------------------
    def add_card(
        self,
        sprint_key: str,
        *,
        title: str,
        branch_name: str,
        assignee: Optional[str] = None,
        description: str = "",
        is_qa_branch: bool = False,
        created_by: Optional[str] = None,
        key: Optional[str] = None,
    ) -> SprintCard:
        card_title = title.strip()
        branch = branch_name.strip()
        if not card_title or not branch:
            raise ValueError("card title and branch are required")
        card_key = key.strip() if key else f"{sprint_key}::{branch}"
        now = int(time.time())

        with self._db.connect() as conn:
            sprint = conn.execute("SELECT id FROM sprints WHERE key = ?", (sprint_key,)).fetchone()
            if not sprint:
                raise KeyError(f"sprint '{sprint_key}' not found")
            assignee_id = self._resolve_user_id(conn, assignee) if assignee else None
            creator_id = self._resolve_user_id(conn, created_by) if created_by else None
            conn.execute(
                """
                INSERT INTO sprint_cards (
                    key, sprint_id, title, description, branch_name,
                    assignee_id, created_at, created_by, updated_at,
                    updated_by, is_qa_branch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    branch_name = excluded.branch_name,
                    assignee_id = excluded.assignee_id,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by,
                    is_qa_branch = excluded.is_qa_branch
                """,
                (
                    card_key,
                    sprint["id"],
                    card_title,
                    description,
                    branch,
                    assignee_id,
                    now,
                    creator_id,
                    now,
                    creator_id,
                    1 if is_qa_branch else 0,
                ),
            )

        return self.get_card(card_key)

    def get_card(self, key: str) -> SprintCard:
        with self._db.connect() as conn:
            row = conn.execute(
                """
                SELECT c.key, c.title, c.branch_name, assignee.username AS assignee,
                       c.unit_status, c.qa_status, c.is_qa_branch,
                       unit_user.username AS unit_checked_by,
                       qa_user.username AS qa_checked_by
                FROM sprint_cards AS c
                LEFT JOIN sprint_users AS assignee ON assignee.id = c.assignee_id
                LEFT JOIN sprint_users AS unit_user ON unit_user.id = c.unit_checked_by
                LEFT JOIN sprint_users AS qa_user ON qa_user.id = c.qa_checked_by
                WHERE c.key = ?
                """,
                (key,),
            ).fetchone()
            if not row:
                raise KeyError(f"card '{key}' not found")
            return self._row_to_card(row)

    def set_unit_status(self, key: str, status: str | bool, *, by: Optional[str] = None) -> None:
        normalized = self._normalize_status(status)
        now = int(time.time())
        with self._db.connect() as conn:
            user_id = self._resolve_user_id(conn, by) if by else None
            checked_at = now if normalized != STATUS_PENDING else None
            cur = conn.execute(
                """
                UPDATE sprint_cards
                SET unit_status = ?, unit_checked_at = ?, unit_checked_by = ?,
                    updated_at = ?, updated_by = ?
                WHERE key = ?
                """,
                (normalized, checked_at, user_id, now, user_id, key),
            )
            if cur.rowcount == 0:
                raise KeyError(f"card '{key}' not found")

    def set_qa_status(self, key: str, status: str | bool, *, by: Optional[str] = None) -> None:
        normalized = self._normalize_status(status)
        now = int(time.time())
        with self._db.connect() as conn:
            user_id = self._resolve_user_id(conn, by) if by else None
            checked_at = now if normalized != STATUS_PENDING else None
            cur = conn.execute(
                """
                UPDATE sprint_cards
                SET qa_status = ?, qa_checked_at = ?, qa_checked_by = ?,
                    updated_at = ?, updated_by = ?
                WHERE key = ?
                """,
                (normalized, checked_at, user_id, now, user_id, key),
            )
            if cur.rowcount == 0:
                raise KeyError(f"card '{key}' not found")

    def cards_ready_for_merge(self, keys: Iterable[str]) -> dict[str, bool]:
        placeholders = ",".join("?" for _ in keys)
        if not placeholders:
            return {}
        with self._db.connect() as conn:
            rows = conn.execute(
                f"SELECT key, unit_status, qa_status, is_qa_branch FROM sprint_cards WHERE key IN ({placeholders})",
                list(keys),
            ).fetchall()
        return {
            row["key"]: (
                row["unit_status"] == STATUS_APPROVED
                and (row["is_qa_branch"] or row["qa_status"] == STATUS_APPROVED)
            )
            for row in rows
        }

    # ------------------------------------------------------------------
    # Internal helpers
    def _row_to_sprint(self, conn, row) -> SprintRecord:
        cards = self._fetch_cards(conn, row["id"])
        return SprintRecord(
            key=row["key"],
            name=row["name"],
            version_branch=row["version_branch"],
            group_name=row["group_name"],
            project_name=row["project_name"],
            cards=cards,
        )

    def _fetch_cards(self, conn, sprint_id: int) -> List[SprintCard]:
        rows = conn.execute(
            """
            SELECT c.key, c.title, c.branch_name, assignee.username AS assignee,
                   c.unit_status, c.qa_status, c.is_qa_branch,
                   unit_user.username AS unit_checked_by,
                   qa_user.username AS qa_checked_by
            FROM sprint_cards AS c
            LEFT JOIN sprint_users AS assignee ON assignee.id = c.assignee_id
            LEFT JOIN sprint_users AS unit_user ON unit_user.id = c.unit_checked_by
            LEFT JOIN sprint_users AS qa_user ON qa_user.id = c.qa_checked_by
            WHERE c.sprint_id = ?
            ORDER BY c.id ASC
            """,
            (sprint_id,),
        ).fetchall()
        return [self._row_to_card(row) for row in rows]

    @staticmethod
    def _row_to_card(row) -> SprintCard:
        return SprintCard(
            key=row["key"],
            title=row["title"],
            branch_name=row["branch_name"],
            assignee=row["assignee"],
            unit_status=row["unit_status"],
            qa_status=row["qa_status"],
            is_qa_branch=bool(row["is_qa_branch"]),
            unit_checked_by=row["unit_checked_by"],
            qa_checked_by=row["qa_checked_by"],
        )

    @staticmethod
    def _normalize_status(status: str | bool) -> str:
        if isinstance(status, bool):
            return STATUS_APPROVED if status else STATUS_REJECTED
        lowered = str(status).strip().lower()
        if lowered not in _VALID_STATUSES:
            raise ValueError(
                "status must be one of {'pending', 'approved', 'rejected'} or a boolean"
            )
        return lowered

    @staticmethod
    def _resolve_user_id(conn, username: Optional[str]) -> int:
        if username is None:
            raise ValueError("username required for this operation")
        row = conn.execute("SELECT id FROM sprint_users WHERE username = ?", (username,)).fetchone()
        if not row:
            raise KeyError(f"user '{username}' not found")
        return int(row["id"])

