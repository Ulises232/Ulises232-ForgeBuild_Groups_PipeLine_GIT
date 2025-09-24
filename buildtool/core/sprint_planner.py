"""Sprint, tarjetas y roles vinculados al historial de ramas."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
import re
import time
from typing import Callable, Iterable, List, Optional, Sequence

from .branch_history_db import BranchHistoryDB
from .branch_store import NasUnavailableError, history_db_for


class AuthorizationError(RuntimeError):
    """Raised when a user lacks permissions to perform an action."""


class TicketValidationError(ValueError):
    """Raised when ticket information is inconsistent with sprint rules."""


class MergeBlockedError(RuntimeError):
    """Raised when a ticket cannot merge into the sprint branch yet."""


class SprintStatus(str, Enum):
    PLANNED = "planned"
    ACTIVE = "active"
    COMPLETED = "completed"
    ARCHIVED = "archived"

    @classmethod
    def from_value(cls, value: Optional[str]) -> "SprintStatus":
        needle = (value or "").strip().lower()
        for item in cls:
            if item.value == needle:
                return item
        return cls.PLANNED


class ApprovalState(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"

    @classmethod
    def from_value(cls, value: Optional[str]) -> "ApprovalState":
        needle = (value or "").strip().lower()
        for item in cls:
            if item.value == needle:
                return item
        return cls.PENDING


@dataclass(frozen=True)
class RoleDefinition:
    key: str
    display_name: str
    permissions: Sequence[str]
    description: str = ""
    is_system: bool = False
    created_at: int = 0
    created_by: str = ""

    def normalized_permissions(self) -> Sequence[str]:
        return tuple(sorted({perm for perm in self.permissions if perm}))


@dataclass(frozen=True)
class UserAccount:
    username: str
    role_key: str
    display_name: str = ""
    email: str = ""
    is_active: bool = True
    created_at: int = 0
    created_by: str = ""


@dataclass(frozen=True)
class SprintRecord:
    key: str
    name: str
    version: str
    base_branch: str
    group_name: Optional[str] = None
    project: Optional[str] = None
    base_branch_key: Optional[str] = None
    status: SprintStatus = SprintStatus.PLANNED
    start_date: int = 0
    end_date: int = 0
    description: str = ""
    created_at: int = 0
    created_by: str = ""

    def branch_prefix(self) -> str:
        sanitized = re.sub(r"\s+", "-", self.name.strip()) if self.name else ""
        return f"{self.base_branch}/{sanitized}" if sanitized else self.base_branch


@dataclass(frozen=True)
class TicketRecord:
    key: str
    sprint_key: str
    title: str
    branch_name: str
    description: str = ""
    assignee: Optional[str] = None
    qa_owner: Optional[str] = None
    requires_qa: bool = True
    unit_status: ApprovalState = ApprovalState.PENDING
    unit_updated_at: int = 0
    unit_updated_by: str = ""
    qa_status: ApprovalState = ApprovalState.PENDING
    qa_updated_at: int = 0
    qa_updated_by: str = ""
    created_at: int = 0
    created_by: str = ""

    def can_merge(self) -> bool:
        if self.requires_qa:
            return (
                self.unit_status is ApprovalState.APPROVED
                and self.qa_status is ApprovalState.APPROVED
            )
        return self.unit_status is ApprovalState.APPROVED

    def with_unit_status(self, state: ApprovalState, actor: str, ts: int) -> "TicketRecord":
        return replace(
            self,
            unit_status=state,
            unit_updated_at=ts,
            unit_updated_by=actor or "",
        )

    def with_qa_status(self, state: ApprovalState, actor: str, ts: int) -> "TicketRecord":
        return replace(
            self,
            qa_status=state,
            qa_updated_at=ts,
            qa_updated_by=actor or "",
        )


DEFAULT_ROLES: Sequence[RoleDefinition] = (
    RoleDefinition(
        key="leader",
        display_name="Líder de proyecto",
        permissions=(
            "role.view",
            "user.manage",
            "sprint.manage",
            "ticket.create",
            "ticket.assign",
            "ticket.approve.unit",
            "ticket.approve.qa",
            "ticket.merge.override",
        ),
        is_system=True,
        created_by="system",
    ),
    RoleDefinition(
        key="developer",
        display_name="Desarrollador",
        permissions=(
            "sprint.view",
            "ticket.create",
            "ticket.approve.unit",
        ),
        is_system=True,
        created_by="system",
    ),
    RoleDefinition(
        key="qa",
        display_name="QA",
        permissions=(
            "sprint.view",
            "ticket.approve.qa",
        ),
        is_system=True,
        created_by="system",
    ),
    RoleDefinition(
        key="admin",
        display_name="Administrador",
        permissions=(
            "role.manage",
            "user.manage",
            "sprint.manage",
            "ticket.manage",
            "ticket.approve.unit",
            "ticket.approve.qa",
            "ticket.merge.override",
        ),
        is_system=True,
        created_by="system",
    ),
)


class SprintPlanner:
    """Coordinador de sprints, tarjetas y aprobaciones vinculadas a ramas."""

    def __init__(
        self,
        storage: str = "local",
        *,
        db: Optional[BranchHistoryDB] = None,
        clock: Optional[Callable[[], int]] = None,
    ) -> None:
        self.storage = storage
        self.db = db or history_db_for(storage)  # type: ignore[arg-type]
        self._clock = clock or (lambda: int(time.time()))
        self._ensure_default_roles()

    # ------------------------------------------------------------------
    # roles y usuarios
    def list_roles(self) -> List[RoleDefinition]:
        rows = self.db.fetch_roles()
        return [_role_from_row(row) for row in rows]

    def save_role(self, role: RoleDefinition, *, allow_system: bool = False) -> None:
        if role.is_system and not allow_system:
            raise AuthorizationError("No se puede modificar un rol del sistema")
        payload = _role_to_payload(role)
        self.db.upsert_role(payload)

    def delete_role(self, key: str) -> None:
        self.db.delete_role(key)

    def list_users(self, *, active_only: bool = False) -> List[UserAccount]:
        rows = self.db.fetch_users(active_only=active_only)
        return [_user_from_row(row) for row in rows]

    def save_user(self, user: UserAccount) -> None:
        payload = _user_to_payload(user, now=self._clock())
        self.db.upsert_user(payload)

    def deactivate_user(self, username: str) -> None:
        record = self.db.get_user(username)
        if not record:
            return
        record["is_active"] = 0
        self.db.upsert_user(record)

    def activate_user(self, username: str) -> None:
        record = self.db.get_user(username)
        if not record:
            return
        record["is_active"] = 1
        self.db.upsert_user(record)

    # ------------------------------------------------------------------
    # sprints
    def list_sprints(
        self, *, group: Optional[str] = None, project: Optional[str] = None
    ) -> List[SprintRecord]:
        rows = self.db.fetch_sprints(group=group, project=project)
        return [_sprint_from_row(row) for row in rows]

    def get_sprint(self, key: str) -> Optional[SprintRecord]:
        row = self.db.get_sprint(key)
        return _sprint_from_row(row) if row else None

    def save_sprint(self, sprint: SprintRecord, *, actor: Optional[str] = None) -> None:
        if actor:
            self._require_permission(actor, "sprint.manage")
        payload = _sprint_to_payload(sprint, now=self._clock())
        self.db.upsert_sprint(payload)

    def delete_sprint(self, key: str, *, actor: Optional[str] = None) -> None:
        if actor:
            self._require_permission(actor, "sprint.manage")
        self.db.delete_sprint(key)

    # ------------------------------------------------------------------
    # tickets
    def list_tickets(self, *, sprint_keys: Optional[Iterable[str]] = None) -> List[TicketRecord]:
        rows = self.db.fetch_tickets(sprint_keys=sprint_keys)
        return [_ticket_from_row(row) for row in rows]

    def get_ticket(self, key: str) -> Optional[TicketRecord]:
        row = self.db.get_ticket(key)
        return _ticket_from_row(row) if row else None

    def save_ticket(
        self,
        ticket: TicketRecord,
        *,
        actor: Optional[str] = None,
        validate_branch: bool = True,
    ) -> None:
        if actor:
            self._require_permission(actor, "ticket.create")
        sprint = self.get_sprint(ticket.sprint_key)
        if not sprint:
            raise TicketValidationError(
                f"El sprint '{ticket.sprint_key}' no existe en la base de historial"
            )
        if validate_branch:
            self._validate_branch_for_ticket(sprint, ticket)
        payload = _ticket_to_payload(ticket, now=self._clock())
        self.db.upsert_ticket(payload)

    def delete_ticket(self, key: str, *, actor: Optional[str] = None) -> None:
        if actor:
            self._require_permission(actor, "ticket.manage")
        self.db.delete_ticket(key)

    def mark_unit_status(
        self, key: str, state: ApprovalState, *, actor: str
    ) -> TicketRecord:
        self._require_permission(actor, "ticket.approve.unit")
        ticket = self.get_ticket(key)
        if not ticket:
            raise TicketValidationError(f"La tarjeta '{key}' no existe")
        updated = ticket.with_unit_status(state, actor, self._clock())
        payload = _ticket_to_payload(updated)
        self.db.upsert_ticket(payload)
        return updated

    def mark_qa_status(
        self, key: str, state: ApprovalState, *, actor: str
    ) -> TicketRecord:
        ticket = self.get_ticket(key)
        if not ticket:
            raise TicketValidationError(f"La tarjeta '{key}' no existe")
        if ticket.requires_qa:
            self._require_permission(actor, "ticket.approve.qa")
        updated = ticket.with_qa_status(state, actor, self._clock())
        payload = _ticket_to_payload(updated)
        self.db.upsert_ticket(payload)
        return updated

    def ensure_merge_allowed(
        self, key: str, *, actor: Optional[str] = None
    ) -> TicketRecord:
        ticket = self.get_ticket(key)
        if not ticket:
            raise TicketValidationError(f"La tarjeta '{key}' no existe")
        if ticket.can_merge():
            return ticket
        if actor:
            try:
                self._require_permission(actor, "ticket.merge.override")
                return ticket
            except AuthorizationError:
                pass
        raise MergeBlockedError(
            "La tarjeta no cuenta con los checks necesarios para mergear la rama"
        )

    # ------------------------------------------------------------------
    # helpers
    def _ensure_default_roles(self) -> None:
        try:
            existing = {row["key"] for row in self.db.fetch_roles()}
        except NasUnavailableError:
            # Si la NAS no está disponible al iniciar en modo NAS, se deja que
            # el flujo de UI capture la excepción más adelante.
            existing = set()
        now = self._clock()
        for role in DEFAULT_ROLES:
            if role.key in existing:
                continue
            payload = _role_to_payload(role, now=now)
            payload["is_system"] = 1
            self.db.upsert_role(payload)

    def _require_permission(self, username: str, permission: str) -> None:
        record = self.db.get_user(username)
        if not record or not record.get("is_active", True):
            raise AuthorizationError(f"El usuario '{username}' no está activo en el sistema")
        role_key = record.get("role_key")
        role = self.db.get_role(role_key) if role_key else None
        if not role:
            raise AuthorizationError(
                f"El rol '{role_key}' no existe o fue deshabilitado"
            )
        permissions = {perm for perm in role.get("permissions", [])}
        if permission not in permissions:
            raise AuthorizationError(
                f"El usuario '{username}' no cuenta con el permiso '{permission}'"
            )

    def _validate_branch_for_ticket(
        self, sprint: SprintRecord, ticket: TicketRecord
    ) -> None:
        branch = ticket.branch_name.strip()
        if not branch:
            raise TicketValidationError("La tarjeta debe definir una rama de trabajo")
        if branch == sprint.base_branch:
            raise TicketValidationError(
                "La tarjeta no puede reutilizar la rama base del sprint"
            )
        if not branch.startswith(sprint.base_branch):
            raise TicketValidationError(
                "La rama de la tarjeta debe crearse a partir de la versión del sprint"
            )


# ----------------------------------------------------------------------
# row conversion helpers

def _role_from_row(row: dict) -> RoleDefinition:
    return RoleDefinition(
        key=row.get("key", ""),
        display_name=row.get("display_name", ""),
        permissions=tuple(row.get("permissions", []) or []),
        description=row.get("description", ""),
        is_system=bool(row.get("is_system", False)),
        created_at=int(row.get("created_at") or 0),
        created_by=row.get("created_by", ""),
    )


def _role_to_payload(role: RoleDefinition, *, now: Optional[int] = None) -> dict:
    ts = now if now is not None else int(time.time())
    return {
        "key": role.key,
        "display_name": role.display_name,
        "permissions": list(role.normalized_permissions()),
        "description": role.description,
        "is_system": 1 if role.is_system else 0,
        "created_at": role.created_at or ts,
        "created_by": role.created_by or "system",
    }


def _user_from_row(row: dict) -> UserAccount:
    return UserAccount(
        username=row.get("username", ""),
        role_key=row.get("role_key", ""),
        display_name=row.get("display_name", ""),
        email=row.get("email", ""),
        is_active=bool(row.get("is_active", True)),
        created_at=int(row.get("created_at") or 0),
        created_by=row.get("created_by", ""),
    )


def _user_to_payload(user: UserAccount, *, now: int) -> dict:
    return {
        "username": user.username,
        "display_name": user.display_name,
        "email": user.email,
        "role_key": user.role_key,
        "is_active": 1 if user.is_active else 0,
        "created_at": user.created_at or now,
        "created_by": user.created_by or "system",
    }


def _sprint_from_row(row: dict) -> SprintRecord:
    return SprintRecord(
        key=row.get("key", ""),
        name=row.get("name", ""),
        version=row.get("version", ""),
        base_branch=row.get("base_branch", ""),
        group_name=row.get("group_name"),
        project=row.get("project"),
        base_branch_key=row.get("base_branch_key"),
        status=SprintStatus.from_value(row.get("status")),
        start_date=int(row.get("start_date") or 0),
        end_date=int(row.get("end_date") or 0),
        description=row.get("description", ""),
        created_at=int(row.get("created_at") or 0),
        created_by=row.get("created_by", ""),
    )


def _sprint_to_payload(sprint: SprintRecord, *, now: int) -> dict:
    return {
        "key": sprint.key,
        "name": sprint.name,
        "version": sprint.version,
        "group_name": sprint.group_name,
        "project": sprint.project,
        "base_branch": sprint.base_branch,
        "base_branch_key": sprint.base_branch_key,
        "status": sprint.status.value,
        "start_date": sprint.start_date,
        "end_date": sprint.end_date,
        "description": sprint.description,
        "created_at": sprint.created_at or now,
        "created_by": sprint.created_by or "system",
    }


def _ticket_from_row(row: dict) -> TicketRecord:
    return TicketRecord(
        key=row.get("key", ""),
        sprint_key=row.get("sprint_key", ""),
        title=row.get("title", ""),
        description=row.get("description", ""),
        branch_name=row.get("branch_name", ""),
        assignee=row.get("assignee") or None,
        qa_owner=row.get("qa_owner") or None,
        requires_qa=bool(row.get("requires_qa", True)),
        unit_status=ApprovalState.from_value(row.get("unit_status")),
        unit_updated_at=int(row.get("unit_updated_at") or 0),
        unit_updated_by=row.get("unit_updated_by", ""),
        qa_status=ApprovalState.from_value(row.get("qa_status")),
        qa_updated_at=int(row.get("qa_updated_at") or 0),
        qa_updated_by=row.get("qa_updated_by", ""),
        created_at=int(row.get("created_at") or 0),
        created_by=row.get("created_by", ""),
    )


def _ticket_to_payload(ticket: TicketRecord, *, now: Optional[int] = None) -> dict:
    ts = now if now is not None else int(time.time())
    return {
        "key": ticket.key,
        "sprint_key": ticket.sprint_key,
        "title": ticket.title,
        "description": ticket.description,
        "branch_name": ticket.branch_name,
        "assignee": ticket.assignee,
        "qa_owner": ticket.qa_owner,
        "requires_qa": 1 if ticket.requires_qa else 0,
        "unit_status": ticket.unit_status.value,
        "unit_updated_at": ticket.unit_updated_at,
        "unit_updated_by": ticket.unit_updated_by,
        "qa_status": ticket.qa_status.value,
        "qa_updated_at": ticket.qa_updated_at,
        "qa_updated_by": ticket.qa_updated_by,
        "created_at": ticket.created_at or ts,
        "created_by": ticket.created_by or "system",
    }

