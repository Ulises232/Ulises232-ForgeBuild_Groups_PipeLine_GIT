from __future__ import annotations
from dataclasses import dataclass, asdict, fields
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import base64
import getpass
import hashlib
import hmac
import os
import time

from .branch_history_db import BranchHistoryDB, Sprint, Card, User, Role, Company
from .session import current_username


# ---------------- paths helpers -----------------


def _state_dir() -> Path:
    base = os.environ.get("APPDATA")
    if base:
        d = Path(base) / "forgebuild"
    else:
        d = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")) / "forgebuild"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _current_username() -> str:
    fallback = os.environ.get("USERNAME") or os.environ.get("USER") or getpass.getuser()
    return current_username(fallback)



# ---------------- data structures -----------------

@dataclass
class BranchRecord:
    branch: str
    group: Optional[str] = None
    project: Optional[str] = None
    created_at: int = 0
    created_by: str = ""
    exists_origin: bool = False
    merge_status: str = "none"
    diverged: Optional[bool] = None
    stale_days: Optional[int] = None
    last_action: str = "create"
    last_updated_at: int = 0
    last_updated_by: str = ""
    local_state: str = "absent"
    local_location: Optional[str] = None
    local_updated_at: int = 0

    def key(self) -> str:
        return f"{self.group or ''}/{self.project or ''}/{self.branch}"

    def has_local_copy(self) -> bool:
        return (self.local_state or "").lower() in {"present", "available", "synced"}

    def set_local_state(
        self,
        state: str,
        *,
        location: Optional[str] = None,
        updated_at: Optional[int] = None,
    ) -> None:
        normalized = (state or "").strip().lower() or "absent"
        self.local_state = normalized
        if location is not None:
            trimmed = location.strip()
            self.local_location = trimmed or None
        if updated_at:
            try:
                self.local_updated_at = int(updated_at)
            except (TypeError, ValueError):
                self.local_updated_at = int(time.time())
        else:
            self.local_updated_at = int(time.time())

    def mark_local(self, present: bool, *, location: Optional[str] = None) -> None:
        self.set_local_state("present" if present else "absent", location=location)


Index = Dict[str, BranchRecord]


_BRANCH_RECORD_FIELDS = {f.name for f in fields(BranchRecord)}
_LEGACY_FIELD_ALIASES = {
    "last_update": "last_updated_at",
    "last_update_by": "last_updated_by",
}
_BOOL_FIELDS = {"exists_origin"}
_INT_FIELDS = {"created_at", "last_updated_at", "local_updated_at"}

_PBKDF2_ALGO = "pbkdf2_sha256"
_PBKDF2_DEFAULT_ITERATIONS = 480_000


def _b64encode(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _b64decode(data: str) -> bytes:
    return base64.b64decode(data.encode("ascii"))


def _generate_salt(length: int = 32) -> str:
    return _b64encode(os.urandom(length))


def _parse_iterations(algo: Optional[str]) -> int:
    if not algo:
        return _PBKDF2_DEFAULT_ITERATIONS
    try:
        name, _, rest = algo.partition(":")
        if name.strip().lower() != _PBKDF2_ALGO:
            return _PBKDF2_DEFAULT_ITERATIONS
        if rest:
            return max(1, int(rest))
    except (ValueError, TypeError):
        pass
    return _PBKDF2_DEFAULT_ITERATIONS


def _derive_password(password: str, salt: str, *, iterations: Optional[int] = None) -> str:
    if not iterations or iterations <= 0:
        iterations = _PBKDF2_DEFAULT_ITERATIONS
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        _b64decode(salt),
        iterations,
    )
    return _b64encode(digest)


def _build_algo(iterations: int) -> str:
    return f"{_PBKDF2_ALGO}:{max(1, int(iterations or _PBKDF2_DEFAULT_ITERATIONS))}"


def _password_entry(password: str, *, iterations: int = _PBKDF2_DEFAULT_ITERATIONS) -> Tuple[str, str, str]:
    salt = _generate_salt()
    hashed = _derive_password(password, salt, iterations=iterations)
    return hashed, salt, _build_algo(iterations)


def _verify_password(password: str, stored_hash: Optional[str], salt: Optional[str], algo: Optional[str]) -> bool:
    if not password or not stored_hash or not salt:
        return False
    iterations = _parse_iterations(algo)
    derived = _derive_password(password, salt, iterations=iterations)
    return hmac.compare_digest(stored_hash, derived)


@dataclass
class AuthenticationResult:
    user: Optional[User]
    status: str
    message: Optional[str] = None
    require_password_reset: bool = False
    needs_password: bool = False

    @property
    def success(self) -> bool:
        return self.status == "ok"


def _normalize_record_payload(raw: Any) -> Optional[BranchRecord]:
    if not isinstance(raw, dict):
        return None

    data: Dict[str, Any] = {}

    for key, value in raw.items():
        if key in _BRANCH_RECORD_FIELDS:
            data[key] = value

    for legacy, new in _LEGACY_FIELD_ALIASES.items():
        if new not in data and legacy in raw:
            data[new] = raw[legacy]

    if not data.get("branch"):
        return None

    for key in _INT_FIELDS:
        if key in data:
            try:
                data[key] = int(data[key])
            except (TypeError, ValueError):
                data[key] = 0

    for key in _BOOL_FIELDS:
        if key in data:
            value = data[key]
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"", "false", "0", "no"}:
                    data[key] = False
                elif lowered in {"true", "1", "yes"}:
                    data[key] = True
                else:
                    data[key] = bool(value)
            else:
                data[key] = bool(value)

    try:
        rec = BranchRecord(**data)
    except Exception:
        return None
    return rec


_DB_CACHE: Dict[str, BranchHistoryDB] = {}
_SERVER_CACHE_KEY = "__sqlserver__"


def _get_db(base: Path) -> BranchHistoryDB:
    db = _DB_CACHE.get(_SERVER_CACHE_KEY)
    if not db:
        db = BranchHistoryDB(url=os.environ.get("BRANCH_HISTORY_DB_URL"))
        _DB_CACHE[_SERVER_CACHE_KEY] = db
    return db


def _branch_payload(rec: BranchRecord) -> dict:
    data = asdict(rec)
    data.pop("local_state", None)
    data.pop("local_location", None)
    data.pop("local_updated_at", None)
    data["group_name"] = data.pop("group", None)
    data["key"] = rec.key()
    data["exists_local"] = 1 if rec.has_local_copy() else 0
    return data


def _records_to_payloads(records: Iterable[BranchRecord]) -> List[dict]:
    return [_branch_payload(rec) for rec in records]


def _sync_local_binding(db: BranchHistoryDB, rec: BranchRecord, username: str) -> None:
    state = (rec.local_state or "").strip().lower() or "absent"
    location = rec.local_location or None
    ts = int(rec.local_updated_at or 0)
    if not ts:
        ts = int(time.time())
        rec.local_updated_at = ts
    db.upsert_branch_local_user(rec.key(), username, state, location, ts)


def _row_to_record(row: dict) -> BranchRecord:
    raw_state = (row.get("local_state") or "").strip().lower()
    if not raw_state:
        raw_state = "present" if row.get("exists_local") else "absent"
    location = row.get("local_location") or None
    data = {
        "branch": row.get("branch"),
        "group": row.get("group_name"),
        "project": row.get("project"),
        "created_at": int(row.get("created_at") or 0),
        "created_by": row.get("created_by") or "",
        "exists_origin": bool(row.get("exists_origin")),
        "merge_status": row.get("merge_status") or "",
        "diverged": None if row.get("diverged") is None else bool(row.get("diverged")),
        "stale_days": row.get("stale_days"),
        "last_action": row.get("last_action") or "",
        "last_updated_at": int(row.get("last_updated_at") or 0),
        "last_updated_by": row.get("last_updated_by") or "",
        "local_state": raw_state,
        "local_location": location,
        "local_updated_at": int(row.get("local_updated_at") or 0),
    }
    if data["stale_days"] not in (None, ""):
        try:
            data["stale_days"] = int(data["stale_days"])
        except (TypeError, ValueError):
            data["stale_days"] = None
    else:
        data["stale_days"] = None
    return BranchRecord(**data)


def _row_to_sprint(row: dict) -> Sprint:
    qa_branch = row.get("qa_branch_key")
    if isinstance(qa_branch, str):
        qa_branch = qa_branch or None
    return Sprint(
        id=int(row["id"]) if row.get("id") is not None else None,
        branch_key=row.get("branch_key") or "",
        group_name=row.get("group_name") or None,
        name=row.get("name") or "",
        version=row.get("version") or "",
        qa_branch_key=qa_branch,
        lead_user=row.get("lead_user") or None,
        qa_user=row.get("qa_user") or None,
        company_id=int(row.get("company_id") or 0) or None,
        company_sequence=int(row.get("company_sequence") or 0) or None,
        description=row.get("description") or "",
        status=(row.get("status") or "open").lower(),
        closed_at=int(row.get("closed_at") or 0) or None,
        closed_by=row.get("closed_by") or None,
        created_at=int(row.get("created_at") or 0),
        created_by=row.get("created_by") or "",
        updated_at=int(row.get("updated_at") or 0),
        updated_by=row.get("updated_by") or "",
    )


def _row_to_card(row: dict) -> Card:
    unit_ts_at = row.get("unit_tests_at")
    qa_at = row.get("qa_at")
    sprint_value = row.get("sprint_id")
    try:
        sprint_id = int(sprint_value) if sprint_value not in (None, "") else None
    except (TypeError, ValueError):
        sprint_id = None
    if sprint_id == 0:
        sprint_id = None

    return Card(
        id=int(row["id"]) if row.get("id") is not None else None,
        sprint_id=sprint_id,
        branch_key=row.get("branch_key") or None,
        title=row.get("title") or "",
        ticket_id=row.get("ticket_id") or "",
        branch=row.get("branch") or "",
        group_name=row.get("group_name") or None,
        assignee=row.get("assignee") or None,
        qa_assignee=row.get("qa_assignee") or None,
        description=row.get("description") or "",
        unit_tests_url=row.get("unit_tests_url") or None,
        qa_url=row.get("qa_url") or None,
        unit_tests_done=bool(row.get("unit_tests_done")),
        qa_done=bool(row.get("qa_done")),
        unit_tests_by=row.get("unit_tests_by") or None,
        qa_by=row.get("qa_by") or None,
        unit_tests_at=int(unit_ts_at) if unit_ts_at else None,
        qa_at=int(qa_at) if qa_at else None,
        status=row.get("status") or "pending",
        company_id=int(row.get("company_id") or 0) or None,
        closed_at=int(row.get("closed_at") or 0) or None,
        closed_by=row.get("closed_by") or None,
        branch_created_by=row.get("branch_created_by") or None,
        branch_created_at=int(row.get("branch_created_at") or 0) or None,
        created_at=int(row.get("created_at") or 0),
        created_by=row.get("created_by") or "",
        updated_at=int(row.get("updated_at") or 0),
        updated_by=row.get("updated_by") or "",
    )


def _row_to_company(row: dict) -> Company:
    return Company(
        id=int(row["id"]) if row.get("id") is not None else None,
        name=row.get("name") or "",
        group_name=row.get("group_name") or None,
        next_sprint_number=int(row.get("next_sprint_number") or 0) or 1,
        created_at=int(row.get("created_at") or 0),
        created_by=row.get("created_by") or None,
        updated_at=int(row.get("updated_at") or 0),
        updated_by=row.get("updated_by") or None,
    )


def _row_to_user(row: dict) -> User:
    changed_at = row.get("password_changed_at")
    active_since = row.get("active_since")
    return User(
        username=row.get("username") or "",
        display_name=row.get("display_name") or (row.get("username") or ""),
        active=bool(row.get("active", 1)),
        email=row.get("email") or None,
        has_password=bool(row.get("has_password") or row.get("password_hash")),
        require_password_reset=bool(row.get("require_password_reset", 0)),
        password_changed_at=int(changed_at) if changed_at else None,
        active_since=int(active_since) if active_since else None,
    )


def _row_to_role(row: dict) -> Role:
    return Role(
        key=row.get("key") or "",
        name=row.get("name") or (row.get("key") or ""),
        description=row.get("description") or "",
    )


# ---------------- index persistence -----------------

def _resolve_base(path: Optional[Path]) -> Path:
    if path is None:
        return _state_dir()
    if path.is_dir():
        return path
    return path.parent


def load_index(path: Optional[Path] = None, *, filter_origin: bool = False) -> Index:
    base = _resolve_base(path)
    db = _get_db(base)
    username = _current_username()
    records = db.fetch_branches(filter_origin=filter_origin, username=username)
    items: Index = {}
    for row in records:
        rec = _row_to_record(row)
        items[rec.key()] = rec
    return items


def save_index(index: Index, path: Optional[Path] = None) -> None:
    base = _resolve_base(path)
    db = _get_db(base)
    db.replace_branches(_records_to_payloads(index.values()))
    username = _current_username()
    for rec in index.values():
        _sync_local_binding(db, rec, username)


def load_local_states(
    *,
    branch_keys: Optional[Iterable[str]] = None,
    username: Optional[str] = None,
    path: Optional[Path] = None,
) -> List[dict]:
    base = _resolve_base(path)
    keys = list(branch_keys) if branch_keys else None
    rows = _get_db(base).fetch_branch_local_users(branch_keys=keys, username=username)
    entries: List[dict] = []
    for row in rows:
        entries.append(
            {
                "branch_key": row.get("branch_key"),
                "username": row.get("username"),
                "state": row.get("state"),
                "location": row.get("location"),
                "updated_at": int(row.get("updated_at") or 0),
            }
        )
    return entries


# ---------------- activity log -----------------

def record_activity(
    action: str,
    rec: BranchRecord,
    result: str = "ok",
    message: str = "",
    targets: Iterable[str] = ("local",),
) -> None:
    """Registra la actividad en el backend en línea."""
    entry = {
        "ts": int(time.time()),
        "user": rec.last_updated_by or rec.created_by,
        "group": rec.group,
        "project": rec.project,
        "branch": rec.branch,
        "action": action,
        "result": result,
        "message": message,
    }
    payload = dict(entry)
    payload["group_name"] = entry.get("group")
    payload["branch_key"] = (
        f"{entry.get('group') or ''}/{entry.get('project') or ''}/{entry.get('branch') or ''}"
    )
    _get_db(_state_dir()).append_activity([payload])




# ---------------- basic mutations -----------------

def upsert(rec: BranchRecord, index: Optional[Index] = None, action: str = "upsert") -> Index:
    now = int(time.time())
    rec.last_updated_at = now
    if not rec.created_at:
        rec.created_at = now
    base = _state_dir()
    db = _get_db(base)
    db.upsert_branch(_branch_payload(rec))
    username = _current_username()
    _sync_local_binding(db, rec, username)
    if index is None:
        idx = load_index()
    else:
        idx = index
        idx[rec.key()] = rec
    record_activity(action, rec)
    return idx


def remove(rec: BranchRecord, index: Optional[Index] = None) -> Index:
    _get_db(_state_dir()).delete_branch(rec.key())
    if index is None:
        idx = load_index()
    else:
        idx = index
        idx.pop(rec.key(), None)
    record_activity("remove", rec)
    return idx


# ---------------- sprint management -----------------

def list_sprints(*, branch_keys: Optional[Iterable[str]] = None, path: Optional[Path] = None) -> List[Sprint]:
    base = _resolve_base(path)
    rows = _get_db(base).fetch_sprints(branch_keys=list(branch_keys) if branch_keys else None)
    return [_row_to_sprint(row) for row in rows]


def get_sprint(sprint_id: int, *, path: Optional[Path] = None) -> Optional[Sprint]:
    if sprint_id is None:
        return None
    base = _resolve_base(path)
    row = _get_db(base).fetch_sprint(int(sprint_id))
    return _row_to_sprint(row) if row else None


def find_sprint_by_branch_key(branch_key: str, *, path: Optional[Path] = None) -> Optional[Sprint]:
    base = _resolve_base(path)
    row = _get_db(base).fetch_sprint_by_branch_key(branch_key)
    return _row_to_sprint(row) if row else None


def _split_branch_key(value: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not value:
        return (None, None, None)
    parts = value.split("/", 2)
    if len(parts) == 1:
        return (parts[0] or None, None, None)
    if len(parts) == 2:
        return (parts[0] or None, parts[1] or None, None)
    return (parts[0] or None, parts[1] or None, parts[2] or None)


def upsert_sprint(sprint: Sprint, *, path: Optional[Path] = None) -> Sprint:
    base = _resolve_base(path)
    db = _get_db(base)
    now = int(time.time())
    sprint.branch_key = (sprint.branch_key or "").strip()
    qa_branch = (sprint.qa_branch_key or "")
    sprint.qa_branch_key = qa_branch.strip() or None
    group_value = (sprint.group_name or "").strip()
    sprint.group_name = group_value or None
    if not sprint.created_at:
        sprint.created_at = now
    if not sprint.updated_at:
        sprint.updated_at = now
    existing_row = None
    if sprint.id:
        try:
            existing_row = db.fetch_sprint(int(sprint.id))
        except Exception:
            existing_row = None
    previous_status = (existing_row.get("status") or "open").lower() if existing_row else "open"
    previous_sequence = None
    if existing_row and existing_row.get("company_sequence") not in (None, ""):
        try:
            previous_sequence = int(existing_row.get("company_sequence"))
        except (TypeError, ValueError):
            previous_sequence = None
    company_row = None
    company_info: Optional[Company] = None
    if sprint.company_id not in (None, ""):
        try:
            sprint.company_id = int(sprint.company_id)
        except (TypeError, ValueError):
            sprint.company_id = None
        if sprint.company_id is not None:
            try:
                company_row = db.fetch_company(int(sprint.company_id))
            except Exception:
                company_row = None
            if company_row:
                company_info = _row_to_company(company_row)
    else:
        sprint.company_id = None
    if sprint.company_id is None:
        sprint.company_sequence = None
    else:
        if sprint.company_sequence in (None, 0):
            if (
                previous_sequence
                and existing_row
                and int(existing_row.get("company_id") or 0) == sprint.company_id
            ):
                sprint.company_sequence = previous_sequence
            else:
                next_value = 1
                if company_info:
                    try:
                        next_value = int(company_info.next_sprint_number or 1)
                    except (TypeError, ValueError):
                        next_value = 1
                sprint.company_sequence = next_value
        else:
            try:
                sprint.company_sequence = int(sprint.company_sequence)
            except (TypeError, ValueError):
                sprint.company_sequence = None
    payload = {
        "id": sprint.id,
        "branch_key": sprint.branch_key,
        "qa_branch_key": sprint.qa_branch_key,
        "group_name": sprint.group_name,
        "name": sprint.name,
        "version": sprint.version,
        "lead_user": sprint.lead_user,
        "qa_user": sprint.qa_user,
        "company_id": sprint.company_id,
        "company_sequence": sprint.company_sequence,
        "description": sprint.description,
        "status": sprint.status,
        "closed_at": sprint.closed_at,
        "closed_by": sprint.closed_by,
        "created_at": sprint.created_at,
        "created_by": sprint.created_by,
        "updated_at": sprint.updated_at,
        "updated_by": sprint.updated_by,
    }
    sprint_id = db.upsert_sprint(payload)
    sprint.id = sprint_id
    new_status = (sprint.status or "open").lower()
    if (
        sprint.company_id
        and sprint.company_sequence
        and new_status == "closed"
        and previous_status != "closed"
    ):
        if company_info is None and sprint.company_id:
            try:
                company_row = db.fetch_company(int(sprint.company_id))
                if company_row:
                    company_info = _row_to_company(company_row)
            except Exception:
                company_info = None
        if company_info:
            try:
                desired_next = int(sprint.company_sequence) + 1
            except (TypeError, ValueError):
                desired_next = None
            current_next = int(company_info.next_sprint_number or 1)
            if desired_next is not None and desired_next > current_next:
                company_info.next_sprint_number = desired_next
                company_info.updated_at = now
                company_info.updated_by = sprint.updated_by or _current_username()
                if not company_info.created_at:
                    company_info.created_at = now
                if not company_info.created_by:
                    company_info.created_by = company_info.updated_by
                upsert_company(company_info, path=base)
    return sprint


def delete_sprint(sprint_id: int, *, path: Optional[Path] = None) -> None:
    base = _resolve_base(path)
    _get_db(base).delete_sprint(int(sprint_id))


def list_companies(*, path: Optional[Path] = None) -> List[Company]:
    base = _resolve_base(path)
    rows = _get_db(base).fetch_companies()
    return [_row_to_company(row) for row in rows]


def upsert_company(company: Company, *, path: Optional[Path] = None) -> Company:
    base = _resolve_base(path)
    now = int(time.time())
    username = _current_username()
    company.name = (company.name or "").strip()
    group_value = (company.group_name or "").strip()
    company.group_name = group_value or None
    if company.id is None:
        if not company.created_at:
            company.created_at = now
        company.created_by = company.created_by or username
    if not company.created_at:
        company.created_at = now
    if not company.created_by:
        company.created_by = username
    company.updated_at = now
    company.updated_by = username
    if company.next_sprint_number <= 0:
        company.next_sprint_number = 1
    payload = {
        "id": company.id,
        "name": company.name,
        "group_name": company.group_name,
        "next_sprint_number": company.next_sprint_number,
        "created_at": company.created_at,
        "created_by": company.created_by,
        "updated_at": company.updated_at,
        "updated_by": company.updated_by,
    }
    company_id = _get_db(base).upsert_company(payload)
    company.id = company_id
    return company


def delete_company(company_id: int, *, path: Optional[Path] = None) -> None:
    base = _resolve_base(path)
    _get_db(base).delete_company(int(company_id))


def list_cards(
    *,
    sprint_ids: Optional[Iterable[int]] = None,
    branches: Optional[Iterable[str]] = None,
    company_ids: Optional[Iterable[int]] = None,
    group_names: Optional[Iterable[str]] = None,
    statuses: Optional[Iterable[str]] = None,
    include_closed: bool = True,
    without_sprint: bool = False,
    path: Optional[Path] = None,
) -> List[Card]:
    base = _resolve_base(path)
    ids = list(sprint_ids) if sprint_ids else None
    branch_list = list(branches) if branches else None
    company_list = list(company_ids) if company_ids else None
    group_list = list(group_names) if group_names else None
    status_list = list(statuses) if statuses else None
    rows = _get_db(base).fetch_cards(
        sprint_ids=ids,
        branches=branch_list,
        company_ids=company_list,
        group_names=group_list,
        statuses=status_list,
        include_closed=include_closed,
        without_sprint=without_sprint,
    )
    return [_row_to_card(row) for row in rows]


def _qa_branch_base_from_row(row: Optional[dict]) -> str:
    if not row:
        return ""
    key = (row.get("qa_branch_key") or row.get("branch_key") or "").strip()
    _, _, branch = _split_branch_key(key)
    return branch or ""


def _legacy_version_prefix(row: Optional[dict]) -> str:
    if not row:
        return ""
    version = str(row.get("version") or "").strip()
    if not version:
        return ""
    return f"v{version}_"


def _card_branch_prefix(card: Card, base: Path, row: Optional[dict] = None) -> str:
    if not getattr(card, "sprint_id", None):
        return ""
    if row is None:
        row = _get_db(base).fetch_sprint(int(card.sprint_id))
    if not row:
        return ""
    qa_base = _qa_branch_base_from_row(row)
    ticket = (getattr(card, "ticket_id", "") or "").strip()
    parts: List[str] = []
    if qa_base:
        parts.append(qa_base)
    else:
        legacy = _legacy_version_prefix(row)
        if legacy:
            parts.append(legacy.rstrip("_"))
    if ticket:
        parts.append(ticket)
    return "_".join([part for part in parts if part])


def _normalized_card_branch(card: Card, prefix: str, row: Optional[dict]) -> str:
    branch_value = (card.branch or "").strip()
    if not prefix and not branch_value:
        return ""
    qa_base = _qa_branch_base_from_row(row)
    legacy = _legacy_version_prefix(row) if prefix else ""
    ticket = (getattr(card, "ticket_id", "") or "").strip()
    suffix = branch_value
    trimmed = False
    if branch_value:
        if prefix and branch_value == prefix:
            suffix = ""
            trimmed = True
        elif prefix and branch_value.startswith(f"{prefix}_"):
            suffix = branch_value[len(prefix) + 1 :]
            trimmed = True
        elif prefix and qa_base and branch_value == qa_base:
            suffix = ""
            trimmed = True
        elif prefix and qa_base and branch_value.startswith(f"{qa_base}_"):
            remainder = branch_value[len(qa_base) + 1 :]
            if remainder and ticket:
                parts = remainder.split("_", 1)
                suffix = parts[1] if len(parts) == 2 else ""
            else:
                suffix = remainder
            trimmed = True
        elif prefix and legacy and branch_value.startswith(legacy):
            suffix = branch_value[len(legacy) :]
            trimmed = True
        elif prefix and branch_value.startswith(prefix):
            suffix = branch_value[len(prefix) :]
            trimmed = True
    if trimmed and suffix.startswith("_"):
        suffix = suffix[1:]
    suffix = suffix.strip()
    if prefix:
        return f"{prefix}_{suffix}" if suffix else prefix
    return suffix


def _card_branch_key(card: Card, base: Path) -> Optional[str]:
    if getattr(card, "branch_key", None):
        return card.branch_key
    if not getattr(card, "sprint_id", None):
        return None
    sprint = _get_db(base).fetch_sprint(int(card.sprint_id))
    if not sprint:
        return None
    group, project, _ = _split_branch_key(sprint.get("qa_branch_key"))
    if not any((group, project)):
        group, project, _ = _split_branch_key(sprint.get("branch_key"))
    branch = (card.branch or "").strip()
    if not branch:
        return None
    group_part = group or ""
    project_part = project or ""
    return f"{group_part}/{project_part}/{branch}".strip("/")


def upsert_card(card: Card, *, path: Optional[Path] = None) -> Card:
    base = _resolve_base(path)
    sprint_row = None
    if getattr(card, "sprint_id", None):
        sprint_row = _get_db(base).fetch_sprint(int(card.sprint_id))
    prefix = _card_branch_prefix(card, base, sprint_row)
    card.branch = _normalized_card_branch(card, prefix, sprint_row)
    card.branch_key = _card_branch_key(card, base)
    now = int(time.time())
    if not card.created_at:
        card.created_at = now
    card.updated_at = now
    payload = {
        "id": card.id,
        "sprint_id": card.sprint_id,
        "branch_key": card.branch_key,
        "title": card.title,
        "ticket_id": card.ticket_id,
        "branch": card.branch,
        "group_name": card.group_name,
        "assignee": card.assignee,
        "qa_assignee": card.qa_assignee,
        "description": card.description,
        "unit_tests_url": card.unit_tests_url or None,
        "qa_url": card.qa_url or None,
        "unit_tests_done": card.unit_tests_done,
        "qa_done": card.qa_done,
        "unit_tests_by": card.unit_tests_by,
        "qa_by": card.qa_by,
        "unit_tests_at": card.unit_tests_at,
        "qa_at": card.qa_at,
        "status": card.status,
        "company_id": card.company_id,
        "closed_at": card.closed_at,
        "closed_by": card.closed_by,
        "branch_created_by": card.branch_created_by,
        "branch_created_at": card.branch_created_at,
        "created_at": card.created_at,
        "created_by": card.created_by,
        "updated_at": card.updated_at,
        "updated_by": card.updated_by,
    }
    card_id = _get_db(base).upsert_card(payload)
    card.id = card_id
    return card


def delete_card(card_id: int, *, path: Optional[Path] = None) -> None:
    base = _resolve_base(path)
    _get_db(base).delete_card(int(card_id))


def assign_cards_to_sprint(
    sprint_id: int, card_ids: Iterable[int], *, path: Optional[Path] = None
) -> None:
    base = _resolve_base(path)
    ids = [int(cid) for cid in card_ids if cid not in (None, "")]
    if not ids:
        return
    _get_db(base).assign_cards_to_sprint(int(sprint_id), ids)


# ---------------- users & roles -----------------

def list_users(*, include_inactive: bool = False, path: Optional[Path] = None) -> List[User]:
    base = _resolve_base(path)
    rows = _get_db(base).fetch_users()
    users = [_row_to_user(row) for row in rows]
    if include_inactive:
        return users
    return [user for user in users if user.active]


def get_user(username: str, *, path: Optional[Path] = None) -> Optional[User]:
    base = _resolve_base(path)
    row = _get_db(base).fetch_user(username)
    if not row:
        return None
    return _row_to_user(row)


def set_user_active(username: str, active: bool, *, path: Optional[Path] = None) -> None:
    base = _resolve_base(path)
    timestamp = int(time.time()) if active else None
    _get_db(base).set_user_active(username, active, timestamp=timestamp)


def upsert_user(user: User, *, path: Optional[Path] = None) -> User:
    base = _resolve_base(path)
    payload = {
        "username": user.username,
        "display_name": user.display_name,
        "email": user.email,
        "active": user.active,
        "require_password_reset": user.require_password_reset,
    }
    _get_db(base).upsert_user(payload)
    set_user_active(user.username, bool(user.active), path=base)
    return get_user(user.username, path=base) or user


def set_user_password(
    username: str,
    password: str,
    *,
    require_reset: bool = False,
    path: Optional[Path] = None,
) -> None:
    base = _resolve_base(path)
    password_hash, salt, algo = _password_entry(password)
    changed_at = int(time.time())
    _get_db(base).update_user_password(
        username,
        password_hash=password_hash,
        password_salt=salt,
        password_algo=algo,
        password_changed_at=changed_at,
        require_password_reset=require_reset,
    )


def clear_user_password(username: str, *, path: Optional[Path] = None) -> None:
    base = _resolve_base(path)
    _get_db(base).update_user_password(
        username,
        password_hash=None,
        password_salt=None,
        password_algo=None,
        password_changed_at=None,
        require_password_reset=True,
    )


def mark_user_password_reset(
    username: str,
    *,
    require_reset: bool = True,
    path: Optional[Path] = None,
) -> None:
    base = _resolve_base(path)
    _get_db(base).mark_password_reset(username, require_reset)


def authenticate_user(
    username: str,
    password: Optional[str],
    *,
    path: Optional[Path] = None,
) -> AuthenticationResult:
    base = _resolve_base(path)
    row = _get_db(base).fetch_user(username)
    if not row:
        return AuthenticationResult(None, "not_found", "El usuario no existe.")

    user = _row_to_user(row)
    if not user.active:
        return AuthenticationResult(user, "disabled", "El usuario está deshabilitado.")

    has_password = bool(row.get("password_hash"))
    if not has_password:
        return AuthenticationResult(
            user,
            "password_required",
            "El usuario no tiene contraseña configurada.",
            needs_password=True,
        )

    if not password:
        return AuthenticationResult(
            user,
            "password_required",
            "Captura la contraseña.",
            needs_password=True,
        )

    valid = _verify_password(
        password,
        row.get("password_hash"),
        row.get("password_salt"),
        row.get("password_algo"),
    )
    if not valid:
        return AuthenticationResult(user, "invalid_credentials", "Contraseña incorrecta.")

    if row.get("require_password_reset"):
        return AuthenticationResult(
            user,
            "reset_required",
            "Es necesario restablecer la contraseña.",
            require_password_reset=True,
        )

    return AuthenticationResult(user, "ok")


def update_user_profile(
    username: str,
    *,
    display_name: Optional[str],
    email: Optional[str],
    path: Optional[Path] = None,
) -> None:
    base = _resolve_base(path)
    current = get_user(username, path=base)
    if not current:
        return
    new_display = display_name if display_name is not None else current.display_name
    new_email = email if email is not None else current.email
    _get_db(base).update_user_profile(username, new_display, new_email)


def create_user(
    username: str,
    display_name: str,
    *,
    email: Optional[str] = None,
    roles: Optional[Sequence[str]] = None,
    password: Optional[str] = None,
    active: bool = True,
    require_password_reset: bool = False,
    path: Optional[Path] = None,
) -> User:
    user = User(
        username=username,
        display_name=display_name,
        email=email,
        active=active,
        require_password_reset=require_password_reset or not bool(password),
    )
    created = upsert_user(user, path=path)
    if password:
        set_user_password(
            username,
            password,
            require_reset=require_password_reset,
            path=path,
        )
    else:
        mark_user_password_reset(username, require_reset=True, path=path)
    if roles is not None:
        set_user_roles(username, roles, path=path)
    return get_user(username, path=path) or created


def update_user(
    username: str,
    *,
    display_name: Optional[str] = None,
    email: Optional[str] = None,
    roles: Optional[Sequence[str]] = None,
    active: Optional[bool] = None,
    require_password_reset: Optional[bool] = None,
    path: Optional[Path] = None,
) -> Optional[User]:
    if display_name is not None or email is not None:
        update_user_profile(username, display_name=display_name, email=email, path=path)
    if active is not None:
        set_user_active(username, active, path=path)
    if require_password_reset is not None:
        mark_user_password_reset(username, require_reset=require_password_reset, path=path)
    if roles is not None:
        set_user_roles(username, roles, path=path)
    return get_user(username, path=path)


def delete_user(username: str, *, path: Optional[Path] = None) -> None:
    set_user_active(username, False, path=path)


def list_roles(*, path: Optional[Path] = None) -> List[Role]:
    base = _resolve_base(path)
    rows = _get_db(base).fetch_roles()
    return [_row_to_role(row) for row in rows]


def upsert_role(role: Role, *, path: Optional[Path] = None) -> Role:
    base = _resolve_base(path)
    payload = {
        "key": role.key,
        "name": role.name,
        "description": role.description,
    }
    _get_db(base).upsert_role(payload)
    return role


def delete_role(role_key: str, *, path: Optional[Path] = None) -> None:
    base = _resolve_base(path)
    _get_db(base).delete_role(role_key)


def list_user_roles(username: Optional[str] = None, *, path: Optional[Path] = None) -> Dict[str, List[str]]:
    base = _resolve_base(path)
    rows = _get_db(base).fetch_user_roles(username)
    roles: Dict[str, List[str]] = {}
    for row in rows:
        user = row.get("username") or ""
        roles.setdefault(user, []).append(row.get("role_key") or "")
    return roles


def set_user_roles(username: str, roles: Iterable[str], *, path: Optional[Path] = None) -> None:
    base = _resolve_base(path)
    _get_db(base).set_user_roles(username, list(roles))


# ---------------- filtering helpers -----------------



# ---------------- merging -----------------

def merge_indexes(a: Index, b: Index) -> Index:
    out: Index = {}
    out.update(a)
    for key, rec in b.items():
        if key not in out:
            out[key] = rec
            continue
        existing = out[key]
        if rec.last_updated_at >= existing.last_updated_at:
            out[key] = rec
    return out


def load_activity_log(path: Optional[Path] = None) -> List[dict[str, Any]]:
    """Return parsed activity log entries from *path* (defaults to local log)."""
    base = _resolve_base(path)
    rows = _get_db(base).fetch_activity()
    entries: List[dict[str, Any]] = []
    for row in rows:
        entries.append(
            {
                "ts": row.get("ts"),
                "user": row.get("user"),
                "group": row.get("group_name"),
                "project": row.get("project"),
                "branch": row.get("branch"),
                "action": row.get("action"),
                "result": row.get("result"),
                "message": row.get("message"),
            }
        )
    return entries

