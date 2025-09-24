from __future__ import annotations
from dataclasses import dataclass, asdict, fields
from pathlib import Path
from typing import Dict, Optional, List, Any, Iterable
import os
import time
import json

from .config import load_config
from .branch_history_db import BranchHistoryDB, Sprint, Card, User, Role


class NasUnavailableError(RuntimeError):
    """Raised when the configured NAS directory cannot be accessed."""


# ---------------- paths helpers -----------------

def _root_dir() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except Exception:
        return Path.cwd()


def _state_dir() -> Path:
    base = os.environ.get("APPDATA")
    if base:
        d = Path(base) / "forgebuild"
    else:
        d = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")) / "forgebuild"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _nas_dir() -> Path:
    cfg = load_config()
    base = getattr(getattr(cfg, "paths", {}), "nas_dir", "")
    if not base:
        base = os.environ.get("NAS_DIR")
    if not base:
        base = str(_root_dir() / "_nas_dev")
    p = Path(base)
    try:
        p.mkdir(parents=True, exist_ok=True)
    except (FileNotFoundError, OSError) as exc:
        detail = getattr(exc, "strerror", "") or str(exc)
        raise NasUnavailableError(
            f"El recurso NAS en '{base}' no está disponible: {detail}"
        ) from exc
    return p


def _db_path(base: Path) -> Path:
    return base / "branches_history.sqlite3"


def _legacy_index_path(base: Path) -> Path:
    return base / "branches_index.json"


def _legacy_log_path(base: Path) -> Path:
    return base / "activity_log.jsonl"


# ---------------- data structures -----------------

@dataclass
class BranchRecord:
    branch: str
    group: Optional[str] = None
    project: Optional[str] = None
    created_at: int = 0
    created_by: str = ""
    exists_local: bool = True
    exists_origin: bool = False
    merge_status: str = "none"
    diverged: Optional[bool] = None
    stale_days: Optional[int] = None
    last_action: str = "create"
    last_updated_at: int = 0
    last_updated_by: str = ""

    def key(self) -> str:
        return f"{self.group or ''}/{self.project or ''}/{self.branch}"


Index = Dict[str, BranchRecord]


_BRANCH_RECORD_FIELDS = {f.name for f in fields(BranchRecord)}
_LEGACY_FIELD_ALIASES = {
    "last_update": "last_updated_at",
    "last_update_by": "last_updated_by",
}
_BOOL_FIELDS = {"exists_local", "exists_origin"}
_INT_FIELDS = {"created_at", "last_updated_at"}


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


_DB_CACHE: Dict[Path, BranchHistoryDB] = {}


def _get_db(base: Path) -> BranchHistoryDB:
    base = base.resolve()
    db = _DB_CACHE.get(base)
    if not db:
        db = BranchHistoryDB(_db_path(base))
        _DB_CACHE[base] = db
        _run_migrations(base, db)
    return db


def _run_migrations(base: Path, db: BranchHistoryDB) -> None:
    _migrate_index(base, db)
    _migrate_activity_log(base, db)


def _retire_legacy_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        backup = path.with_suffix(path.suffix + ".migrated")
        if backup.exists():
            path.unlink()
        else:
            path.rename(backup)
    except Exception:
        try:
            path.unlink()
        except Exception:
            pass


def _migrate_index(base: Path, db: BranchHistoryDB) -> None:
    legacy = _legacy_index_path(base)
    if not legacy.exists():
        return
    if db.fetch_branches():
        return
    try:
        raw = json.loads(legacy.read_text(encoding="utf-8"))
    except Exception:
        return
    index: Dict[str, BranchRecord] = {}
    for rec in raw.get("items", []):
        br = _normalize_record_payload(rec)
        if not br:
            continue
        index[br.key()] = br
    if index:
        db.replace_branches(_records_to_payloads(index.values()))
        _retire_legacy_file(legacy)


def _migrate_activity_log(base: Path, db: BranchHistoryDB) -> None:
    legacy = _legacy_log_path(base)
    if not legacy.exists():
        return
    entries: List[dict] = []
    try:
        for line in legacy.read_text(encoding="utf-8").splitlines():
            try:
                entry = json.loads(line)
            except Exception:
                continue
            if isinstance(entry, dict):
                entries.append(entry)
    except Exception:
        return
    if entries:
        db.append_activity(entries)
        _retire_legacy_file(legacy)


def _records_to_payloads(records: Iterable[BranchRecord]) -> List[dict]:
    payloads: List[dict] = []
    for rec in records:
        data = asdict(rec)
        data["group_name"] = data.pop("group", None)
        data["key"] = rec.key()
        payloads.append(data)
    return payloads


def _row_to_record(row: dict) -> BranchRecord:
    data = {
        "branch": row.get("branch"),
        "group": row.get("group_name"),
        "project": row.get("project"),
        "created_at": int(row.get("created_at") or 0),
        "created_by": row.get("created_by") or "",
        "exists_local": bool(row.get("exists_local")),
        "exists_origin": bool(row.get("exists_origin")),
        "merge_status": row.get("merge_status") or "",
        "diverged": None if row.get("diverged") is None else bool(row.get("diverged")),
        "stale_days": row.get("stale_days"),
        "last_action": row.get("last_action") or "",
        "last_updated_at": int(row.get("last_updated_at") or 0),
        "last_updated_by": row.get("last_updated_by") or "",
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
    return Sprint(
        id=int(row["id"]) if row.get("id") is not None else None,
        branch_key=row.get("branch_key") or "",
        name=row.get("name") or "",
        version=row.get("version") or "",
        lead_user=row.get("lead_user") or None,
        qa_user=row.get("qa_user") or None,
        description=row.get("description") or "",
        created_at=int(row.get("created_at") or 0),
        created_by=row.get("created_by") or "",
        updated_at=int(row.get("updated_at") or 0),
        updated_by=row.get("updated_by") or "",
    )


def _row_to_card(row: dict) -> Card:
    unit_ts_at = row.get("unit_tests_at")
    qa_at = row.get("qa_at")
    return Card(
        id=int(row["id"]) if row.get("id") is not None else None,
        sprint_id=int(row.get("sprint_id") or 0),
        title=row.get("title") or "",
        branch=row.get("branch") or "",
        assignee=row.get("assignee") or None,
        qa_assignee=row.get("qa_assignee") or None,
        description=row.get("description") or "",
        unit_tests_done=bool(row.get("unit_tests_done")),
        qa_done=bool(row.get("qa_done")),
        unit_tests_by=row.get("unit_tests_by") or None,
        qa_by=row.get("qa_by") or None,
        unit_tests_at=int(unit_ts_at) if unit_ts_at else None,
        qa_at=int(qa_at) if qa_at else None,
        status=row.get("status") or "pending",
    )


def _row_to_user(row: dict) -> User:
    return User(
        username=row.get("username") or "",
        display_name=row.get("display_name") or (row.get("username") or ""),
        active=bool(row.get("active", 1)),
        email=row.get("email") or None,
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
    records = db.fetch_branches(filter_origin=filter_origin)
    items: Index = {}
    for row in records:
        rec = _row_to_record(row)
        items[rec.key()] = rec
    return items


def save_index(index: Index, path: Optional[Path] = None) -> None:
    base = _resolve_base(path)
    db = _get_db(base)
    db.replace_branches(_records_to_payloads(index.values()))


# ---------------- activity log -----------------

def record_activity(
    action: str,
    rec: BranchRecord,
    result: str = "ok",
    message: str = "",
    targets: Iterable[str] = ("local",),
) -> None:
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
    seen: set[str] = set()
    for target in targets:
        if target in seen:
            continue
        seen.add(target)
        if target == "nas":
            try:
                base = _nas_dir()
            except NasUnavailableError as exc:
                raise NasUnavailableError(
                    f"No se pudo registrar la actividad en la NAS: {exc}"
                ) from exc
        else:
            base = _state_dir()
        payload = dict(entry)
        payload["group_name"] = entry.get("group")
        payload["branch_key"] = f"{entry.get('group') or ''}/{entry.get('project') or ''}/{entry.get('branch') or ''}"
        _get_db(base).append_activity([payload])


# ---------------- basic mutations -----------------

def upsert(rec: BranchRecord, index: Optional[Index] = None, action: str = "upsert") -> Index:
    now = int(time.time())
    rec.last_updated_at = now
    if not rec.created_at:
        rec.created_at = now
    _get_db(_state_dir()).upsert_branch(_records_to_payloads([rec])[0])
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


def upsert_sprint(sprint: Sprint, *, path: Optional[Path] = None) -> Sprint:
    base = _resolve_base(path)
    payload = {
        "id": sprint.id,
        "branch_key": sprint.branch_key,
        "name": sprint.name,
        "version": sprint.version,
        "lead_user": sprint.lead_user,
        "qa_user": sprint.qa_user,
        "description": sprint.description,
        "created_at": sprint.created_at,
        "created_by": sprint.created_by,
        "updated_at": sprint.updated_at,
        "updated_by": sprint.updated_by,
    }
    sprint_id = _get_db(base).upsert_sprint(payload)
    sprint.id = sprint_id
    return sprint


def delete_sprint(sprint_id: int, *, path: Optional[Path] = None) -> None:
    base = _resolve_base(path)
    _get_db(base).delete_sprint(int(sprint_id))


def list_cards(
    *,
    sprint_ids: Optional[Iterable[int]] = None,
    branches: Optional[Iterable[str]] = None,
    path: Optional[Path] = None,
) -> List[Card]:
    base = _resolve_base(path)
    ids = list(sprint_ids) if sprint_ids else None
    branch_list = list(branches) if branches else None
    rows = _get_db(base).fetch_cards(sprint_ids=ids, branches=branch_list)
    return [_row_to_card(row) for row in rows]


def upsert_card(card: Card, *, path: Optional[Path] = None) -> Card:
    base = _resolve_base(path)
    payload = {
        "id": card.id,
        "sprint_id": card.sprint_id,
        "title": card.title,
        "branch": card.branch,
        "assignee": card.assignee,
        "qa_assignee": card.qa_assignee,
        "description": card.description,
        "unit_tests_done": card.unit_tests_done,
        "qa_done": card.qa_done,
        "unit_tests_by": card.unit_tests_by,
        "qa_by": card.qa_by,
        "unit_tests_at": card.unit_tests_at,
        "qa_at": card.qa_at,
        "status": card.status,
    }
    card_id = _get_db(base).upsert_card(payload)
    card.id = card_id
    return card


def delete_card(card_id: int, *, path: Optional[Path] = None) -> None:
    base = _resolve_base(path)
    _get_db(base).delete_card(int(card_id))


# ---------------- users & roles -----------------

def list_users(*, include_inactive: bool = True, path: Optional[Path] = None) -> List[User]:
    base = _resolve_base(path)
    rows = _get_db(base).fetch_users()
    users = [_row_to_user(row) for row in rows]
    if include_inactive:
        return users
    return [user for user in users if user.active]


def upsert_user(user: User, *, path: Optional[Path] = None) -> User:
    base = _resolve_base(path)
    payload = {
        "username": user.username,
        "display_name": user.display_name,
        "email": user.email,
        "active": user.active,
    }
    _get_db(base).upsert_user(payload)
    return user


def delete_user(username: str, *, path: Optional[Path] = None) -> None:
    base = _resolve_base(path)
    _get_db(base).delete_user(username)


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

def _filter_origin(index: Index) -> Index:
    """Keep only records that were pushed to origin."""
    return {k: v for k, v in index.items() if v.exists_origin}


# ---------------- NAS sync -----------------

LOCK_NAME = "branches.lock"


def _acquire_lock(base: Path, timeout: int = 10) -> bool:
    lock = base / LOCK_NAME
    start = time.time()
    while lock.exists() and time.time() - start < timeout:
        time.sleep(0.1)
    try:
        lock.write_text(str(os.getpid()))
        return True
    except Exception:
        return False


def _release_lock(base: Path) -> None:
    try:
        (base / LOCK_NAME).unlink()
    except Exception:
        pass


def recover_from_nas() -> Index:
    try:
        base = _nas_dir()
    except NasUnavailableError as exc:
        raise NasUnavailableError(
            f"No se pudo acceder a la NAS para recuperar el historial: {exc}"
        ) from exc
    local = load_index()
    nas = load_index(base, filter_origin=True)
    merged = merge_indexes(local, nas)
    save_index(merged)
    if nas:
        nas_entries = _get_db(base).fetch_activity(branch_keys=nas.keys())
        if nas_entries:
            _get_db(_state_dir()).append_activity(nas_entries)
    return merged


def publish_to_nas() -> Index:
    try:
        base = _nas_dir()
    except NasUnavailableError as exc:
        raise NasUnavailableError(
            f"No se pudo acceder a la NAS para publicar el historial: {exc}"
        ) from exc
    if not _acquire_lock(base):
        raise RuntimeError("NAS lock busy")
    try:
        local_origin = _filter_origin(load_index())
        remote_origin = load_index(base, filter_origin=True)
        merged = merge_indexes(remote_origin, local_origin)
        save_index(merged, base)

        remote_db = _get_db(base)
        remote_db.prune_activity(merged.keys())
        if local_origin:
            entries = _get_db(_state_dir()).fetch_activity(branch_keys=local_origin.keys())
            remote_db.append_activity(entries)
        return merged
    finally:
        _release_lock(base)


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


def load_nas_index() -> Index:
    """Load the branches index stored in the NAS directory."""
    try:
        base = _nas_dir()
    except NasUnavailableError:
        return {}
    return load_index(base)


def save_nas_index(index: Index) -> None:
    """Persist the NAS index to disk."""
    try:
        base = _nas_dir()
    except NasUnavailableError as exc:
        raise NasUnavailableError(
            f"No se pudo guardar el índice en la NAS: {exc}"
        ) from exc
    save_index(index, base)


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


def load_nas_activity_log() -> List[dict[str, Any]]:
    """Return parsed activity log entries stored in the NAS directory."""
    try:
        base = _nas_dir()
    except NasUnavailableError:
        return []
    return load_activity_log(base)
