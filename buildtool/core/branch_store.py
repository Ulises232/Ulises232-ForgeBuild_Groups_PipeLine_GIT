from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Optional, List
import json
import os
import time

from .config import load_config

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
    p.mkdir(parents=True, exist_ok=True)
    return p


def _index_path(base: Path) -> Path:
    return base / "branches_index.json"


def _log_path(base: Path) -> Path:
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


# ---------------- index persistence -----------------

def load_index(path: Optional[Path] = None) -> Index:
    p = path or _index_path(_state_dir())
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    items = {}
    for rec in raw.get("items", []):
        try:
            br = BranchRecord(**rec)
            items[br.key()] = br
        except Exception:
            continue
    return items


def save_index(index: Index, path: Optional[Path] = None) -> None:
    p = path or _index_path(_state_dir())
    tmp = p.with_suffix(".tmp")
    payload = {"version": 1, "items": [asdict(v) for v in index.values()]}
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


# ---------------- activity log -----------------

def _load_log(path: Path) -> List[str]:
    if not path.exists():
        return []
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []


def _append_log(lines: List[str], path: Path) -> None:
    with path.open("a", encoding="utf-8") as fh:
        for ln in lines:
            fh.write(ln + "\n")


def record_activity(action: str, rec: BranchRecord, result: str = "ok", message: str = "") -> None:
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
    p = _log_path(_state_dir())
    _append_log([json.dumps(entry, ensure_ascii=False)], p)


# ---------------- basic mutations -----------------

def upsert(rec: BranchRecord, index: Optional[Index] = None, action: str = "upsert") -> Index:
    idx = index or load_index()
    now = int(time.time())
    rec.last_updated_at = now
    if not rec.created_at:
        rec.created_at = now
    idx[rec.key()] = rec
    save_index(idx)
    record_activity(action, rec)
    return idx


def remove(rec: BranchRecord, index: Optional[Index] = None) -> Index:
    idx = index or load_index()
    idx.pop(rec.key(), None)
    save_index(idx)
    record_activity("remove", rec)
    return idx


# ---------------- filtering helpers -----------------

def _filter_origin(index: Index) -> Index:
    """Keep only records that were pushed to origin."""
    return {k: v for k, v in index.items() if v.exists_origin}


def _filter_log_by_index(lines: List[str], index: Index) -> List[str]:
    keys = set(index.keys())
    out: List[str] = []
    for ln in lines:
        try:
            entry = json.loads(ln)
            key = f"{entry.get('group') or ''}/{entry.get('project') or ''}/{entry.get('branch') or ''}"
        except Exception:
            continue
        if key in keys:
            out.append(ln)
    return out



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
    base = _nas_dir()
    local = load_index()
    nas = _filter_origin(load_index(_index_path(base)))
    merged = merge_indexes(local, nas)
    save_index(merged)

    # merge activity log
    local_log = _load_log(_log_path(_state_dir()))
    nas_log_raw = _load_log(_log_path(base))
    nas_log = _filter_log_by_index(nas_log_raw, nas)
    seen = set(local_log)
    new_lines = [ln for ln in nas_log if ln not in seen]
    if new_lines:
        _append_log(new_lines, _log_path(_state_dir()))
    return merged


def publish_to_nas() -> Index:
    base = _nas_dir()
    if not _acquire_lock(base):
        raise RuntimeError("NAS lock busy")
    try:
        local = _filter_origin(load_index())
        remote = _filter_origin(load_index(_index_path(base)))
        merged = merge_indexes(remote, local)
        save_index(merged, _index_path(base))

        # publish activity log
        base_log = _log_path(base)
        local_log_raw = _load_log(_log_path(_state_dir()))
        local_log = _filter_log_by_index(local_log_raw, local)
        remote_log_raw = _load_log(base_log)
        remote_log = _filter_log_by_index(remote_log_raw, remote)
        if remote_log != remote_log_raw:
            base_log.write_text("\n".join(remote_log) + ("\n" if remote_log else ""), encoding="utf-8")
        seen = set(remote_log)
        new_lines = [ln for ln in local_log if ln not in seen]
        if new_lines:
            _append_log(new_lines, base_log)
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
