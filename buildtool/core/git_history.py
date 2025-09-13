
from __future__ import annotations
from typing import Dict, List, Any
from pathlib import Path
import json, time

def _root_dir() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except Exception:
        return Path.cwd()

def _state_dir() -> Path:
    d = _root_dir() / ".forgebuild"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _hist_path() -> Path:
    return _state_dir() / "git_history.json"

def _load_raw() -> Dict[str, Any]:
    p = _hist_path()
    if not p.exists():
        return {"branches": [], "by_project": {}}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"branches": [], "by_project": {}}

def _save_raw(obj: Dict[str, Any]) -> None:
    try:
        _hist_path().write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def add_branch(group_key: str|None, project_key: str|None, name: str) -> None:
    obj = _load_raw()
    now = int(time.time())
    items: List[Dict[str, Any]] = obj.get("branches", [])
    items = [x for x in items if x.get("name") != name]
    items.insert(0, {"name": name, "ts": now})
    obj["branches"] = items[:100]
    scope = f"{group_key or ''}/{project_key or ''}"
    byp: Dict[str, Any] = obj.setdefault("by_project", {})
    sitems: List[Dict[str, Any]] = byp.get(scope, [])
    sitems = [x for x in sitems if x.get("name") != name]
    sitems.insert(0, {"name": name, "ts": now})
    byp[scope] = sitems[:50]
    _save_raw(obj)

def remove_branch(group_key: str|None, project_key: str|None, name: str) -> None:
    obj = _load_raw()
    scope = f"{group_key or ''}/{project_key or ''}"
    byp: Dict[str, Any] = obj.get("by_project", {})
    if scope in byp:
        byp[scope] = [x for x in byp[scope] if x.get("name") != name]
    obj["branches"] = [x for x in obj.get("branches", []) if x.get("name") != name]
    _save_raw(obj)

def recent_branches(group_key: str|None, project_key: str|None, limit: int=20) -> List[str]:
    obj = _load_raw()
    scope = f"{group_key or ''}/{project_key or ''}"
    byp = obj.get("by_project", {})
    lst = byp.get(scope)
    if lst:
        return [x["name"] for x in lst[:limit]]
    return [x["name"] for x in obj.get("branches", [])[:limit]]
