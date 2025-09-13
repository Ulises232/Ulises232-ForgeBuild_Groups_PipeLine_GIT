from __future__ import annotations
from pathlib import Path
import json, threading, time
from typing import Dict, List, Any, Optional

DEFAULT_PATH = Path.cwd() / ".forgebuild" / "branches.json"
_LOCK = threading.Lock()

def _ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

class StateStore:
    """Persist small branch state to speed up UI (history, known locals/remotes, current)."""
    def __init__(self, path: Path = DEFAULT_PATH):
        self.path = path
        self.data: Dict[str, Any] = {}
        self._loaded = False

    def _key(self, gkey: Optional[str], pkey: Optional[str], module: Optional[str]=None) -> str:
        g = gkey or "_"
        p = pkey or "_"
        m = module or "_"
        return f"{g}::{p}::{m}"

    def load(self):
        with _LOCK:
            if self._loaded: return
            try:
                if self.path.exists():
                    self.data = json.loads(self.path.read_text(encoding="utf-8"))
                else:
                    self.data = {}
            except Exception:
                self.data = {}
            self._loaded = True

    def save(self):
        with _LOCK:
            try:
                _ensure_parent(self.path)
                self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass

    # ---------- history (per project) ----------
    def add_history(self, gkey: Optional[str], pkey: Optional[str], branch: str, limit: int=50):
        self.load()
        k = self._key(gkey, pkey, None)
        rec = self.data.setdefault(k, {"history": [], "updated": time.time()})
        hist: List[str] = rec.setdefault("history", [])
        if branch in hist:
            hist.remove(branch)
        hist.insert(0, branch)
        del hist[limit:]
        rec["updated"] = time.time()
        self.save()

    def get_history(self, gkey: Optional[str], pkey: Optional[str]) -> List[str]:
        self.load()
        k = self._key(gkey, pkey, None)
        rec = self.data.get(k, {})
        return list(rec.get("history", []))

    # ---------- module state ----------
    def set_current(self, gkey: Optional[str], pkey: Optional[str], module: str, branch: str):
        self.load()
        k = self._key(gkey, pkey, module)
        rec = self.data.setdefault(k, {"current": None, "locals": [], "remotes": [], "updated": time.time()})
        rec["current"] = branch
        rec["updated"] = time.time()
        self.save()

    def get_current(self, gkey: Optional[str], pkey: Optional[str], module: str) -> Optional[str]:
        self.load()
        k = self._key(gkey, pkey, module)
        rec = self.data.get(k, {})
        return rec.get("current")

    def add_local(self, gkey: Optional[str], pkey: Optional[str], module: str, branch: str):
        self.load()
        k = self._key(gkey, pkey, module)
        rec = self.data.setdefault(k, {"current": None, "locals": [], "remotes": [], "updated": time.time()})
        if branch not in rec["locals"]:
            rec["locals"].append(branch)
        rec["updated"] = time.time()
        self.save()

    def remove_local(self, gkey: Optional[str], pkey: Optional[str], module: str, branch: str):
        self.load()
        k = self._key(gkey, pkey, module)
        rec = self.data.setdefault(k, {"current": None, "locals": [], "remotes": [], "updated": time.time()})
        if branch in rec["locals"]:
            rec["locals"].remove(branch)
        if rec.get("current") == branch:
            rec["current"] = None
        rec["updated"] = time.time()
        self.save()

    def add_remote(self, gkey: Optional[str], pkey: Optional[str], module: str, branch: str):
        self.load()
        k = self._key(gkey, pkey, module)
        rec = self.data.setdefault(k, {"current": None, "locals": [], "remotes": [], "updated": time.time()})
        if branch not in rec["remotes"]:
            rec["remotes"].append(branch)
        rec["updated"] = time.time()
        self.save()

    def get_presence(self, gkey: Optional[str], pkey: Optional[str], module: str):
        self.load()
        k = self._key(gkey, pkey, module)
        rec = self.data.get(k, {})
        return list(rec.get("locals", [])), list(rec.get("remotes", []))

STATE = StateStore()
