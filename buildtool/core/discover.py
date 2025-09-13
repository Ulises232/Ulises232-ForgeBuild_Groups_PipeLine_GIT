from __future__ import annotations
from typing import List, Tuple, Optional
from pathlib import Path
from .git_fast import get_current_branch_fast

def discover_status_fast(cfg, gkey: Optional[str], pkey: Optional[str]) -> List[tuple[str, str, Path]]:
    items: List[tuple[str,str,Path]] = []
    def _iter_modules():
        if hasattr(cfg, "groups") and cfg.groups:
            for g in cfg.groups:
                if gkey and g.key != gkey: continue
                for p in (g.projects or []):
                    if pkey and p.key != pkey: continue
                    for m in (p.modules or []):
                        yield p, m
        else:
            for p in (getattr(cfg, "projects", []) or []):
                if pkey and p.key != pkey: continue
                for m in (p.modules or []):
                    yield p, m
    for p, m in _iter_modules():
        path = Path(p.root) / m.path if getattr(p, "root", None) else Path(m.path)
        branch = get_current_branch_fast(path) or "?"
        items.append((m.name, branch, path))
    return items