
from __future__ import annotations
import shutil, pathlib, fnmatch
from typing import Iterable, List, Optional

def copy_artifacts(src_dir: pathlib.Path, patterns: Iterable[str], dest_dir: pathlib.Path, log_cb=print, *, recursive: bool=False, exclude_suffixes: Optional[List[str]]=None, exclude_dirs: Optional[List[str]]=None) -> int:
    dest_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    exclude_suffixes = exclude_suffixes or ["-sources.jar","-javadoc.jar","-tests.jar",".pom"]
    exclude_dirs = set(exclude_dirs or ["dependency","dependencies","lib","libs","WEB-INF","classes"])
    iterator = src_dir.rglob("*") if recursive else src_dir.glob("*")
    for path in iterator:
        if path.is_dir():
            if recursive and path.name in exclude_dirs:
                continue
            continue
        name = path.name
        if any(name.endswith(s) for s in exclude_suffixes):
            continue
        if not any(fnmatch.fnmatch(name, pat) for pat in patterns):
            continue
        target = dest_dir / name
        shutil.copy2(path, target)
        log_cb(f"Copiado: {path} -> {target}")
        n += 1
    return n
