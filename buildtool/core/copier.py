
from __future__ import annotations
import shutil, pathlib, fnmatch
from typing import Iterable, List, Optional
from threading import Event

def copy_artifacts(
    src_dir: pathlib.Path,
    patterns: Iterable[str],
    dest_dir: pathlib.Path,
    log_cb=print,
    *,
    recursive: bool=False,
    exclude_suffixes: Optional[List[str]]=None,
    exclude_dirs: Optional[List[str]]=None,
    cancel_event: Event | None = None,
) -> int:
    dest_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    exclude_suffixes = exclude_suffixes or ["-sources.jar","-javadoc.jar","-tests.jar",".pom"]
    exclude_dirs = set(exclude_dirs or ["dependency","dependencies","lib","libs","WEB-INF","classes"])
    iterator = src_dir.rglob("*") if recursive else src_dir.glob("*")
    for path in iterator:
        if cancel_event and cancel_event.is_set():
            break
        if path.is_dir():
            continue
        name = path.name
        if recursive:
            rel_path = path.relative_to(src_dir)
            if any(part in exclude_dirs for part in rel_path.parts[:-1]):
                continue
        else:
            rel_path = None
        if any(name.endswith(s) for s in exclude_suffixes):
            continue
        if not any(fnmatch.fnmatch(name, pat) for pat in patterns):
            continue
        target = dest_dir / (rel_path if recursive else name)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        log_cb(f"Copiado: {path} -> {target}")
        n += 1
    return n
