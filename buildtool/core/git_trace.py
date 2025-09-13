
# buildtool/core/git_trace.py
from __future__ import annotations
import os, datetime
from typing import Callable, Optional

_EMIT: Optional[Callable[[str], None]] = None

def set_logger(emit: Callable[[str], None]) -> None:
    global _EMIT
    _EMIT = emit

def _now() -> str:
    return datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

def log(line: str) -> None:
    msg = f"{_now()} {line}"
    if _EMIT:
        try: _EMIT(msg)
        except Exception: pass
    try: print(msg)
    except Exception: pass
    path = os.environ.get("GIT_TRACE_FILE", "").strip()
    if path:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass
