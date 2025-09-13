
# buildtool/core/git_console_trace.py
# Trazador a **consola** (stdout) siempre, con flush inmediato.
# Opcional: guarda a archivo si GIT_TRACE_FILE está definido.
from __future__ import annotations
import os, sys, datetime

def _ts() -> str:
    return datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

def clog(line: str) -> None:
    msg = f"{_ts()} {line}"
    try:
        print(msg, flush=True)
    except Exception:
        try:
            sys.stdout.write(msg + "\n"); sys.stdout.flush()
        except Exception:
            pass
    path = os.environ.get("GIT_TRACE_FILE", "").strip()
    if path:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass
