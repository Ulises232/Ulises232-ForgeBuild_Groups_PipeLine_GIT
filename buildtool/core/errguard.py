from __future__ import annotations
import sys, os, threading, traceback, io
from pathlib import Path
from datetime import datetime

# ---- helper paths ----
def _log_dirs():
    dirs = []
    try:
        dirs.append(Path.cwd() / ".forgebuild")
    except Exception:
        pass
    try:
        if os.name == "nt":
            la = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
            if la:
                dirs.append(Path(la) / "ForgeBuild")
    except Exception:
        pass
    try:
        dirs.append(Path.home() / "ForgeBuildLogs")
    except Exception:
        pass
    # de-dup
    out, seen = [], set()
    for d in dirs:
        try:
            key = str(d.resolve())
        except Exception:
            key = str(d)
        if key not in seen:
            seen.add(key); out.append(d)
    return out

def _ensure_dirs(ds):
    ok = []
    for d in ds:
        try:
            d.mkdir(parents=True, exist_ok=True)
            ok.append(d)
        except Exception:
            pass
    return ok

def _open_files(ds):
    files = []
    for d in ds:
        try:
            f = (d / "app.log").open("a", encoding="utf-8", buffering=1, newline="\n")
            files.append(f)
        except Exception:
            pass
    return files

_files = []

def _write_all(text: str) -> None:
    for f in list(_files):
        try:
            f.write(text)
        except Exception:
            pass

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

class _Tee(io.TextIOBase):
    def __init__(self, real):
        self._real = real
    def write(self, s):
        try:
            _write_all(s)
        except Exception:
            pass
        try:
            return self._real.write(s)
        except Exception:
            return 0
    def flush(self):
        try:
            for f in _files:
                try:
                    f.flush()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            return self._real.flush()
        except Exception:
            return None

def log(msg: str) -> None:
    line = f"[{_ts()}] {msg}\n"
    _write_all(line)
    try:
        sys.__stdout__.write(line)
        sys.__stdout__.flush()
    except Exception:
        pass

def _exc_to_text(exctype, value, tb) -> str:
    return "".join(traceback.format_exception(exctype, value, tb))

def _install_qt_handler() -> None:
    try:
        from PySide6.QtCore import qInstallMessageHandler
    except Exception:
        return
    def handler(msg_type, context, message):
        # 0 Debug, 1 Warning, 2 Critical, 3 Fatal, 4 Info
        level = {0:"Debug",1:"Warning",2:"Critical",3:"Fatal",4:"Info"}.get(int(msg_type), str(msg_type))
        loc = getattr(context, "file", None)
        line = getattr(context, "line", None)
        prefix = f"[Qt/{level}]"
        if loc and line:
            prefix += f" {loc}:{line}"
        log(f"{prefix} {message}")
    try:
        qInstallMessageHandler(handler)
    except Exception:
        pass

_installed = False

def install(verbose: bool=False) -> None:
    global _installed, _files
    if _installed:
        return
    # Qt debug env
    os.environ.setdefault("QT_DEBUG_PLUGINS", "1")
    os.environ.setdefault("QT_LOGGING_RULES", "*.debug=true;qt.qpa.*=true")
    # log sinks
    dirs = _ensure_dirs(_log_dirs())
    _files = _open_files(dirs)
    log("== errguard.install: sinks ready ==")
    # tee stdout/stderr
    try:
        sys.stdout = _Tee(sys.__stdout__ if hasattr(sys, "__stdout__") and sys.__stdout__ else sys.stdout)
        sys.stderr = _Tee(sys.__stderr__ if hasattr(sys, "__stderr__") and sys.__stderr__ else sys.stderr)
    except Exception:
        pass
    # excepthooks
    def _hook(exctype, value, tb):
        log("== Unhandled exception ==\n" + _exc_to_text(exctype, value, tb))
    sys.excepthook = _hook
    if hasattr(threading, "excepthook"):
        def _thook(args):
            try:
                txt = _exc_to_text(args.exc_type, args.exc_value, args.exc_traceback)
            except Exception:
                txt = f"{args}"
            log("== Thread exception ==\n" + txt)
        threading.excepthook = _thook
    try:
        import asyncio
        loop = asyncio.get_event_loop()
        def _ah(loop, context):
            exc = context.get("exception")
            if exc:
                txt = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                log("== Asyncio exception ==\n" + txt)
            else:
                log("== Asyncio error == " + str(context))
        loop.set_exception_handler(_ah)
    except Exception:
        pass
    _install_qt_handler()
    _installed = True
    if verbose:
        log("== errguard: installed (verbose) ==")
