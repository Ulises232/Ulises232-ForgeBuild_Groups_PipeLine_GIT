# buildtool/core/errguard.py
from __future__ import annotations

import asyncio
import faulthandler
import io
import logging
import os
import platform
import sys
import threading
import traceback
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional, Callable

# ----------- Qt opcional -----------
try:
    from PySide6.QtCore import qInstallMessageHandler, QtMsgType, QMessageLogContext, qVersion
    from PySide6.QtWidgets import QApplication, QMessageBox
    _HAS_QT = True
except Exception:
    _HAS_QT = False
    # Shims para tipado
    QtMsgType = None
    QMessageLogContext = None

# ----------- estado global -----------
_APP_NAME = "buildtool"
_LOGGER: Optional[logging.Logger] = None
_FAULT_FILE: Optional[io.TextIOBase] = None
_INSTALLED = False  # idempotencia
_SHOW_DIALOGS = True

# ----------- utilidades -----------
def _default_logs_dir(app_name: str) -> str:
    # Respeta repo/local primero
    for path in (
        os.path.join(os.getcwd(), ".forgebuild", "logs"),
        os.path.join(os.getcwd(), "logs"),
    ):
        try:
            os.makedirs(path, exist_ok=True)
            return path
        except Exception:
            pass
    # Último recurso: temp
    import tempfile
    path = os.path.join(tempfile.gettempdir(), f"{app_name}_logs")
    os.makedirs(path, exist_ok=True)
    return path

def _fmt_env() -> str:
    parts = [
        f"Python: {sys.version.split()[0]}",
        f"Platform: {platform.platform()}",
        f"Exe: {sys.executable}",
        f"PID: {os.getpid()}",
        f"Time: {datetime.now().isoformat(timespec='seconds')}",
        f"WD: {os.getcwd()}",
    ]
    if _HAS_QT:
        try:
            parts.append(f"Qt: {qVersion()}")
        except Exception:
            pass
    return " | ".join(parts)

def _build_logger(app_name: str, logs_dir: Optional[str], verbose: bool) -> logging.Logger:
    global _LOGGER
    logs_dir = logs_dir or _default_logs_dir(app_name)
    log_path = os.path.join(logs_dir, f"{app_name}.log")

    logger = logging.getLogger(app_name)
    logger.setLevel(logging.DEBUG)

    # Evita duplicar handlers si reinstalan
    if not any(isinstance(h, RotatingFileHandler) and getattr(h, "_fb_path", None) == log_path
               for h in logger.handlers):
        fh = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
        fh._fb_path = log_path  # marker
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(threadName)s %(name)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    # Stream a stderr (visible en dev)
    if not any(isinstance(h, logging.StreamHandler) and getattr(h, "_fb_stderr", False)
               for h in logger.handlers):
        sh = logging.StreamHandler(sys.stderr)
        sh._fb_stderr = True
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(threadName)s %(name)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        sh.setFormatter(fmt)
        sh.setLevel(logging.DEBUG if verbose else logging.INFO)
        logger.addHandler(sh)

    logger.debug("Logger listo en %s", log_path)
    return logger

def _maybe_show_dialog(title: str, message: str) -> None:
    if not _SHOW_DIALOGS or not _HAS_QT:
        return
    try:
        app = QApplication.instance()
        if app is None:
            return
        def _show():
            mb = QMessageBox()
            mb.setIcon(QMessageBox.Critical)
            mb.setWindowTitle(title)
            mb.setText(message)
            mb.setStandardButtons(QMessageBox.Ok)
            mb.exec()
        if threading.current_thread() is threading.main_thread():
            _show()
        else:
            from PySide6.QtCore import QTimer
            QTimer.singleShot(0, _show)
    except Exception:
        pass

# ----------- handlers de error -----------
def _sys_excepthook(exc_type, exc, tb) -> None:
    msg = "".join(traceback.format_exception(exc_type, exc, tb))
    if _LOGGER:
        _LOGGER.critical("Excepción NO capturada\nENV: %s\n%s", _fmt_env(), msg)
    _maybe_show_dialog("Error crítico", "Ocurrió un error inesperado. Revisa el log para detalles.")

def _thread_excepthook(args: threading.ExceptHookArgs) -> None:
    try:
        msg = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
        if _LOGGER:
            _LOGGER.critical("Excepción en hilo '%s'\n%s", args.thread.name, msg)
        _maybe_show_dialog("Error en hilo", f"Un hilo falló ({args.thread.name}). Revisa el log.")
    except Exception:
        pass

def _asyncio_handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
    try:
        exc = context.get("exception")
        if exc:
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        else:
            tb = str(context)
        if _LOGGER:
            _LOGGER.error("Excepción en asyncio loop:\n%s", tb)
    except Exception:
        pass

def _unraisable_hook(unraisable) -> None:
    try:
        line = f"[UNRAISABLE] {unraisable.exc_value!r} in {unraisable.object!r}"
        if _LOGGER:
            _LOGGER.error(line)
    except Exception:
        pass

def _qt_message_handler(mode: int, context: "QMessageLogContext", message: str) -> None:
    """
    Handler único de Qt. Respeta el filtrado que ya definió qt_silence vía
    QT_LOGGING_RULES / QLoggingCategory; aquí solo registramos lo que Qt emita.
    """
    if not _LOGGER:
        return
    # Nivel
    lvl = {
        QtMsgType.QtDebugMsg: logging.DEBUG,
        QtMsgType.QtInfoMsg: logging.INFO,
        QtMsgType.QtWarningMsg: logging.WARNING,
        QtMsgType.QtCriticalMsg: logging.ERROR,
        QtMsgType.QtFatalMsg: logging.CRITICAL,
    }.get(mode, logging.INFO)

    # Contexto (puede venir vacío)
    try:
        file = getattr(context, "file", "") or ""
        line = getattr(context, "line", 0) or 0
        func = getattr(context, "function", "") or ""
        cat  = getattr(context, "category", "") or ""
        _LOGGER.log(lvl, "Qt: %s [%s:%s %s] {%s}", message, file, line, func, cat)
    except Exception:
        _LOGGER.log(lvl, "Qt: %s", message)

# ----------- API pública -----------
def install_error_guard(
    app_name: str = "buildtool",
    logs_dir: Optional[str] = None,
    verbose: bool = False,
    show_dialogs: bool = True,
) -> logging.Logger:
    """
    Instala TODOS los ganchos de error y devuelve el logger configurado.
    Idempotente: puedes llamarlo varias veces sin efectos adversos.
    """
    global _INSTALLED, _LOGGER, _APP_NAME, _SHOW_DIALOGS, _FAULT_FILE
    _APP_NAME = app_name
    _SHOW_DIALOGS = bool(show_dialogs)

    if _LOGGER is None:
        _LOGGER = _build_logger(app_name, logs_dir, verbose)

    if not _INSTALLED:
        # Python
        sys.excepthook = _sys_excepthook
        if hasattr(threading, "excepthook"):
            threading.excepthook = _thread_excepthook  # type: ignore[attr-defined]
        if hasattr(sys, "unraisablehook"):
            sys.unraisablehook = _unraisable_hook  # type: ignore[attr-defined]

        # asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop and loop.is_running():
                loop.set_exception_handler(_asyncio_handler)
            # Si no está corriendo, muchos frameworks crean loop luego;
            # en ese caso, puedes volver a setear handler donde crees el loop.
        except Exception:
            pass

        # faulthandler (archivo separado)
        try:
            fault_path = os.path.join(_default_logs_dir(app_name), f"{app_name}_faults.log")
            _FAULT_FILE = open(fault_path, "a", encoding="utf-8")
            faulthandler.enable(file=_FAULT_FILE, all_threads=True)
            _LOGGER.debug("faulthandler habilitado en: %s", fault_path)
        except Exception as e:
            _LOGGER.warning("No se pudo habilitar faulthandler: %s", e)

        # Qt: solo instalamos nuestro handler (qt_silence ya filtró reglas)
        if _HAS_QT:
            try:
                qInstallMessageHandler(_qt_message_handler)
                _LOGGER.debug("Qt message handler instalado (respetando QT_LOGGING_RULES existentes)")
            except Exception as e:
                _LOGGER.warning("No se pudo instalar qInstallMessageHandler: %s", e)

        _INSTALLED = True
        _LOGGER.info("ErrorGuard instalado. %s", _fmt_env())
    else:
        _LOGGER.debug("ErrorGuard ya instalado; se omite reinstalación.")

    return _LOGGER

# Alias para compatibilidad con tu app.py existente
def install(verbose: bool = False, app_name: str = "buildtool", logs_dir: Optional[str] = None) -> None:
    install_error_guard(app_name=app_name, logs_dir=logs_dir, verbose=verbose)

def on_about_to_quit_flush() -> None:
    """Vacía buffers y cierra archivos al apagar la app."""
    try:
        logging.shutdown()
    except Exception:
        pass
    try:
        if _FAULT_FILE:
            _FAULT_FILE.flush()
            _FAULT_FILE.close()
    except Exception:
        pass

# Logging helper público
def log(msg: str, level: int = logging.INFO) -> None:
    if _LOGGER is None:
        # Inicialización perezosa para no perder mensajes tempranos
        install_error_guard(_APP_NAME, None, verbose=False)
    try:
        _LOGGER.log(level, msg)
    except Exception:
        # Evita que el logging mismo truene la app
        try:
            sys.stderr.write(msg + "\n")
        except Exception:
            pass

def get_logger() -> logging.Logger:
    if _LOGGER is None:
        return install_error_guard(_APP_NAME, None, verbose=False)
    return _LOGGER
