# buildtool/app.py
from __future__ import annotations

import sys, os, traceback, atexit
from pathlib import Path

# PyInstaller ejecuta este archivo como script suelto (sin paquete). Aseguramos que el
# paquete `buildtool` sea importable agregando el directorio raíz al sys.path cuando
# no hay paquete padre definido.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from buildtool.core.errguard import install_error_guard, on_about_to_quit_flush, log
from buildtool.core.qt_silence import setup_qt_logging  # ← filtra niveles de Qt (no instala handler)

# Bitácora adicional (opcional) en el HOME del usuario
CRASH_LOG = os.path.join(os.path.expanduser("~"), "forgebuild_crash.log")

def _open_crash_file():
    try:
        return open(CRASH_LOG, "a", encoding="utf-8")
    except Exception:
        return None

def main():
    print("== ForgeBuild app: entering main() ==")

    # 1) Instalar guard de errores LO MÁS TEMPRANO POSIBLE (hooks + logger + faulthandler + handler Qt)
    install_error_guard(app_name="buildtool", verbose=True)

    # 2) Reglas de logging de Qt (solo filtra niveles; NO instala handler)
    #    Opciones: "warn" (por defecto), "error", "off"
    setup_qt_logging(level="warn")

    # 3) Variables de entorno Qt útiles (no fuerzan abort por warnings)
    os.environ.setdefault("QT_FATAL_WARNINGS", "0")
    os.environ.setdefault("QT_ASSUME_STDERR_HAS_CONSOLE", "1")
    # Si necesitas más verbosidad de Qt durante diagnóstico, descomenta:
    # os.environ.setdefault("QT_LOGGING_RULES", "*.debug=true;qt.qpa.*=true")
    # Si hay broncas de GPU/OpenGL en algunas máquinas:
    # os.environ.setdefault("QT_OPENGL", "software")

    # 4) Bitácora adicional (opcional)
    crash_fh = _open_crash_file()
    if crash_fh:
        crash_fh.write("\n===== ForgeBuild start =====\n")
        crash_fh.flush()

        @atexit.register
        def _atexit_marker():
            try:
                crash_fh.write("===== ForgeBuild normal exit =====\n")
                crash_fh.flush()
            except Exception:
                pass

    # 5) Crear QApplication DESPUÉS de instalar errguard y configurar reglas Qt
    try:
        from PySide6.QtWidgets import QApplication
    except Exception as e:
        log(f"!! cannot import PySide6: {e}")
        raise

    app = QApplication.instance() or QApplication(sys.argv)
    log("== app.main: QApplication ready ==")

    # (Opcional) Volcado extra en crash_fh para uncaught del main
    def _sys_excepthook(exctype, value, tb):
        msg = "[UNCAUGHT] " + "".join(traceback.format_exception(exctype, value, tb))
        try:
            log(msg)
        except Exception:
            pass
        if crash_fh:
            try:
                crash_fh.write(msg + "\n")
                crash_fh.flush()
            except Exception:
                pass
        # No hacer sys.exit(); errguard ya evita cierre abrupto

    sys.excepthook = _sys_excepthook

    if hasattr(sys, "unraisablehook"):
        def _unraisable_hook(unraisable):
            line = f"[UNRAISABLE] {unraisable.exc_value!r} in {unraisable.object!r}"
            try:
                log(line)
            except Exception:
                pass
            if crash_fh:
                try:
                    crash_fh.write(line + "\n")
                    crash_fh.flush()
                except Exception:
                    pass
        sys.unraisablehook = _unraisable_hook

    # 6) Crear y mostrar la ventana principal
    from buildtool.main_window import MainWindow
    log("== app.main: importing MainWindow ok ==")
    w = MainWindow()
    log("== app.main: MainWindow() constructed ==")
    w.show()
    log("== app.main: MainWindow shown, entering event loop ==")

    # 7) Conectar flush de logs al salir
    try:
        app.aboutToQuit.connect(lambda: log("== app.main: aboutToQuit =="))
        app.aboutToQuit.connect(on_about_to_quit_flush)
    except Exception:
        pass

    # 8) Ejecutar loop y cierre ordenado (apaga hilos si usas TRACKER)
    rc = 0
    try:
        rc = app.exec()
    finally:
        try:
            from buildtool.core.thread_tracker import TRACKER
            TRACKER.stop_all(timeout_ms=7000)
        except Exception:
            pass

        log(f"== app.main: event loop finished rc={rc} ==")
        if crash_fh:
            try:
                crash_fh.write(f"===== ForgeBuild loop exit rc={rc} =====\n")
                crash_fh.flush()
            except Exception:
                pass

        on_about_to_quit_flush()

    sys.exit(rc)

if __name__ == "__main__":
    main()
