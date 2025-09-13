from __future__ import annotations
import sys, os, traceback, atexit, io, faulthandler
from .core import errguard

CRASH_LOG = os.path.join(os.path.expanduser("~"), "forgebuild_crash.log")

def _open_crash_file():
    try:
        # Abre en append para acumular runs
        return open(CRASH_LOG, "a", encoding="utf-8")
    except Exception:
        return None

def main():
    print("== ForgeBuild app: entering main() ==")
    errguard.install(verbose=True)
    errguard.log(f"== app.main: python={sys.version} exe={sys.executable} cwd={os.getcwd()} ==")

    # === Ambiente Qt para evitar aborts por warnings y mejorar trazas ===
    os.environ.setdefault("QT_FATAL_WARNINGS", "0")         # no abort por warnings
    os.environ.setdefault("QT_ASSUME_STDERR_HAS_CONSOLE", "1")
    os.environ.setdefault("QT_LOGGING_RULES", "*.debug=true;qt.qpa.*=true")
    # Si has visto problemas de GPU/OpenGL, prueba:
    # os.environ.setdefault("QT_OPENGL", "software")

    # === Crash file & faulthandler ===
    crash_fh = _open_crash_file()
    if crash_fh:
        crash_fh.write("\n===== ForgeBuild start =====\n")
        crash_fh.flush()
        try:
            faulthandler.enable(file=crash_fh)
        except Exception:
            pass
        # Si cuelga, volcar trazas a los X segundos
        # faulthandler.dump_traceback_later(30.0, repeat=True, file=crash_fh)

        @atexit.register
        def _atexit_marker():
            try:
                crash_fh.write("===== ForgeBuild normal exit =====\n")
                crash_fh.flush()
            except Exception:
                pass

    try:
        from PySide6.QtWidgets import QApplication
        from PySide6.QtCore import qInstallMessageHandler, QtMsgType
    except Exception as e:
        errguard.log(f"!! cannot import PySide6: {e}")
        raise

    # 1) Excepciones no atrapadas del hilo principal
    def _sys_excepthook(exctype, value, tb):
        msg = "[UNCAUGHT] " + "".join(traceback.format_exception(exctype, value, tb))
        try:
            errguard.log(msg)
        except Exception:
            pass
        if crash_fh:
            try:
                crash_fh.write(msg + "\n")
                crash_fh.flush()
            except Exception:
                pass
        # No sys.exit aquí

    sys.excepthook = _sys_excepthook

    # 2) Excepciones “no alzables” (callbacks de GC, etc.)
    def _unraisable_hook(unraisable):
        line = f"[UNRAISABLE] {unraisable.exc_value!r} in {unraisable.object!r}"
        try:
            errguard.log(line)
        except Exception:
            pass
        if crash_fh:
            try:
                crash_fh.write(line + "\n")
                crash_fh.flush()
            except Exception:
                pass

    if hasattr(sys, "unraisablehook"):
        sys.unraisablehook = _unraisable_hook

    # 3) Mensajes de Qt a log y a archivo
    def _qt_msg_handler(mode, ctx, msg):
        level = {
            QtMsgType.QtDebugMsg:    "DBG",
            QtMsgType.QtInfoMsg:     "INF",
            QtMsgType.QtWarningMsg:  "WRN",
            QtMsgType.QtCriticalMsg: "CRT",
            QtMsgType.QtFatalMsg:    "FTL",
        }.get(mode, "UNK")
        line = f"[Qt/{level}] {msg}"
        try:
            errguard.log(line)
        except Exception:
            print(line)
        if crash_fh:
            try:
                crash_fh.write(line + "\n")
                crash_fh.flush()
            except Exception:
                pass
        # Nota: si es FATAL, Qt puede abortar; al menos queda trazado

    qInstallMessageHandler(_qt_msg_handler)

    app = QApplication.instance() or QApplication(sys.argv)
    errguard.log("== app.main: QApplication ready ==")

    from .main_window import MainWindow
    errguard.log("== app.main: importing MainWindow ok ==")
    w = MainWindow()
    errguard.log("== app.main: MainWindow() constructed ==")

    try:
        app.aboutToQuit.connect(lambda: errguard.log("== app.main: aboutToQuit =="))
    except Exception:
        pass

    w.show()
    errguard.log("== app.main: MainWindow shown, entering event loop ==")

    # IMPORTANTE: asegura que pares hilos si algo pide cerrar
    rc = 0
    try:
        rc = app.exec()
    finally:
        # Cierre forzado de hilos como última línea de defensa
        try:
            from .core.thread_tracker import TRACKER
            TRACKER.stop_all(timeout_ms=7000)
        except Exception:
            pass

        errguard.log(f"== app.main: event loop finished rc={rc} ==")
        if crash_fh:
            try:
                crash_fh.write(f"=====No es repo Gi ForgeBuild loop exit rc={rc} =====\n")
                crash_fh.flush()
            except Exception:
                pass

    sys.exit(rc)

if __name__ == "__main__":
    main()
