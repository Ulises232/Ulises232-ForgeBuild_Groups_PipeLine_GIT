from PySide6.QtCore import QObject, Signal, Slot, QThread

from .thread_tracker import TRACKER


class TaskWorker(QObject):
    progress = Signal(str)
    finished = Signal(bool)

    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self._fn = fn
        self._args = args
        self._kwargs = kwargs

    @Slot()
    def run(self):
        ok = False
        try:
            ok = bool(self._fn(*self._args, **self._kwargs))
        except Exception as e:
            self.progress.emit(f"[task][ERROR] {e!r}")
            ok = False
        self.finished.emit(ok)


def run_in_thread(fn_or_worker, *args, **kwargs):
    """Ejecuta ``fn_or_worker`` en un ``QThread`` registrado en ``TRACKER``."""

    th = QThread()

    if isinstance(fn_or_worker, QObject):
        worker = fn_or_worker
        worker.moveToThread(th)
        if not hasattr(worker, "run") or not callable(getattr(worker, "run")):
            raise AttributeError("El worker debe exponer un método 'run' invocable")
        th.started.connect(worker.run)
    else:
        worker = TaskWorker(fn_or_worker, *args, **kwargs)
        worker.moveToThread(th)
        th.started.connect(worker.run)

    TRACKER.add(th)  # <--- IMPORTANTE
    return th, worker
