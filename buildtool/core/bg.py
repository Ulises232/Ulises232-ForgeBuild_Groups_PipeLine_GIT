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

def run_in_thread(fn, *args, **kwargs):
    th = QThread()
    worker = TaskWorker(fn, *args, **kwargs)
    worker.moveToThread(th)
    th.started.connect(worker.run)
    TRACKER.add(th)  # <--- IMPORTANTE
    return th, worker
