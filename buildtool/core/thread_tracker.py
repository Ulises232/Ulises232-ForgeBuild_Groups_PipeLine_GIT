# buildtool/core/thread_tracker.py
from __future__ import annotations
from PySide6.QtCore import QThread
from typing import Set

class ThreadTracker:
    def __init__(self) -> None:
        self._threads: Set[QThread] = set()

    def add(self, th: QThread) -> None:
        self._threads.add(th)

    def remove(self, th: QThread) -> None:
        self._threads.discard(th)

    def stop_all(self, timeout_ms: int = 5000) -> None:
        # Intenta cerrar TODOS los hilos vivos para evitar "QThread destroyed..." 
        for th in list(self._threads):
            try:
                th.quit()
                th.wait(timeout_ms)
            except Exception:
                pass
            finally:
                try:
                    th.deleteLater()
                except Exception:
                    pass
                self._threads.discard(th)

TRACKER = ThreadTracker()
