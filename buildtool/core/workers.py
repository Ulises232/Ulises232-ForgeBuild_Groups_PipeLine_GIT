"""Trabajadores reutilizables para tareas del *pipeline*.

Este módulo contiene utilidades compartidas entre las vistas de *Build* y
*Deploy* para ejecutar las operaciones pesadas en un hilo en segundo plano,
exponiendo señales uniformes para reportar progreso y finalización.
"""

from __future__ import annotations

from typing import Any, Callable

from PySide6.QtCore import QObject, Signal, Slot


class PipelineWorker(QObject):
    """Ejecuta una tarea de ``build`` o ``deploy`` reportando progreso.

    Parameters
    ----------
    task:
        Función a ejecutar. Debe aceptar un parámetro ``log_cb`` keyword
        argument con el que se reportan las líneas de salida.
    success_message:
        Mensaje opcional a emitir cuando la tarea concluye exitosamente.
    log_prefix:
        Prefijo a colocar delante de cada línea de log. Útil cuando se lanzan
        múltiples tareas en paralelo y se quiere distinguir su origen.
    **task_kwargs:
        Parámetros que se pasarán a ``task`` al momento de la ejecución.

    Señales
    -------
    progress(str)
        Emite cada línea generada por ``task``.
    finished(bool)
        Se emite con ``True`` si ``task`` completó sin errores.
    """

    progress = Signal(str)
    finished = Signal(bool)

    def __init__(
        self,
        task: Callable[..., Any],
        *,
        success_message: str | None = None,
        log_prefix: str | None = None,
        **task_kwargs: Any,
    ) -> None:
        super().__init__()
        self._task = task
        self._success_message = success_message
        self._log_prefix = f"[{log_prefix}] " if log_prefix else ""
        self._task_kwargs = task_kwargs

    # ------------------------------------------------------------------
    @Slot()
    def run(self) -> None:
        """Ejecuta la tarea encapsulada."""

        ok = True

        def emit(line: str) -> None:
            self.progress.emit(f"{self._log_prefix}{line}")

        try:
            self._task(log_cb=emit, **self._task_kwargs)
            if self._success_message:
                emit(self._success_message)
        except Exception as exc:  # pragma: no cover - propagación controlada
            ok = False
            emit(f"<< ERROR: {exc}")

        self.finished.emit(ok)


def build_worker(
    task: Callable[..., Any],
    *,
    success_message: str,
    **task_kwargs: Any,
) -> PipelineWorker:
    """Crea un :class:`PipelineWorker` para tareas de *build*.

    Parameters
    ----------
    task:
        Función a ejecutar (normalmente :func:`build_project_scheduled`).
    success_message:
        Mensaje a emitir al finalizar correctamente.
    **task_kwargs:
        Argumentos destinados a ``task``.
    """

    return PipelineWorker(task, success_message=success_message, **task_kwargs)


def deploy_worker(
    task: Callable[..., Any],
    *,
    profile: str,
    success_message: str,
    **task_kwargs: Any,
) -> PipelineWorker:
    """Crea un :class:`PipelineWorker` configurado para despliegues.

    Parameters
    ----------
    profile:
        Nombre del perfil que se está desplegando. También se utiliza como
        prefijo para los mensajes de log emitidos por el worker.
    """

    task_kwargs.setdefault("profile", profile)

    return PipelineWorker(
        task,
        log_prefix=profile,
        success_message=success_message,
        **task_kwargs,
    )

