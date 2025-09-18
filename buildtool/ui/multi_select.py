"""Widgets compartidos para selección múltiple.

Este módulo expone utilidades reutilizadas por distintas vistas del
aplicativo, en especial:

* :class:`Logger`: QObject muy simple que ofrece una señal ``line`` para
  redirigir líneas de texto a distintos consumidores.
* :class:`MultiSelectComboBox`: variante de ``QComboBox`` que permite
  seleccionar múltiples elementos y mostrar un resumen de la selección.

Ambas clases están documentadas para dejar claras sus señales y métodos
públicos, facilitando su reutilización.
"""

from __future__ import annotations

from PySide6.QtCore import Qt, Signal, QObject
from PySide6.QtGui import QStandardItemModel, QStandardItem
from PySide6.QtWidgets import QAbstractItemView, QComboBox


class Logger(QObject):
    """Interfaz mínima para reenviar mensajes de log.

    Señales
    -------
    line(str)
        Se emite cada vez que hay una nueva línea disponible para
        mostrar en la interfaz.
    """

    #: Señal emitida con cada línea de bitácora producida.
    line = Signal(str)


class MultiSelectComboBox(QComboBox):
    """``QComboBox`` que soporta selección múltiple mediante *checkboxes*.

    Parameters
    ----------
    placeholder:
        Texto a mostrar cuando no hay selección.
    show_max:
        Cantidad máxima de elementos listados antes de resumir la selección
        con ``+N``.
    parent:
        Widget padre estándar de Qt.

    Métodos públicos
    ----------------
    set_items(items, checked_all=False)
        Carga una nueva lista de elementos y define su estado inicial de
        selección.
    all_items()
        Devuelve la lista completa de etiquetas disponibles.
    checked_items()
        Devuelve únicamente los elementos seleccionados actualmente.
    """

    def __init__(self, placeholder: str = "Selecciona…", show_max: int = 2, parent=None) -> None:
        super().__init__(parent)
        self.setEditable(True)
        self.lineEdit().setReadOnly(True)
        self.lineEdit().setPlaceholderText(placeholder)
        self.setInsertPolicy(QComboBox.NoInsert)
        self.setFocusPolicy(Qt.StrongFocus)
        self._show_max = show_max

        model = QStandardItemModel(self)
        self.setModel(model)

        view = self.view()
        view.setSelectionMode(QAbstractItemView.SingleSelection)
        view.pressed.connect(self._on_item_pressed)

        self.setStyleSheet("QComboBox{min-width:220px;padding:6px 10px;}")

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------
    def set_items(self, items: list[str] | tuple[str, ...], checked_all: bool = False) -> None:
        """Carga el combo con ``items`` y marca todos si se indica.

        Parameters
        ----------
        items:
            Secuencia con las etiquetas a mostrar.
        checked_all:
            Si es ``True`` todos los elementos se marcan por defecto.
        """

        model: QStandardItemModel = self.model()
        model.clear()

        for text in items:
            item = QStandardItem(text)
            item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
            item.setData(Qt.Checked if checked_all else Qt.Unchecked, Qt.CheckStateRole)
            model.appendRow(item)

        self._refresh_display()

    def all_items(self) -> list[str]:
        """Devuelve todas las etiquetas disponibles en el combo."""

        model: QStandardItemModel = self.model()
        return [model.item(i).text() for i in range(model.rowCount())]

    def checked_items(self) -> list[str]:
        """Obtiene únicamente los elementos seleccionados."""

        selected: list[str] = []
        model: QStandardItemModel = self.model()

        for i in range(model.rowCount()):
            item: QStandardItem = model.item(i)
            if item.checkState() == Qt.Checked:
                selected.append(item.text())

        return selected

    # ------------------------------------------------------------------
    # Manejo interno
    # ------------------------------------------------------------------
    def _on_item_pressed(self, index) -> None:
        model: QStandardItemModel = self.model()
        item: QStandardItem = model.itemFromIndex(index)
        if item.checkState() == Qt.Checked:
            item.setCheckState(Qt.Unchecked)
        else:
            item.setCheckState(Qt.Checked)
        self._refresh_display()

    def _refresh_display(self) -> None:
        selected = self.checked_items()
        if not selected:
            self.lineEdit().setText("")
            return

        text = ", ".join(selected[: self._show_max])
        if len(selected) > self._show_max:
            text += f" +{len(selected) - self._show_max}"
        self.lineEdit().setText(text)

