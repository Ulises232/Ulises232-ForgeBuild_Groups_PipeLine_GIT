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

from PySide6.QtCore import (
    Qt,
    Signal,
    Slot,
    QObject,
    QSortFilterProxyModel,
    QSignalBlocker,
    QModelIndex,
)
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

        self._display_placeholder = placeholder
        self._summary_text: str = ""
        self._filter_enabled = False
        self._filter_placeholder = "Escribe para filtrar…"
        self._filter_text: str = ""
        self._in_filter_mode = False
        self._model = QStandardItemModel(self)
        self._proxy_model = QSortFilterProxyModel(self)
        self._proxy_model.setFilterCaseSensitivity(Qt.CaseInsensitive)
        self._proxy_model.setFilterRole(Qt.DisplayRole)
        self._proxy_model.setSourceModel(self._model)
        super().setModel(self._proxy_model)
        self.setModelColumn(0)

        view = self.view()
        view.setSelectionMode(QAbstractItemView.SingleSelection)
        view.pressed.connect(self._on_item_pressed)

        self.setStyleSheet("QComboBox{min-width:220px;padding:6px 10px;}")

        self.lineEdit().textEdited.connect(self._on_line_edit_text_edited)
        self.lineEdit().editingFinished.connect(self._on_line_edit_editing_finished)

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

        self._model.clear()

        for text in items:
            item = QStandardItem(text)
            item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
            item.setData(Qt.Checked if checked_all else Qt.Unchecked, Qt.CheckStateRole)
            self._model.appendRow(item)

        self._apply_filter(self._filter_text if self._filter_enabled else "")
        self._refresh_display()

    def all_items(self) -> list[str]:
        """Devuelve todas las etiquetas disponibles en el combo."""

        return [self._model.item(i).text() for i in range(self._model.rowCount())]

    def checked_items(self) -> list[str]:
        """Obtiene únicamente los elementos seleccionados."""

        selected: list[str] = []
        for i in range(self._model.rowCount()):
            item: QStandardItem = self._model.item(i)
            if item.checkState() == Qt.Checked:
                selected.append(item.text())

        return selected

    def set_checked_items(self, items: list[str] | tuple[str, ...]) -> None:
        """Marca únicamente los elementos presentes en ``items``."""

        wanted = set(items)
        for i in range(self._model.rowCount()):
            item: QStandardItem = self._model.item(i)
            item.setCheckState(Qt.Checked if item.text() in wanted else Qt.Unchecked)

        self._refresh_display()

    def enable_filter(self, placeholder: str = "Escribe para filtrar…") -> None:
        """Activa el filtrado incremental dentro del combo."""

        if self._filter_enabled:
            self._filter_placeholder = placeholder
            return

        self._filter_enabled = True
        self._filter_placeholder = placeholder
        # No se requiere configuración adicional: el proxy se crea en ``__init__``.

    def apply_filter(self, text: str) -> None:
        """Permite aplicar el filtro desde código sin abrir el popup."""

        if not self._filter_enabled:
            return
        self._filter_text = text
        self._apply_filter(text)

    # ------------------------------------------------------------------
    # Manejo interno
    # ------------------------------------------------------------------
    @Slot(QModelIndex)
    def _on_item_pressed(self, index: QModelIndex) -> None:
        model_index = self._map_to_source(index)
        if not model_index.isValid():
            return

        item: QStandardItem = self._model.itemFromIndex(model_index)
        if item.checkState() == Qt.Checked:
            item.setCheckState(Qt.Unchecked)
        else:
            item.setCheckState(Qt.Checked)
        self._refresh_display()

    def _refresh_display(self) -> None:
        selected = self.checked_items()
        if not selected:
            self._summary_text = ""
            if not self._should_preserve_filter_text():
                self._set_line_edit_text("")
            return

        text = ", ".join(selected[: self._show_max])
        if len(selected) > self._show_max:
            text += f" +{len(selected) - self._show_max}"
        self._summary_text = text
        if not self._should_preserve_filter_text():
            self._set_line_edit_text(text)

    def showPopup(self) -> None:
        if self._filter_enabled:
            self._enter_filter_mode()
        super().showPopup()

    def hidePopup(self) -> None:
        super().hidePopup()
        if self._filter_enabled:
            self._leave_filter_mode()

    def _apply_filter(self, text: str) -> None:
        self._proxy_model.setFilterFixedString(text)

    def _map_to_source(self, index):
        return self._proxy_model.mapToSource(index)

    def _should_preserve_filter_text(self) -> bool:
        return self._filter_enabled and self._in_filter_mode and bool(self._filter_text)

    def _set_line_edit_text(self, text: str) -> None:
        blocker = QSignalBlocker(self.lineEdit())
        _ = blocker  # avoid unused warning
        self.lineEdit().setText(text)

    def _enter_filter_mode(self) -> None:
        self._in_filter_mode = True
        self.lineEdit().setReadOnly(False)
        self.lineEdit().setPlaceholderText(self._filter_placeholder)
        self._set_line_edit_text(self._filter_text)
        self.lineEdit().selectAll()

    def _leave_filter_mode(self) -> None:
        self._in_filter_mode = False
        self._filter_text = ""
        self._apply_filter("")
        self.lineEdit().setReadOnly(True)
        self.lineEdit().setPlaceholderText(self._display_placeholder)
        self._refresh_display()

    @Slot(str)
    def _on_line_edit_text_edited(self, text: str) -> None:
        if not self._filter_enabled or not self._in_filter_mode:
            return
        self._filter_text = text
        self._apply_filter(text)
        if not text:
            self._refresh_display()

    @Slot()
    def _on_line_edit_editing_finished(self) -> None:
        if not self._filter_enabled or not self._in_filter_mode:
            return
        if not self._filter_text:
            self._refresh_display()

