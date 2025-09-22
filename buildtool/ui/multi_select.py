"""Widgets compartidos para selección múltiple."""

from __future__ import annotations

from typing import Iterable, Optional

from PySide6.QtCore import Qt, Signal, QObject, QPoint
from PySide6.QtGui import QAction
from PySide6.QtWidgets import QWidgetAction
from qfluentwidgets import ComboBox, SearchLineEdit
from qfluentwidgets.components.widgets.combo_box import ComboBoxMenu
from qfluentwidgets.components.widgets.menu import MenuAnimationType


class _MultiSelectMenu(ComboBoxMenu):
    """Combo box menu que conserva el popup al alternar elementos."""

    def _onItemClicked(self, item) -> None:  # type: ignore[override]
        action = item.data(Qt.ItemDataRole.UserRole)
        if action not in self._actions or not action.isEnabled():
            return

        if action.isCheckable():
            action.setChecked(not action.isChecked())
            return

        super()._onItemClicked(item)


class Logger(QObject):
    """Interfaz mínima para reenviar mensajes de log."""

    #: Señal emitida con cada línea de bitácora producida.
    line = Signal(str)


class MultiSelectComboBox(ComboBox):
    """Selector Fluent que soporta múltiples elementos marcados."""

    selectionChanged = Signal(list)

    def __init__(
        self, placeholder: str = "Selecciona…", show_max: int = 2, parent=None
    ) -> None:
        super().__init__(parent=parent)
        self._placeholder = placeholder
        self._show_max = show_max
        self._checked: dict[int, bool] = {}
        self._filter_enabled = False
        self._filter_placeholder = "Escribe para filtrar…"
        self._filter_text: str = ""
        self._menu_actions: list[tuple[QAction, int]] = []
        self._active_menu: Optional[_MultiSelectMenu] = None
        self._search_action: Optional[QWidgetAction] = None
        self._search_input: Optional[SearchLineEdit] = None

        self.setPlaceholderText(placeholder)
        self.setCurrentIndex(-1)

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------
    def set_items(
        self, items: Iterable[str], checked_all: bool = False
    ) -> None:
        """Carga el combo con ``items`` y marca todos si se indica."""

        texts = list(items)
        self.clear()
        for text in texts:
            self.addItem(text)
        if checked_all:
            self.set_checked_items(texts)
        else:
            self._refresh_summary()

    def all_items(self) -> list[str]:
        """Devuelve todas las etiquetas disponibles en el combo."""

        return [self.itemText(i) for i in range(self.count())]

    def checked_items(self) -> list[str]:
        """Obtiene únicamente los elementos seleccionados."""

        return [self.itemText(i) for i, checked in self._checked.items() if checked]

    def set_checked_items(self, items: Iterable[str]) -> None:
        """Marca únicamente los elementos presentes en ``items``."""

        wanted = set(items)
        for index in range(self.count()):
            text = self.itemText(index)
            self._checked[index] = text in wanted
        self._refresh_summary()
        self.selectionChanged.emit(self.checked_items())

    def enable_filter(self, placeholder: str = "Escribe para filtrar…") -> None:
        """Activa el filtrado incremental dentro del combo."""

        self._filter_enabled = True
        self._filter_placeholder = placeholder

    def apply_filter(self, text: str) -> None:
        """Permite aplicar el filtro desde código sin abrir el popup."""

        if not self._filter_enabled:
            return
        self._filter_text = text
        self._apply_filter_to_menu(text)

    @property
    def filter_text(self) -> str:
        """Devuelve el texto de filtro activo."""

        return self._filter_text

    # ------------------------------------------------------------------
    # Manejo interno
    # ------------------------------------------------------------------
    def addItem(self, text, icon=None, userData=None):  # type: ignore[override]
        super().addItem(text, icon, userData)
        self._checked[self.count() - 1] = False
        self.setCurrentIndex(-1)
        self._refresh_summary()

    def addItems(self, texts: Iterable[str]):  # type: ignore[override]
        for text in texts:
            self.addItem(text)

    def clear(self) -> None:  # type: ignore[override]
        super().clear()
        self._checked.clear()
        self.setCurrentIndex(-1)
        self._refresh_summary()

    def _refresh_summary(self) -> None:
        selected = self.checked_items()
        if not selected:
            super().setCurrentIndex(-1)
            self.setPlaceholderText(self._placeholder)
            return

        text = ", ".join(selected[: self._show_max])
        if len(selected) > self._show_max:
            text += f" +{len(selected) - self._show_max}"

        self._updateTextState(False)
        super().setText(text)

    def _on_item_toggled(self, index: int, checked: bool) -> None:
        self._checked[index] = checked
        self._refresh_summary()
        self.selectionChanged.emit(self.checked_items())

    def _apply_filter_to_menu(self, text: str) -> None:
        if not self._active_menu:
            return

        self._filter_text = text
        lower = text.lower()
        for action, index in self._menu_actions:
            if not text:
                action.setVisible(True)
            else:
                action.setVisible(lower in self.itemText(index).lower())

        if self._search_input and self._search_input.text() != text:
            self._search_input.blockSignals(True)
            self._search_input.setText(text)
            self._search_input.blockSignals(False)

    def _on_search_text_changed(self, text: str) -> None:
        self._apply_filter_to_menu(text)

    def _on_menu_closed(self) -> None:
        if self._search_input:
            self._filter_text = self._search_input.text()
        self._active_menu = None
        self._menu_actions = []
        self._search_input = None
        self._search_action = None
        self.dropMenu = None

    def _showComboMenu(self):  # type: ignore[override]
        if self.count() == 0:
            return

        menu = _MultiSelectMenu(self)
        self._active_menu = menu
        self._menu_actions = []

        if self._filter_enabled:
            search = SearchLineEdit(menu)
            search.setPlaceholderText(self._filter_placeholder)
            search.setText(self._filter_text)
            search_action = QWidgetAction(menu)
            search_action.setDefaultWidget(search)
            menu.addAction(search_action)
            self._search_input = search
            self._search_action = search_action
            search.textChanged.connect(self._on_search_text_changed)

        for index in range(self.count()):
            action = QAction(self.itemIcon(index), self.itemText(index), menu)
            action.setCheckable(True)
            action.setChecked(self._checked.get(index, False))
            action.toggled.connect(
                lambda checked, i=index: self._on_item_toggled(i, checked)
            )
            menu.addAction(action)
            self._menu_actions.append((action, index))

        menu.setMaxVisibleItems(self.maxVisibleItems())
        menu.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        menu.closedSignal.connect(self._on_menu_closed)
        self.dropMenu = menu

        if self._filter_enabled and self._filter_text:
            self._apply_filter_to_menu(self._filter_text)

        x = -menu.width() // 2 + menu.layout().contentsMargins().left() + self.width() // 2
        pd = self.mapToGlobal(QPoint(x, self.height()))
        hd = menu.view.heightForAnimation(pd, MenuAnimationType.DROP_DOWN)

        pu = self.mapToGlobal(QPoint(x, 0))
        hu = menu.view.heightForAnimation(pu, MenuAnimationType.PULL_UP)

        if hd >= hu:
            menu.view.adjustSize(pd, MenuAnimationType.DROP_DOWN)
            menu.exec(pd, aniType=MenuAnimationType.DROP_DOWN)
        else:
            menu.view.adjustSize(pu, MenuAnimationType.PULL_UP)
            menu.exec(pu, aniType=MenuAnimationType.PULL_UP)
