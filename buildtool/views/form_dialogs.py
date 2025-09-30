from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QDialog, QVBoxLayout, QWidget


class FormDialog(QDialog):
    """Reusable dialog that hosts a standalone form widget."""

    def __init__(self, parent: QWidget | None, title: str, content: QWidget) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.setAttribute(Qt.WA_DeleteOnClose)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.addWidget(content)
        self._content = content

    @property
    def content(self) -> QWidget:
        return self._content
