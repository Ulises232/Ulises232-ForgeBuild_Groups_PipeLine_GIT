
from __future__ import annotations
from typing import Optional, List, Tuple
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton, QTreeWidget, QTreeWidgetItem
)
from PySide6.QtCore import Qt
from ..core.config import Config
from ..core.git_tasks import discover_status
from ..core.gitwrap import list_local_branches, list_remote_branches

class BranchesView(QWidget):
    """Visor compacto de ramas por módulo (local vs origin)."""
    def __init__(self, cfg: Config, group_key: Optional[str], project_key: Optional[str], parent=None):
        super().__init__(parent)
        self.cfg = cfg
        self.group_key = group_key
        self.project_key = project_key
        self._setup_ui()
        self._refresh()

    def _setup_ui(self):
        root = QVBoxLayout(self); root.setContentsMargins(8,8,8,8); root.setSpacing(8)
        bar = QHBoxLayout()
        self.txtFilter = QLineEdit(); self.txtFilter.setPlaceholderText("Filtrar rama (contiene)")
        self.btnRefresh = QPushButton("Refrescar")
        bar.addWidget(QLabel("Ramas:")); bar.addWidget(self.txtFilter, 1); bar.addWidget(self.btnRefresh)
        root.addLayout(bar)
        self.tree = QTreeWidget(); self.tree.setHeaderLabels(["Módulo", "Locales", "Origin"])
        self.tree.setRootIsDecorated(False); self.tree.setAlternatingRowColors(True)
        root.addWidget(self.tree, 1)
        self.btnRefresh.clicked.connect(self._refresh)
        self.txtFilter.textChanged.connect(self._refresh)

    def _refresh(self):
        try:
            self.tree.clear()
            status = discover_status(self.cfg, self.group_key, self.project_key)
            filt = (self.txtFilter.text() or "").strip().lower()
            for name, br, path in status:
                try:
                    loc = list_local_branches(path)
                except Exception:
                    loc = []
                try:
                    rem = list_remote_branches(path)
                except Exception:
                    rem = []
                if filt:
                    if (not any(filt in s.lower() for s in loc)) and (not any(filt in s.lower() for s in rem)):
                        continue
                it = QTreeWidgetItem([name, ", ".join(sorted(loc)), ", ".join(sorted(rem))])
                self.tree.addTopLevelItem(it)
            self.tree.resizeColumnToContents(0)
        except Exception as e:
            # No hacemos propagar a Qt: fallos se ignoran y se puede reintentar.
            pass
