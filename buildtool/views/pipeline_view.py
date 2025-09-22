from __future__ import annotations

from PySide6.QtCore import QObject, Qt, Signal
from PySide6.QtWidgets import QHBoxLayout, QLabel, QStackedWidget, QVBoxLayout, QWidget

from qfluentwidgets import Pivot, ScrollArea, SubtitleLabel, StrongBodyLabel
from ..core.config import Config
from .build_view import BuildView
from .deploy_view import DeployView
from .pipeline_history_view import PipelineHistoryView
from ..ui.icons import get_icon


class PresetNotifier(QObject):
    changed = Signal()


class PipelineView(QWidget):
    def __init__(self, cfg: Config, on_request_reload_config):
        super().__init__()
        self.cfg = cfg
        self.on_request_reload_config = on_request_reload_config
        self.preset_notifier = PresetNotifier()
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(12)

        header_widget = QWidget(self)
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(12)

        header_icon = QLabel(header_widget)
        header_icon.setPixmap(get_icon("pipeline").pixmap(32, 32))
        header_layout.addWidget(header_icon, 0, Qt.AlignVCenter)

        title_container = QWidget(header_widget)
        title_layout = QVBoxLayout(title_container)
        title_layout.setContentsMargins(0, 0, 0, 0)
        title_layout.setSpacing(2)

        title = SubtitleLabel("Pipeline de compilación y despliegue", title_container)
        description = StrongBodyLabel(
            "Construye, despliega y consulta el historial desde un solo lugar",
            title_container,
        )
        title_layout.addWidget(title)
        title_layout.addWidget(description)

        header_layout.addWidget(title_container, 1)
        root.addWidget(header_widget)

        self.pivot = Pivot(self)
        root.addWidget(self.pivot)

        self.stack = QStackedWidget(self)
        root.addWidget(self.stack, 1)

        self.build_view = BuildView(self.cfg, self.on_request_reload_config, self.preset_notifier)
        self.deploy_view = DeployView(self.cfg, self.preset_notifier)
        self.history_view = PipelineHistoryView(self.cfg)

        self._routes = {}
        self._register_page("build", self.build_view, "Build", "build")
        self._register_page("deploy", self.deploy_view, "Deploy", "deploy")
        self._register_page("history", self.history_view, "Historial", "history")

        self.pivot.currentItemChanged.connect(self._switch_to_route)
        self.pivot.setCurrentItem("build")
        self._switch_to_route("build")

    def _wrap_in_scroll(self, widget: QWidget) -> ScrollArea:
        area = ScrollArea(self)
        area.setWidgetResizable(True)
        area.setFrameShape(ScrollArea.NoFrame)
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        layout.addWidget(widget)
        area.setWidget(container)
        return area

    def _register_page(self, route: str, view: QWidget, label: str, icon_name: str) -> None:
        page = self._wrap_in_scroll(view)
        page.setObjectName(f"{route}Scroll")
        self.stack.addWidget(page)
        self._routes[route] = page
        self.pivot.addItem(route, label, icon=get_icon(icon_name))

    def _switch_to_route(self, route: str) -> None:
        target = self._routes.get(route)
        if target is None:
            return
        self.stack.setCurrentWidget(target)

    def closeEvent(self, event):
        for child in (self.build_view, self.deploy_view, self.history_view):
            try:
                child.close()
            except Exception:
                pass
        super().closeEvent(event)
