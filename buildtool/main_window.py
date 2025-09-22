from typing import Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QWidget

from qfluentwidgets import (
    FluentTitleBar,
    FluentWindow,
    NavigationInterface,
    NavigationItemPosition,
    PrimaryPushButton,
)

from buildtool import __version__
from buildtool.core.thread_tracker import TRACKER
from .core.config import load_config, Config
from .views.pipeline_view import PipelineView
from .views.git_view import GitView
from .views.groups_wizard import GroupsWizard
from .ui.icons import get_icon
from .ui.theme import apply_theme, ThemeMode


TAB_PIPELINE = "Pipeline"
TAB_GIT = "Repos (Git)"

_PIPELINE_ROUTE = "pipelineView"
_GIT_ROUTE = "gitView"


class MainWindow(FluentWindow):
    def __init__(self):
        super().__init__()
        self.setObjectName("MainWindow")
        self.setWindowTitle(f"ForgeBuild (Grupos) v{__version__}")
        self.resize(1280, 780)
        self.setWindowIcon(get_icon("app"))

        self.cfg: Config = load_config()
        self._groups_win: Optional[GroupsWizard] = None
        self._routes: dict[str, QWidget] = {}
        self._ensure_theme()

        self._setup_title_bar()
        self._setup_navigation()
        self.stackedWidget.currentChanged.connect(self._on_stack_changed)
        self._load_interfaces()
        self.btnGroups.clicked.connect(self.open_groups)

    def reload_config(self):
        current_route = self._current_route() or _PIPELINE_ROUTE

        self.cfg = load_config()

        self._clear_interfaces()

        self.pipeline = self._create_pipeline_view()
        self.git = self._create_git_view()
        self._register_interfaces()

        if self._route_to_widget(current_route) is None:
            current_route = _PIPELINE_ROUTE
        self._switch_to_route(current_route)

    def open_groups(self):
        if self._groups_win is None:
            self._groups_win = GroupsWizard(self.cfg, self.reload_config)
            self._groups_win.setAttribute(Qt.WA_DeleteOnClose, True)
            self._groups_win.setWindowModality(Qt.ApplicationModal)
            self._groups_win.resize(820, 680)
            self._groups_win.destroyed.connect(lambda: setattr(self, "_groups_win", None))
            self._groups_win.show()
        else:
            if not self._groups_win.isVisible(): self._groups_win.show()
        self._groups_win.raise_(); self._groups_win.activateWindow()
    
    def closeEvent(self, event):
        # apaga todos los hilos ANTES de cerrar la UI
        try:
            TRACKER.stop_all(timeout_ms=7000)
        except Exception:
            pass
        super().closeEvent(event)

    def _ensure_theme(self, mode: ThemeMode = "auto") -> None:
        apply_theme(mode)

    def set_theme_mode(self, mode: ThemeMode) -> str:
        """Expose theme switching for future UI toggles."""

        return apply_theme(mode)

    # --- Fluent UI helpers -------------------------------------------------

    def _setup_title_bar(self) -> None:
        self.btnGroups = PrimaryPushButton("Config/Wizard", self)
        self.btnGroups.setObjectName("configWizardButton")
        self.btnGroups.setIcon(get_icon("config"))
        self.btnGroups.setCursor(Qt.PointingHandCursor)

        try:
            self.setTitleBar(FluentTitleBar(self))
        except Exception:
            return

        if not hasattr(self, "titleBar"):
            return

        self.titleBar.hBoxLayout.insertStretch(2, 1)
        self.titleBar.hBoxLayout.insertWidget(3, self.btnGroups, 0, Qt.AlignRight | Qt.AlignVCenter)

    def _setup_navigation(self) -> None:
        if not isinstance(getattr(self, "navigationInterface", None), NavigationInterface):
            self.navigationInterface = NavigationInterface(self, showMenuButton=True, showReturnButton=False)
            self.hBoxLayout.insertWidget(0, self.navigationInterface)

        self.navigationInterface.setReturnButtonVisible(False)
        self.navigationInterface.setMinimumExpandWidth(320)
        self.navigationInterface.setCollapsible(True)

    def _load_interfaces(self) -> None:
        self._clear_interfaces()
        self.pipeline = self._create_pipeline_view()
        self.git = self._create_git_view()
        self._register_interfaces()

    def _register_interfaces(self) -> None:
        for widget, icon_name, label, route in (
            (self.pipeline, "pipeline", TAB_PIPELINE, _PIPELINE_ROUTE),
            (self.git, "git", TAB_GIT, _GIT_ROUTE),
        ):
            if widget is None:
                continue
            self._add_interface(widget, icon_name, label, route)

        if self._routes:
            first_route = next(iter(self._routes))
            self._switch_to_route(first_route)

    def _create_pipeline_view(self) -> PipelineView:
        return PipelineView(self.cfg, self.reload_config)

    def _create_git_view(self) -> GitView:
        return GitView(self.cfg, self)

    def _route_to_widget(self, route: str) -> Optional[QWidget]:
        return self._routes.get(route)

    def _add_interface(self, widget: QWidget, icon_name: str, label: str, route: str) -> None:
        widget.setObjectName(route)
        self.stackedWidget.addWidget(widget)
        self._routes[route] = widget
        self.navigationInterface.addItem(
            routeKey=route,
            icon=get_icon(icon_name),
            text=label,
            onClick=lambda r=route: self._switch_to_route(r),
            position=NavigationItemPosition.TOP,
            tooltip=label,
        )

    def _switch_to_route(self, route: str) -> None:
        widget = self._route_to_widget(route)
        if widget is None:
            return
        super().switchTo(widget)
        self.navigationInterface.setCurrentItem(route)

    def _clear_interfaces(self) -> None:
        nav = getattr(self, "navigationInterface", None)
        for route, widget in list(self._routes.items()):
            try:
                widget.close()
            except Exception:
                pass
            self.stackedWidget.removeWidget(widget)
            if nav is not None:
                nav.removeWidget(route)
            widget.setParent(None)
            widget.deleteLater()
        self._routes.clear()
        self.pipeline = None
        self.git = None

    def _current_route(self) -> Optional[str]:
        current_widget = self.stackedWidget.currentWidget()
        for route, widget in self._routes.items():
            if widget is current_widget:
                return route
        return None

    def _on_stack_changed(self, index: int) -> None:
        widget = self.stackedWidget.widget(index)
        for route, candidate in self._routes.items():
            if candidate is widget:
                self.navigationInterface.setCurrentItem(route)
                break
