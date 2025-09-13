from __future__ import annotations
from PySide6.QtWidgets import QWidget, QVBoxLayout, QTabWidget
from ..core.config import Config
from .build_view import BuildView
from .deploy_view import DeployView

class PipelineView(QWidget):
    def __init__(self, cfg: Config, on_request_reload_config):
        super().__init__()
        self.cfg = cfg
        self.on_request_reload_config = on_request_reload_config
        root = QVBoxLayout(self); root.setContentsMargins(0,0,0,0); root.setSpacing(8)
        self.tabs = QTabWidget(); self.tabs.setTabPosition(QTabWidget.North); self.tabs.setMovable(False)
        root.addWidget(self.tabs)
        self.build_tab = BuildView(self.cfg, self.on_request_reload_config)
        self.deploy_tab = DeployView(self.cfg)
        self.tabs.addTab(self.build_tab, "Build")
        self.tabs.addTab(self.deploy_tab, "Deploy")
