from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QTextEdit, QComboBox, QAbstractItemView
from PySide6.QtCore import Qt, Signal, QObject, QThread
from PySide6.QtGui import QStandardItemModel, QStandardItem
from ..core.config import Config
from ..core.tasks import build_project_scheduled

class Logger(QObject):
    line = Signal(str)

def _run_build_scheduled(cfg: Config, group_key: str, project_key: str, profiles: list[str], modules: list[str], logger: Logger):
    def emit(s: str): logger.line.emit(s)
    try:
        build_project_scheduled(
            cfg, project_key, profiles,
            modules_filter=set(modules) if modules else None,
            log_cb=emit, group_key=group_key
        )
        emit(">> Listo.")
    except Exception as e:
        emit(f"<< ERROR: {e}")

class MultiSelectComboBox(QComboBox):
    def __init__(self, placeholder="Selecciona…", show_max=2, parent=None):
        super().__init__(parent)
        self.setEditable(True)
        self.lineEdit().setReadOnly(True)
        self.lineEdit().setPlaceholderText(placeholder)
        self.setInsertPolicy(QComboBox.NoInsert)
        self.setFocusPolicy(Qt.StrongFocus)
        self._show_max = show_max
        model = QStandardItemModel(self); self.setModel(model)
        view = self.view(); view.setSelectionMode(QAbstractItemView.SingleSelection)
        view.pressed.connect(self._on_item_pressed)
        self.setStyleSheet("QComboBox{min-width:220px;padding:6px 10px;}")

    def set_items(self, items, checked_all=False):
        model: QStandardItemModel = self.model(); model.clear()
        for text in items:
            it = QStandardItem(text)
            it.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
            it.setData(Qt.Checked if checked_all else Qt.Unchecked, Qt.CheckStateRole)
            model.appendRow(it)
        self._refresh_display()

    def all_items(self):
        model: QStandardItemModel = self.model()
        return [model.item(i).text() for i in range(model.rowCount())]

    def checked_items(self):
        out=[]; model: QStandardItemModel = self.model()
        for i in range(model.rowCount()):
            it: QStandardItem = model.item(i)
            if it.checkState()==Qt.Checked: out.append(it.text())
        return out

    def _on_item_pressed(self, index):
        model: QStandardItemModel = self.model()
        it: QStandardItem = model.itemFromIndex(index)
        it.setCheckState(Qt.Unchecked if it.checkState()==Qt.Checked else Qt.Checked)
        self._refresh_display()

    def _refresh_display(self):
        sel=self.checked_items()
        if not sel: self.lineEdit().setText(""); return
        self.lineEdit().setText(", ".join(sel[:self._show_max]) + (f" +{len(sel)-self._show_max}" if len(sel)>self._show_max else ""))

class BuildView(QWidget):
    def __init__(self, cfg: Config, on_request_reload_config):
        super().__init__()
        self.cfg = cfg
        self.on_request_reload_config = on_request_reload_config
        lay = QVBoxLayout(self); lay.setContentsMargins(16,12,16,12); lay.setSpacing(10)

        row = QHBoxLayout(); row.setSpacing(12)
        row.addWidget(QLabel("Grupo:"))
        self.cboGroup = QComboBox()
        groups = [g.key for g in (cfg.groups or [])] or ["GLOBAL"]
        for g in groups: self.cboGroup.addItem(g, g)
        row.addWidget(self.cboGroup)

        self.lblProject = QLabel("Proyecto:")
        self.cboProject = QComboBox()
        row.addWidget(self.lblProject); row.addWidget(self.cboProject)

        row.addWidget(QLabel("Perfiles:"))
        self.cboProfiles = MultiSelectComboBox("Perfiles…", show_max=2); row.addWidget(self.cboProfiles)

        row.addWidget(QLabel("Módulos:"))
        self.cboModules = MultiSelectComboBox("Módulos…", show_max=2); row.addWidget(self.cboModules)

        self.btnBuildSel = QPushButton("Compilar seleccionados"); row.addWidget(self.btnBuildSel)
        self.btnBuildAll = QPushButton("Compilar TODOS"); row.addWidget(self.btnBuildAll)
        row.addStretch(1); lay.addLayout(row)

        self.log = QTextEdit(); self.log.setReadOnly(True)
        self.log.setStyleSheet("font-family: Consolas, monospace; font-size: 12px;")
        self.log.setMinimumHeight(320); lay.addWidget(self.log)

        self.btnBuildSel.clicked.connect(self.start_build_selected)
        self.btnBuildAll.clicked.connect(self.start_build_all)
        self.cboGroup.currentIndexChanged.connect(self.refresh_group)
        self.cboProject.currentIndexChanged.connect(self.refresh_project_data)

        self._worker = None
        self.refresh_group()

    def _current_group(self):
        val = self.cboGroup.currentData()
        return None if val=="GLOBAL" else val

    def _get_current_project(self):
        gkey = self._current_group()
        pkey = self.cboProject.currentData()
        proj = None
        if gkey:
            grp = next((g for g in self.cfg.groups if g.key==gkey), None)
            if grp:
                proj = next((p for p in grp.projects if p.key==pkey), None)
        if not proj:
            proj = next((p for p in self.cfg.projects if p.key==pkey), None)
        return proj, gkey

    def refresh_group(self):
        self.cboProject.clear()
        gkey = self._current_group()

        if gkey:
            grp = next((g for g in self.cfg.groups if g.key==gkey), None)
            projects = [p.key for p in (grp.projects if grp else [])]
            for k in projects: self.cboProject.addItem(k, k)
            show_proj = len(projects) > 1
            self.lblProject.setVisible(show_proj)
            self.cboProject.setVisible(show_proj)
        else:
            projects = [p.key for p in self.cfg.projects]
            for k in projects: self.cboProject.addItem(k, k)
            self.lblProject.setVisible(True)
            self.cboProject.setVisible(True)

        self.refresh_project_data()

    def refresh_project_data(self):
        gkey = self._current_group()
        pkey = self.cboProject.currentData()
        profiles = []
        modules = []

        if gkey:
            grp = next((g for g in self.cfg.groups if g.key==gkey), None)
            if grp:
                proj = next((p for p in grp.projects if p.key==pkey), None) if self.cboProject.isVisible() else (grp.projects[0] if grp.projects else None)
                if proj:
                    modules = [m.name for m in proj.modules]
                    if getattr(proj, "profiles", None): profiles = proj.profiles
                if not profiles and grp.profiles: profiles = grp.profiles
        if not modules:
            proj = next((p for p in self.cfg.projects if p.key==pkey), None)
            if proj:
                modules = [m.name for m in proj.modules]
                if getattr(proj, "profiles", None): profiles = proj.profiles
        if not profiles and self.cfg.profiles:
            profiles = self.cfg.profiles

        self.cboProfiles.set_items(profiles or [])
        self.cboModules.set_items(modules or [], checked_all=True)

    def _start_schedule(self, profiles):
        proj, gkey = self._get_current_project()
        if not proj:
            self.log.append("<< No hay proyecto configurado en este grupo."); return
        selected_modules = self.cboModules.checked_items() or self.cboModules.all_items()

        logger = Logger(); logger.line.connect(lambda s: self.log.append(s))
        self._worker = QThread(self)
        self._worker.run = lambda: _run_build_scheduled(self.cfg, gkey, proj.key, profiles, selected_modules, logger)
        self._worker.start()

    def start_build_selected(self):
        profiles = self.cboProfiles.checked_items()
        if not profiles: self.log.append("<< Elige al menos un perfil."); return
        self._start_schedule(profiles)

    def start_build_all(self):
        profiles = self.cboProfiles.all_items()
        if not profiles: self.log.append("<< No hay perfiles configurados."); return
        self._start_schedule(profiles)
