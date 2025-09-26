
from __future__ import annotations
from typing import Optional, Callable
import os
from pathlib import Path
from functools import wraps
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QScrollArea,
    QSplitter,
    QTabWidget,
    QTextEdit,
    QToolButton,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
    QSizePolicy,
)
from PySide6.QtCore import Qt, Signal, QObject, QTimer, QSize, Slot
from ..core.config import Config
from ..core.config_queries import find_project, get_group, iter_group_projects
# Import our local-only shim
from ..core.git_tasks_local import (
    switch_branch, create_version_branches, create_branches_local,
    push_branch, delete_local_branch_by_name, merge_into_current_branch
)
from ..core import sprint_queries
from ..core.branch_store import upsert_card
from ..core import errguard
from ..core.bg import run_in_thread
from ..core.discover import discover_status_fast
from ..core.state import STATE
from PySide6 import QtCore, QtWidgets
from datetime import datetime
from ..core.git_fast import get_current_branch_fast
import shiboken6
from ..core.branch_store import load_index
from .activity_log_view import ActivityLogView
from .branch_history_view import BranchHistoryView
from ..ui.icons import get_icon
from ..ui.widgets import combo_with_arrow, set_combo_enabled

def safe_slot(fn: Callable):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except Exception as e:
            self._dbg(f"!! {fn.__name__}: {e}")
            return None
    return Slot()(wrapper)

def _is_valid_qobj(obj) -> bool:
    try:
        return obj is not None and shiboken6.isValid(obj)
    except Exception:
        return False


class Logger(QObject):
    line = Signal(str)

class GitView(QWidget):
    """v4.1 — Local-only shim: usa comandos git locales; acciones globales; historial y cache."""
    def __init__(self, cfg: Config, parent=None):
        super().__init__(parent)
        self.cfg = cfg
        self.logger = Logger()
        self._threads: list = []
        self._debug_enabled = bool(int(os.environ.get("FORGEBUILD_GITVIEW_DEBUG", "0")))
        self._setup_ui()
        self._wire_events()
        self._dbg(f"git_view.py from: {__file__}")
        self._load_projects_flat()
        QTimer.singleShot(0, self._post_init)
        self._dbg("init: post_init scheduled")

    def _dbg(self, msg: str, force: bool = False):
        lowered = msg.lower()
        important = force or "error" in lowered or "warn" in lowered or "fail" in lowered
        if not (important or self._debug_enabled):
            return
        s = f"[GitView] {msg}"
        try:
            errguard.log(s)
        except Exception:
            pass
        try:
            print(s)
        except Exception:
            pass
        try:
            self.logger.line.emit(s)
        except Exception:
            pass

    def _make_tool_button(self, text: str, icon_name: str) -> QToolButton:
        btn = QToolButton()
        btn.setText(text)
        btn.setIcon(get_icon(icon_name))
        btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        btn.setAutoRaise(True)
        btn.setIconSize(QSize(18, 18))
        return btn

    def _set_busy(self, busy: bool, note: str = ""):
        for w in (
            self.btnCreateLocal,
            self.btnPushBranch,
            self.btnDeleteBranch,
            self.btnRunCreateVersion,
            self.btnSwitch,
            self.btnMerge,
            self.btnRefresh,
            self.btnReconcile,
        ):
            try: w.setEnabled(not busy)
            except Exception: pass
        for combo in (self.cboProject, self.cboHistorySwitch, self.cboDeleteBranch, self.cboHistoryMerge):
            set_combo_enabled(combo, not busy)
        try:
            QApplication.setOverrideCursor(Qt.WaitCursor) if busy else QApplication.restoreOverrideCursor()
        except Exception:
            pass
        if note:
            self.logger.line.emit(note)

    def _alert(self, msg: str, error: bool = False):
        try:
            if error:
                QMessageBox.critical(self, "Git", msg)
            else:
                QMessageBox.information(self, "Git", msg)
        except Exception:
            self._dbg(f"ALERT: {msg}")

    def _setup_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(12)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(12)
        icon = QLabel()
        icon.setPixmap(get_icon("git").pixmap(32, 32))
        header.addWidget(icon)
        title = QLabel("Repositorios Git")
        title.setProperty("role", "title")
        header.addWidget(title)
        header.addStretch(1)
        self.btnRefresh = self._make_tool_button("Refrescar vista", "refresh")
        header.addWidget(self.btnRefresh)
        root.addLayout(header)

        self.tabs = QTabWidget()
        self.tabs.setTabPosition(QTabWidget.North)
        self.tabs.setMovable(False)
        root.addWidget(self.tabs, 1)

        main_panel = QWidget()
        main_layout = QVBoxLayout(main_panel)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        top_container = QWidget()
        top_layout = QVBoxLayout(top_container)
        top_layout.setContentsMargins(0, 0, 0, 0)
        top_layout.setSpacing(12)
        scroll.setWidget(top_container)
        main_layout.addWidget(scroll)
        self.tabs.addTab(main_panel, get_icon("git"), "Gestión")

        project_box = QGroupBox("Proyecto activo")
        proj = QGridLayout(project_box)
        proj.setHorizontalSpacing(10)
        proj.setVerticalSpacing(8)
        row = 0
        proj.addWidget(QLabel("Proyecto:"), row, 0)
        self.cboProject = QComboBox()
        proj.addWidget(combo_with_arrow(self.cboProject), row, 1)
        row += 1
        self.lblScope = QLabel("Acciones aplican a TODOS los módulos del proyecto actual.")
        proj.addWidget(self.lblScope, row, 0, 1, 2)
        row += 1
        self.lblCurrent = QLabel("Rama actual: ?")
        proj.addWidget(self.lblCurrent, row, 0, 1, 2)
        top_layout.addWidget(project_box)

        ops = QGroupBox("Acciones de ramas")
        opsl = QGridLayout(ops)
        opsl.setHorizontalSpacing(10)
        opsl.setVerticalSpacing(12)
        opsl.setColumnStretch(0, 1)
        opsl.setColumnStretch(1, 1)

        self.cboHistorySwitch = QComboBox()
        self.cboHistorySwitch.setEditable(True)
        self.btnSwitch = self._make_tool_button("Switch (global)", "branch")
        grp_switch = QGroupBox("Cambiar a otra rama")
        hs = QHBoxLayout(grp_switch)
        hs.setContentsMargins(10, 10, 10, 10)
        hs.setSpacing(10)
        hs.addWidget(combo_with_arrow(self.cboHistorySwitch), 1)
        hs.addWidget(self.btnSwitch)

        self.txtNewBranch = QLineEdit()
        self.txtNewBranch.setPlaceholderText("Nombre de la nueva rama")
        self.btnCreateLocal = self._make_tool_button("Crear (local, global)", "branch")
        self.btnPushBranch = self._make_tool_button("Push (global)", "push")
        grp_new = QGroupBox("Nueva rama")
        hnew = QHBoxLayout(grp_new)
        hnew.setContentsMargins(10, 10, 10, 10)
        hnew.setSpacing(10)
        hnew.addWidget(self.txtNewBranch, 1)
        hnew.addWidget(self.btnCreateLocal)
        hnew.addWidget(self.btnPushBranch)

        self.cboDeleteBranch = QComboBox()
        self.cboDeleteBranch.setEditable(True)
        self.chkConfirmDelete = QCheckBox("Confirmar")
        self.btnDeleteBranch = self._make_tool_button("Eliminar local (global)", "delete")
        grp_del = QGroupBox("Eliminar rama")
        hd = QHBoxLayout(grp_del)
        hd.setContentsMargins(10, 10, 10, 10)
        hd.setSpacing(10)
        hd.addWidget(combo_with_arrow(self.cboDeleteBranch), 1)
        hd.addWidget(self.chkConfirmDelete)
        hd.addWidget(self.btnDeleteBranch)

        self.txtVersion = QLineEdit()
        self.txtVersion.setPlaceholderText("3.00.17")
        self.chkQA = QCheckBox("Crear *_QA")
        self.btnRunCreateVersion = self._make_tool_button("Crear ramas de versión (local, global)", "version")
        grp_ver = QGroupBox("Ramas de versión")
        hv = QHBoxLayout(grp_ver)
        hv.setContentsMargins(10, 10, 10, 10)
        hv.setSpacing(10)
        hv.addWidget(self.txtVersion, 1)
        hv.addWidget(self.chkQA)
        hv.addWidget(self.btnRunCreateVersion)

        self.cboHistoryMerge = QComboBox()
        self.cboHistoryMerge.setEditable(True)
        self.chkMergePush = QCheckBox("Push al terminar")
        self.btnMerge = self._make_tool_button("Merge a rama actual (global)", "merge")
        grp_merge = QGroupBox("Merge a la rama actual")
        hm = QHBoxLayout(grp_merge)
        hm.setContentsMargins(10, 10, 10, 10)
        hm.setSpacing(10)
        hm.addWidget(combo_with_arrow(self.cboHistoryMerge), 1)
        hm.addWidget(self.chkMergePush)
        hm.addWidget(self.btnMerge)

        opsl.addWidget(grp_switch, 0, 0)
        opsl.addWidget(grp_new, 0, 1)
        opsl.addWidget(grp_del, 1, 0)
        opsl.addWidget(grp_ver, 1, 1)
        opsl.addWidget(grp_merge, 2, 0, 1, 2)

        misc = QHBoxLayout()
        misc.setSpacing(10)
        self.btnReconcile = self._make_tool_button("Reconciliar con Git (solo local)", "sync")
        misc.addStretch(1)
        misc.addWidget(self.btnReconcile)
        opsl.addLayout(misc, 3, 0, 1, 2)

        top_layout.addWidget(ops)

        modules_box = QGroupBox("Módulos y ramas actuales")
        modules_layout = QVBoxLayout(modules_box)
        modules_layout.setContentsMargins(10, 10, 10, 10)
        modules_layout.setSpacing(10)
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Módulo", "Rama actual"])
        self.tree.setRootIsDecorated(False)
        self.tree.setAlternatingRowColors(True)
        self.tree.setMinimumHeight(260)
        modules_layout.addWidget(self.tree)

        history_box = QGroupBox("Historial de ramas")
        history_layout = QVBoxLayout(history_box)
        history_layout.setContentsMargins(10, 10, 10, 10)
        history_layout.setSpacing(10)
        self.treeHist = QTreeWidget()
        self.treeHist.setHeaderLabels(["Rama", "Usuario", "Creación", "Local", "Origin", "Merge"])
        self.treeHist.setRootIsDecorated(False)
        self.treeHist.setAlternatingRowColors(True)
        self.treeHist.setMinimumHeight(260)
        history_layout.addWidget(self.treeHist)

        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(modules_box)
        splitter.addWidget(history_box)
        splitter.setChildrenCollapsible(False)
        splitter.setSizes([420, 420])
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 1)
        splitter.setMinimumHeight(320)
        modules_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        history_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        top_layout.addWidget(splitter, 1)

        detail_panel = QWidget()
        detail_layout = QVBoxLayout(detail_panel)
        detail_layout.setContentsMargins(0, 0, 0, 0)
        detail_layout.setSpacing(0)

        bottom_tabs = QTabWidget()
        bottom_tabs.setTabPosition(QTabWidget.North)
        bottom_tabs.setMovable(False)
        detail_layout.addWidget(bottom_tabs)
        self.tabs.addTab(detail_panel, get_icon("log"), "Registros")

        console = QWidget()
        clog_layout = QVBoxLayout(console)
        clog_layout.setContentsMargins(12, 12, 12, 12)
        clog_layout.setSpacing(8)
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setLineWrapMode(QTextEdit.NoWrap)
        clog_layout.addWidget(self.log, 1)
        self.btnClearLog = self._make_tool_button("Limpiar", "broom")
        hcl = QHBoxLayout()
        hcl.addStretch(1)
        hcl.addWidget(self.btnClearLog)
        clog_layout.addLayout(hcl)
        bottom_tabs.addTab(console, get_icon("log"), "Consola")

        self.historyView = BranchHistoryView(self)
        bottom_tabs.addTab(self.historyView, get_icon("branch"), "Historial")
        self.activityView = ActivityLogView(self)
        bottom_tabs.addTab(self.activityView, get_icon("history"), "Actividad")

        self.logger = Logger()
        self.logger.line.connect(self.log.append)

        self.tabs.setCurrentIndex(0)

    def _wire_events(self):
        self.cboProject.currentTextChanged.connect(self._on_project_changed)
        self.btnRefresh.clicked.connect(self._post_project_change)
        self.btnReconcile.clicked.connect(self._do_reconcile)
        self.btnSwitch.clicked.connect(self._do_switch)
        self.btnCreateLocal.clicked.connect(self._do_create_local)
        self.btnPushBranch.clicked.connect(self._do_push_branch)
        self.btnDeleteBranch.clicked.connect(self._do_delete_branch)
        self.btnRunCreateVersion.clicked.connect(self._do_create_version)
        self.btnMerge.clicked.connect(self._do_merge)
        self.btnClearLog.clicked.connect(self.log.clear)


    def _load_projects_flat(self):
        self._dbg("load_projects_flat: start")
        try:
            self.cboProject.blockSignals(True)
            self.cboProject.clear()
            count = 0
            seen = set()
            for group, project in iter_group_projects(self.cfg):
                if project.key in seen:
                    continue
                seen.add(project.key)
                self.cboProject.addItem(project.key, userData=group.key)
                count += 1
            if count == 0:
                self.cboProject.addItem("Sin proyectos", userData=None)
                self.cboProject.setEnabled(False)
            else:
                self.cboProject.setEnabled(True)
            self._dbg(f"load_projects_flat: added {count} projects")
        finally:
            self.cboProject.blockSignals(False)
        QTimer.singleShot(0, self._post_project_change)

    def _current_keys(self) -> tuple[Optional[str], Optional[str]]:
        pkey = self.cboProject.currentText().strip() or None
        idx = self.cboProject.currentIndex()
        gkey = self.cboProject.itemData(idx) if idx >= 0 else None
        return gkey, pkey

    def _current_project_branch(self, gkey: Optional[str], pkey: Optional[str]) -> Optional[str]:
        try:
            items = discover_status_fast(self.cfg, gkey, pkey) or []
            for _, _, path in items:
                current = get_current_branch_fast(path)
                if current:
                    return current
        except Exception as exc:
            self._dbg(f"current_project_branch: {exc}")
        text = self.lblCurrent.text() or ""
        if ":" in text:
            guess = text.split(":", 1)[1].strip()
            return guess or None
        return None

    @Slot(str)
    def _on_project_changed(self, _):
        self._dbg("on_project_changed")
        self._post_project_change()

    @Slot()
    def _post_init(self):
        self._dbg("post_init: start")
        self._post_project_change()
        self._dbg("post_init: end")

    def _get_project_obj(self, gkey: str | None, pkey: str | None):
        _, project = find_project(self.cfg, pkey, gkey)
        return project

    @Slot()
    def _post_project_change(self):
        self._dbg("post_project_change: start")
        try:
            gkey, pkey = self._current_keys()
            self._dbg(f"post_project_change: keys gkey={gkey} pkey={pkey}")

            # 1) UI: módulos
            try:
                items = discover_status_fast(self.cfg, gkey, pkey)
                self.lblScope.setText(
                    f"Acciones aplican a TODOS los módulos del proyecto actual. Módulos: {len(items)}"
                )
            except Exception:
                pass

            # 2.1) Fallback por groups.repos cuando no hay workspaces[gkey]
            try:
                grp = get_group(self.cfg, gkey)

                repo_path = None
                if grp:
                    # grp.repos puede ser dict (clave=nombre repo o proyecto)
                    repos = getattr(grp, "repos", {}) or {}
                    # Prioridad: pkey dentro de repos
                    if isinstance(repos, dict) and pkey in repos:
                        repo_path = repos[pkey]
                    # Si no hay pkey, pero solo hay 1 repo, úsalo
                    elif isinstance(repos, dict) and len(repos) == 1:
                        repo_path = list(repos.values())[0]
                    # Si el group tiene atributo 'root', como fallback
                    if not repo_path and getattr(grp, "root", None):
                        repo_path = getattr(grp, "root")

                self._dbg(f"post_project_change: groups.repos hit={bool(repo_path)} path={repo_path}")
                if repo_path:
                    os.environ['HERR_REPO'] = str(Path(repo_path).resolve())
                    self._dbg(f"post_project_change: HERR_REPO(groups.repos)={os.environ['HERR_REPO']}")
                    self._refresh_history()
                    self._refresh_summary()
                    self._refresh_branch_index()
                    self._dbg("post_project_change: end")
                    return
            except Exception as e:
                self._dbg(f"post_project_change: WARN groups.repos→HERR_REPO: {e}")

            # Si llegaste aquí, no hay workspace para gkey; aún así refresca UI
            self._refresh_history()
            self._refresh_summary()
            self._refresh_branch_index()
        except Exception as e:
            self._dbg(f"post_project_change: ERROR {e}")
        self._dbg("post_project_change: end")



    def _refresh_history(self):
        self._dbg("_refresh_history: start")
        if not _is_valid_qobj(self):
            return
        try:
            # Usa QSignalBlocker para no olvidar desbloquear si hay excepciones
            b1 = QtCore.QSignalBlocker(self.cboHistorySwitch)
            b2 = QtCore.QSignalBlocker(self.cboDeleteBranch)
            b3 = QtCore.QSignalBlocker(self.cboHistoryMerge)
            self.cboHistorySwitch.clear(); self.cboDeleteBranch.clear(); self.cboHistoryMerge.clear()
            gkey, pkey = self._current_keys()
            idx = load_index()
            records = [r for r in idx.values() if r.group == gkey and r.project == pkey]
            records.sort(key=lambda r: r.last_updated_at, reverse=True)
            for rec in records[:50]:
                self.cboHistorySwitch.addItem(rec.branch)
                self.cboDeleteBranch.addItem(rec.branch)
                self.cboHistoryMerge.addItem(rec.branch)
            self._dbg("_refresh_history: loaded")
        except Exception as e:
            self._dbg(f"_refresh_history: ERROR {e}")

    def _refresh_summary(self):
        self._dbg("_refresh_summary: start")
        if not _is_valid_qobj(self) or not _is_valid_qobj(self.tree):
            return
        try:
            self.tree.setUpdatesEnabled(False)
            self.tree.clear()

            gkey, pkey = self._current_keys()
            items = discover_status_fast(self.cfg, gkey, pkey) or []
            proj_current = None
            for name, br, path in items:
                current = get_current_branch_fast(path) or br or "?"
                STATE.set_current(gkey, pkey, name, current)
                if proj_current is None:
                    proj_current = current
                it = QtWidgets.QTreeWidgetItem([name, current])
                self.tree.addTopLevelItem(it)

            self.lblCurrent.setText(f"Rama actual: {proj_current or '?'}")
            self.tree.resizeColumnToContents(0)
            self.tree.resizeColumnToContents(1)
            self._dbg(f"_refresh_summary: {self.tree.topLevelItemCount()} rows")
        except Exception as e:
            self._dbg(f"_refresh_summary: ERROR {e}")
        finally:
            if _is_valid_qobj(self.tree):
                self.tree.setUpdatesEnabled(True)

    def _refresh_branch_index(self):
        self._dbg("_refresh_branch_index: start")
        if not _is_valid_qobj(self) or not _is_valid_qobj(getattr(self, "treeHist", None)):
            return
        try:
            self.treeHist.setUpdatesEnabled(False)
            self.treeHist.clear()
            gkey, pkey = self._current_keys()
            idx = load_index()
            records = [r for r in idx.values() if r.group == gkey and r.project == pkey]
            records.sort(key=lambda r: r.last_updated_at, reverse=True)
            for rec in records:
                loc = "Sí" if rec.has_local_copy() else ""
                orig = "Sí" if rec.exists_origin else ""
                fecha = ""
                if rec.created_at:
                    fecha = datetime.fromtimestamp(rec.created_at).strftime("%Y-%m-%d %H:%M")
                it = QtWidgets.QTreeWidgetItem(
                    [rec.branch, rec.created_by, fecha, loc, orig, rec.merge_status]
                )
                self.treeHist.addTopLevelItem(it)
            self.treeHist.resizeColumnToContents(0)
            self.treeHist.resizeColumnToContents(1)
            self.treeHist.resizeColumnToContents(2)
        except Exception as e:
            self._dbg(f"_refresh_branch_index: ERROR {e}")
        finally:
            if _is_valid_qobj(self.treeHist):
                self.treeHist.setUpdatesEnabled(True)

    # en buildtool/views/git_view.py (dentro de GitView)
    def _init_thread_store(self):
        if not hasattr(self, "_live_threads"):
            self._live_threads = set()

    def _start_task(self, title, fn, done_cb=None, *args, success: str | None = None, error: str | None = None, **kwargs):
        self._dbg(f"task start: {title}")
        th, worker = run_in_thread(fn, *args, **kwargs)

        self._pending_done_cb = done_cb
        self._pending_success = success
        self._pending_error = error

        # Señales (no toques UI aquí; sólo enruta a la UI con el slot)
        worker.progress.connect(self.logger.line.emit)

        # Conexión QUEUED garantiza que el slot se ejecute en el hilo del receptor (self = UI)
        worker.finished.connect(self._on_task_finish, QtCore.Qt.QueuedConnection)

        # Limpieza del hilo/worker cuando termine (sin tocar UI)
        def _cleanup():
            try:
                th.quit()
                th.wait()
            except Exception:
                pass
            try:
                worker.deleteLater()
            except Exception:
                pass
            try:
                th.deleteLater()
            except Exception:
                pass
            try:
                from buildtool.core.thread_tracker import TRACKER
                TRACKER.remove(th)
            except Exception:
                pass

        worker.finished.connect(_cleanup, QtCore.Qt.QueuedConnection)
        th.start()

    @QtCore.Slot(bool)
    def _on_task_finish(self, ok: bool):
        """Este slot SIEMPRE corre en el hilo de la UI (QueuedConnection)."""
        try:
            self._dbg(f"task end (slot): ok={ok}", force=not ok)
            self._refresh_history()
            self._refresh_summary()
            self._refresh_branch_index()
            cb = getattr(self, "_pending_done_cb", None)
            if cb:
                try:
                    cb(ok)
                finally:
                    self._pending_done_cb = None
            msg = self._pending_success if ok else self._pending_error
            if msg:
                self._alert(msg, error=not ok)
        except Exception as e:
            errguard.log(f"_on_task_finish error: {e}", level=40)
        finally:
            self._pending_success = None
            self._pending_error = None

    def _set_task_buttons_enabled(self, enabled: bool):
        for btn in (self.btnCreateLocal, self.btnPushBranch, self.btnDeleteBranch, self.btnSwitch, self.btnMerge):
            try: btn.setEnabled(enabled)
            except Exception: pass


    @safe_slot
    def _do_switch(self):
        branch = self.cboHistorySwitch.currentText().strip()
        if not branch:
            self._alert("Especifica una rama", error=True); return
        gkey, pkey = self._current_keys()
        def _after(ok: bool):
            if not ok:
                return
            for name, _, _ in discover_status_fast(self.cfg, gkey, pkey):
                STATE.set_current(gkey, pkey, name, branch)
            STATE.add_history(gkey, pkey, branch)
        self._start_task(
            f"Switch a {branch} (global)",
            lambda cfg, gk, pk, br, emit=self.logger.line.emit: switch_branch(cfg, gk, pk, br, emit, only_modules=None),
            _after, self.cfg, gkey, pkey, branch,
            success=f"Cambiaste a {branch}",
            error=f"No se pudo cambiar a {branch}"
        )

    @safe_slot
    def _do_create_local(self):
        gkey, pkey = self._current_keys()
        name = self.txtNewBranch.text().strip()
        if not name:
            self._alert("Indica el nombre de la rama", error=True); return
        
        def task(cfg, gk, pk, br, emit=self.logger.line.emit):
            emit(f"[task] Crear rama local '{br}' (global)")
            ok = create_branches_local(cfg, gk, pk, br, emit=emit)
            emit("[task] DONE" if ok else "[task] DONE with errors")
            return ok

        self._start_task(
            f"Crear rama local {name} (global)",
            task, None, self.cfg, gkey, pkey, name,
            success=f"Rama {name} creada",
            error=f"No se pudo crear {name}"
        )


    @safe_slot
    def _do_push_branch(self):
        nb = self.txtNewBranch.text().strip() or self.cboHistorySwitch.currentText().strip()
        if not nb:
            self._alert("Indica la rama a enviar", error=True); return
        gkey, pkey = self._current_keys()
        def _after(ok: bool):
            if not ok:
                return
            for name, _, _ in discover_status_fast(self.cfg, gkey, pkey):
                STATE.add_remote(gkey, pkey, name, nb)
        self._start_task(
            f"Push rama {nb} (global)",
            lambda cfg, gk, pk, name, emit=self.logger.line.emit: push_branch(cfg, gk, pk, name, emit, only_modules=None),
            _after, self.cfg, gkey, pkey, nb,
            success=f"Rama {nb} enviada a origin",
            error=f"No se pudo enviar {nb}"
        )

    @safe_slot
    def _do_merge(self):
        source = self.cboHistoryMerge.currentText().strip()
        if not source:
            self._alert("Indica la rama a hacer merge", error=True); return
        push = self.chkMergePush.isChecked()
        gkey, pkey = self._current_keys()

        target_branch = self._current_project_branch(gkey, pkey)
        if not target_branch:
            self._alert("No se pudo determinar la rama destino del merge", error=True)
            return

        card = sprint_queries.find_card_by_branch(source)
        source_key = sprint_queries.branch_key(gkey, pkey, source)
        sprint_for_source = sprint_queries.find_sprint_by_branch(source_key)
        allow_missing_qa = False

        if card:
            sprint_obj = sprint_queries.get_sprint(card.sprint_id)
            sprint_main = sprint_queries.sprint_branch_name(sprint_obj)
            sprint_qa = sprint_queries.sprint_qa_branch_name(sprint_obj)
            if target_branch and sprint_main and target_branch == sprint_main:
                card.status = "bloqueado"
                upsert_card(card)
                self._alert(
                    "Las ramas de tarjeta solo pueden integrarse a la rama QA del sprint.",
                    error=True,
                )
                self.logger.line.emit(
                    f"[merge] Bloqueado merge de {source}: destino '{target_branch}' es la rama madre"
                )
                return
            allow_missing_qa = bool(target_branch and sprint_qa and target_branch == sprint_qa)
            if not sprint_queries.is_card_ready_for_merge(card, allow_qa_missing=allow_missing_qa):
                card.status = "bloqueado"
                upsert_card(card)
                if not card.unit_tests_done:
                    detail = "faltan las pruebas unitarias"
                elif not allow_missing_qa and not card.qa_done:
                    detail = "falta la validación de QA"
                else:
                    detail = "faltan aprobaciones requeridas"
                self._alert(
                    f"La tarjeta asociada aún no cuenta con todas las aprobaciones ({detail}).",
                    error=True,
                )
                self.logger.line.emit(
                    f"[merge] Bloqueado merge de {source}: {detail}"
                )
                return
            card.status = "merge_en_progreso"
            upsert_card(card)

        if (
            sprint_for_source
            and sprint_queries.sprint_qa_branch_name(sprint_for_source) == source
        ):
            target_sprint_main = sprint_queries.sprint_branch_name(sprint_for_source)
            if target_branch and target_sprint_main and target_branch == target_sprint_main:
                pending = sprint_queries.cards_pending_release(
                    sprint_for_source.id or 0
                )
                if pending:
                    self._alert(
                        "No se puede mergear la rama QA hasta que todas las tarjetas estén aprobadas por QA y pruebas unitarias.",
                        error=True,
                    )
                    self.logger.line.emit(
                        f"[merge] Bloqueado merge de {source}: {len(pending)} tarjetas pendientes"
                    )
                    return

        card_id = card.id if card else None

        def _after(ok: bool):
            if ok:
                STATE.add_history(gkey, pkey, source)
            if card_id:
                latest = sprint_queries.find_card_by_branch(source)
                if latest and latest.id == card_id:
                    latest.status = "merge_ok" if ok else "merge_error"
                    upsert_card(latest)

        self._start_task(
            f"Merge {source} -> rama actual (global)",
            lambda cfg, gk, pk, br, do_push, emit=self.logger.line.emit: merge_into_current_branch(cfg, gk, pk, br, do_push, emit, only_modules=None),
            _after, self.cfg, gkey, pkey, source, push,
            success=f"Merge de {source} completado",
            error=f"Merge de {source} tuvo errores"
        )

    @safe_slot
    def _do_delete_branch(self):
        nb = self.cboDeleteBranch.currentText().strip()
        if not nb:
            self._alert("Indica la rama a eliminar", error=True); return
        if not self.chkConfirmDelete.isChecked():
            self._alert("Confirma la eliminación", error=True); return
        gkey, pkey = self._current_keys()
        def _after(ok: bool):
            if not ok:
                return
            for name, _, _ in discover_status_fast(self.cfg, gkey, pkey):
                STATE.remove_local(gkey, pkey, name, nb)
        self._start_task(
            f"Eliminar rama local {nb} (global)",
            lambda cfg, gk, pk, name, confirm, emit=self.logger.line.emit: delete_local_branch_by_name(cfg, gk, pk, name, confirm, emit, only_modules=None),
            _after, self.cfg, gkey, pkey, nb, True,
            success=f"Rama {nb} eliminada",
            error=f"No se pudo eliminar {nb}"
        )

    @safe_slot
    def _do_create_version(self):
        ver = self.txtVersion.text().strip()
        if not ver:
            self._alert("Indica la versión", error=True); return
        create_qa = self.chkQA.isChecked()
        gkey, pkey = self._current_keys()
        def _after(ok: bool):
            if not ok:
                return
            for name, _, _ in discover_status_fast(self.cfg, gkey, pkey):
                STATE.add_local(gkey, pkey, name, ver)
                STATE.set_current(gkey, pkey, name, ver)
                if create_qa:
                    qa = f"{ver}_QA"
                    STATE.add_local(gkey, pkey, name, qa)
            STATE.add_history(gkey, pkey, ver)
            if create_qa:
                STATE.add_history(gkey, pkey, f"{ver}_QA")
        self._start_task(
            f"Crear ramas versión {ver} (global)",
            lambda cfg, gk, pk, v, qa, emit=self.logger.line.emit: create_version_branches(cfg, gk, pk, v, qa, {}, [], emit, only_modules=None),
            _after, self.cfg, gkey, pkey, ver, create_qa,
            success=f"Ramas {ver} creadas",
            error=f"No se pudieron crear ramas {ver}"
        )

    @safe_slot
    def _do_reconcile(self):
        gkey, pkey = self._current_keys()
        def reconcile_task(cfg, gk, pk, emit=self.logger.line.emit):
            from ..core.git_fast import get_current_branch_fast, list_local_branches_fast
            items = discover_status_fast(cfg, gk, pk)
            for name, _, path in items:
                cur = get_current_branch_fast(path) or "?"
                if emit: emit(f"{name}: {cur}")
                STATE.set_current(gk, pk, name, cur)
                for b in list_local_branches_fast(path):
                    STATE.add_local(gk, pk, name, b)
            return True
        self._start_task(
            "Reconciliar con Git (local)", reconcile_task, None, self.cfg, gkey, pkey,
            success="Reconciliación completa",
            error="Error al reconciliar"
        )

