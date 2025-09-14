
from __future__ import annotations
from typing import Optional, Callable
import os
from pathlib import Path
from functools import wraps
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QGridLayout, QLabel, QComboBox, QLineEdit,
    QPushButton, QCheckBox, QTextEdit, QHBoxLayout, QGroupBox, QTreeWidget, QTreeWidgetItem, QApplication
)
from PySide6.QtCore import Qt, Signal, QObject, QTimer
from ..core.config import Config
# Import our local-only shim
from ..core.git_tasks_local import (
    switch_branch, create_version_branches, create_branches_local,
    push_branch, delete_local_branch_by_name
)
from ..core import errguard
from ..core.bg import run_in_thread
from ..core.discover import discover_status_fast
from ..core.state import STATE
from PySide6 import QtCore, QtWidgets
import shiboken6
from buildtool.core import errguard

def safe_slot(fn: Callable):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except Exception as e:
            self._dbg(f"!! {fn.__name__}: {e}")
            return None
    return wrapper

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
        errguard.install(verbose=True)
        self.cfg = cfg
        self.logger = Logger()
        self._threads: list = []
        self._setup_ui()
        self._wire_events()
        self._dbg(f"git_view.py from: {__file__}")
        self._load_projects_flat()
        QTimer.singleShot(0, self._post_init)
        self._dbg("init: post_init scheduled")

    def _dbg(self, msg: str):
        s = f"[GitView] {msg}"
        try: errguard.log(s)
        except Exception: pass
        try: print(s)
        except Exception: pass
        try: self.logger.line.emit(s)
        except Exception: pass

    def _set_busy(self, busy: bool, note: str = ""):
        for w in (self.btnCreateLocal, self.btnPushBranch, self.btnDeleteBranch,
                  self.btnRunCreateVersion, self.btnSwitch, self.btnRefresh, self.btnReconcile):
            try: w.setEnabled(not busy)
            except Exception: pass
        try:
            self.cboProject.setEnabled(not busy)
        except Exception:
            pass
        try:
            QApplication.setOverrideCursor(Qt.WaitCursor) if busy else QApplication.restoreOverrideCursor()
        except Exception:
            pass
        if note:
            self.logger.line.emit(note)

    def _setup_ui(self):
        root = QVBoxLayout(self); root.setContentsMargins(8,8,8,8); root.setSpacing(8)
        top = QGridLayout(); top.setHorizontalSpacing(8); top.setVerticalSpacing(6)
        row = 0
        top.addWidget(QLabel("Proyecto:"), row, 0)
        self.cboProject = QComboBox(); top.addWidget(self.cboProject, row, 1); row += 1

        self.lblScope = QLabel("Acciones aplican a TODOS los módulos del proyecto actual."); top.addWidget(self.lblScope, row, 0, 1, 4); row += 1

        self.cboHistorySwitch = QComboBox(); self.cboHistorySwitch.setEditable(True)
        self.btnSwitch = QPushButton("Switch (global)")
        hs = QHBoxLayout(); hs.addWidget(QLabel("Rama:")); hs.addWidget(self.cboHistorySwitch, 1); hs.addWidget(self.btnSwitch)
        top.addLayout(hs, row, 0, 1, 4); row += 1

        self.txtNewBranch = QLineEdit(); self.txtNewBranch.setPlaceholderText("Nombre de la nueva rama")
        self.btnCreateLocal = QPushButton("Crear rama (local, global)"); self.btnPushBranch = QPushButton("Push rama (global)")
        hnew = QHBoxLayout(); hnew.addWidget(QLabel("Nueva rama:")); hnew.addWidget(self.txtNewBranch, 1); hnew.addWidget(self.btnCreateLocal); hnew.addWidget(self.btnPushBranch)
        top.addLayout(hnew, row, 0, 1, 4); row += 1

        self.cboDeleteBranch = QComboBox(); self.cboDeleteBranch.setEditable(True)
        self.chkConfirmDelete = QCheckBox("Confirmar")
        self.btnDeleteBranch = QPushButton("Eliminar rama local (global)")
        hd = QHBoxLayout(); hd.addWidget(QLabel("Eliminar:")); hd.addWidget(self.cboDeleteBranch, 1); hd.addWidget(self.chkConfirmDelete); hd.addWidget(self.btnDeleteBranch)
        top.addLayout(hd, row, 0, 1, 4); row += 1

        self.txtVersion = QLineEdit(); self.txtVersion.setPlaceholderText("3.00.17")
        self.chkQA = QCheckBox("Crear *_QA"); self.btnRunCreateVersion = QPushButton("Crear ramas de versión (local, global)")
        hv = QHBoxLayout(); hv.addWidget(QLabel("Versión:")); hv.addWidget(self.txtVersion, 1); hv.addWidget(self.chkQA); hv.addWidget(self.btnRunCreateVersion)
        top.addLayout(hv, row, 0, 1, 4); row += 1

        self.btnRefresh = QPushButton("Refrescar vista"); self.btnReconcile = QPushButton("Reconciliar con Git (solo local)")
        hr = QHBoxLayout(); hr.addWidget(self.btnRefresh); hr.addStretch(); hr.addWidget(self.btnReconcile)
        top.addLayout(hr, row, 0, 1, 4); row += 1

        gr = QGroupBox("Resumen de ramas (cache/local)")
        grl = QVBoxLayout(gr)
        self.tree = QTreeWidget(); self.tree.setHeaderLabels(["Módulo", "Rama", "Estado", "Actual"])
        self.tree.setRootIsDecorated(False); self.tree.setAlternatingRowColors(True)
        grl.addWidget(self.tree, 1)
        top.addWidget(gr, row, 0, 1, 4); row += 1

        root.addLayout(top)
        self.log = QTextEdit(); self.log.setReadOnly(True); self.log.setLineWrapMode(QTextEdit.NoWrap)
        root.addWidget(self.log, 1)
        self.logger = Logger(); self.logger.line.connect(self.log.append)

    def _wire_events(self):
        self.cboProject.currentTextChanged.connect(self._on_project_changed)
        self.btnRefresh.clicked.connect(self._post_project_change)
        self.btnReconcile.clicked.connect(self._do_reconcile)
        self.btnSwitch.clicked.connect(self._do_switch)
        self.btnCreateLocal.clicked.connect(self._do_create_local)
        self.btnPushBranch.clicked.connect(self._do_push_branch)
        self.btnDeleteBranch.clicked.connect(self._do_delete_branch)
        self.btnRunCreateVersion.clicked.connect(self._do_create_version)
        self.btnRefresh.clicked.connect(self._post_project_change)


    def _load_projects_flat(self):
        self._dbg("load_projects_flat: start")
        try:
            self.cboProject.blockSignals(True)
            self.cboProject.clear()
            count = 0
            if getattr(self.cfg, "groups", None):
                seen=set()
                for g in self.cfg.groups:
                    for p in (g.projects or []):
                        if p.key not in seen:
                            seen.add(p.key); self.cboProject.addItem(p.key, userData=g.key); count += 1
            else:
                for p in (self.cfg.projects or []):
                    self.cboProject.addItem(p.key, userData=None); count += 1
            self._dbg(f"load_projects_flat: added {count} projects")
        finally:
            self.cboProject.blockSignals(False)
        QTimer.singleShot(0, self._post_project_change)

    def _current_keys(self) -> tuple[Optional[str], Optional[str]]:
        pkey = self.cboProject.currentText().strip() or None
        idx = self.cboProject.currentIndex()
        gkey = self.cboProject.itemData(idx) if idx >= 0 else None
        return gkey, pkey

    def _on_project_changed(self, _):
        self._dbg("on_project_changed")
        self._post_project_change()

    def _post_init(self):
        self._dbg("post_init: start")
        self._post_project_change()
        self._dbg("post_init: end")

    def _get_project_obj(self, gkey: str | None, pkey: str | None):
        # Busca el proyecto por claves (soporta cfg.groups y cfg.projects)
        if getattr(self.cfg, "groups", None):
            for g in self.cfg.groups:
                if gkey and getattr(g, "key", None) != gkey:
                    continue
                for p in (getattr(g, "projects", None) or []):
                    if getattr(p, "key", None) == pkey:
                        return p
        for p in (getattr(self.cfg, "projects", None) or []):
            if getattr(p, "key", None) == pkey:
                return p
        return None

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
                grp = None
                for g in (getattr(self.cfg, "groups", None) or []):
                    if getattr(g, "key", None) == gkey:
                        grp = g
                        break

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
                    self._dbg("post_project_change: end")
                    return
            except Exception as e:
                self._dbg(f"post_project_change: WARN groups.repos→HERR_REPO: {e}")

            # Si llegaste aquí, no hay workspace para gkey; aún así refresca UI
            self._refresh_history()
            self._refresh_summary()
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
            self.cboHistorySwitch.clear(); self.cboDeleteBranch.clear()
            gkey, pkey = self._current_keys()
            hist = STATE.get_history(gkey, pkey) or []
            for br in hist[:50]:
                self.cboHistorySwitch.addItem(br)
                self.cboDeleteBranch.addItem(br)
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
            for name, br, path in items:
                locals_, remotes_ = STATE.get_presence(gkey, pkey, name)
                current = STATE.get_current(gkey, pkey, name) or br or "?"
                branches = sorted(set(locals_) | set(remotes_) | ({current} if current else set()))
                if not branches:
                    branches = [current]

                for b in branches:
                    estado = "origin" if b in remotes_ else ("local" if b in locals_ else "¿?")
                    curflag = "Sí" if b == current else ""
                    it = QtWidgets.QTreeWidgetItem([name, b, estado, curflag])
                    self.tree.addTopLevelItem(it)

            self.tree.resizeColumnToContents(0)
            self.tree.resizeColumnToContents(1)
            self.tree.resizeColumnToContents(2)
            self._dbg(f"_refresh_summary: {self.tree.topLevelItemCount()} rows")
        except Exception as e:
            self._dbg(f"_refresh_summary: ERROR {e}")
        finally:
            if _is_valid_qobj(self.tree):
                self.tree.setUpdatesEnabled(True)

    # en buildtool/views/git_view.py (dentro de GitView)
    def _init_thread_store(self):
        if not hasattr(self, "_live_threads"):
            self._live_threads = set()

    def _start_task(self, title, fn, done_cb=None, *args, **kwargs):
        self._dbg(f"task start: {title}")
        th, worker = run_in_thread(fn, *args, **kwargs)

        # Si quieres conservar un callback de "done":
        self._pending_done_cb = done_cb

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
            self._dbg(f"task end (slot): ok={ok}")
            # Toca UI sólo aquí (hilo principal):
            self._refresh_history()
            self._refresh_summary()
            # Si tu _start_task quiere permitir callbacks externos:
            cb = getattr(self, "_pending_done_cb", None)
            if cb:
                try:
                    cb(ok)
                finally:
                    self._pending_done_cb = None
        except Exception as e:
            errguard.log(f"_on_task_finish error: {e}", level=40)


    def _set_task_buttons_enabled(self, enabled: bool):
        for btn in (self.btnCreateLocal, self.btnPushBranch, self.btnDeleteBranch, self.btnSwitch):
            try: btn.setEnabled(enabled)
            except Exception: pass          


    @safe_slot
    def _do_switch(self):
        branch = self.cboHistorySwitch.currentText().strip()
        if not branch:
            self._dbg("_do_switch: empty"); return
        gkey, pkey = self._current_keys()
        def _after():
            for name, _, _ in discover_status_fast(self.cfg, gkey, pkey):
                STATE.set_current(gkey, pkey, name, branch)
            STATE.add_history(gkey, pkey, branch)
        self._start_task(f"Switch a {branch} (global)",
            lambda cfg, gk, pk, br, emit=self.logger.line.emit: switch_branch(cfg, gk, pk, br, emit, only_modules=None),
            _after, self.cfg, gkey, pkey, branch)

    @safe_slot
    def _do_create_local(self):
        gkey, pkey = self._current_keys()
        name = self.txtNewBranch.text().strip()
        if not name:
            self._dbg("_do_create_local: empty"); return
        
        def task(cfg, gk, pk, br, emit=self.logger.line.emit):
            emit(f"[task] Crear rama local '{br}' (global)")
            ok = create_branches_local(cfg, gk, pk, br, emit=emit)
            emit("[task] DONE" if ok else "[task] DONE with errors")
            return ok

        self._start_task(f"Crear rama local {name} (global)", task, None, self.cfg, gkey, pkey, name)


    @safe_slot
    def _do_push_branch(self):
        nb = self.txtNewBranch.text().strip() or self.cboHistorySwitch.currentText().strip()
        if not nb:
            self._dbg("_do_push_branch: empty"); return
        gkey, pkey = self._current_keys()
        def _after():
            for name, _, _ in discover_status_fast(self.cfg, gkey, pkey):
                STATE.add_remote(gkey, pkey, name, nb)
        self._start_task(f"Push rama {nb} (global)",
            lambda cfg, gk, pk, name, emit=self.logger.line.emit: push_branch(cfg, gk, pk, name, emit, only_modules=None),
            _after, self.cfg, gkey, pkey, nb)

    @safe_slot
    def _do_delete_branch(self):
        nb = self.cboDeleteBranch.currentText().strip()
        if not nb:
            self._dbg("_do_delete_branch: empty"); return
        if not self.chkConfirmDelete.isChecked():
            self._dbg("_do_delete_branch: confirmar"); return
        gkey, pkey = self._current_keys()
        def _after():
            for name, _, _ in discover_status_fast(self.cfg, gkey, pkey):
                STATE.remove_local(gkey, pkey, name, nb)
        self._start_task(f"Eliminar rama local {nb} (global)",
            lambda cfg, gk, pk, name, confirm, emit=self.logger.line.emit: delete_local_branch_by_name(cfg, gk, pk, name, confirm, emit, only_modules=None),
            _after, self.cfg, gkey, pkey, nb, True)

    @safe_slot
    def _do_create_version(self):
        ver = self.txtVersion.text().strip()
        if not ver:
            self._dbg("_do_create_version: empty"); return
        create_qa = self.chkQA.isChecked()
        gkey, pkey = self._current_keys()
        def _after():
            for name, _, _ in discover_status_fast(self.cfg, gkey, pkey):
                STATE.add_local(gkey, pkey, name, ver)
                STATE.set_current(gkey, pkey, name, ver)
                if create_qa:
                    qa = f"{ver}_QA"
                    STATE.add_local(gkey, pkey, name, qa)
            STATE.add_history(gkey, pkey, ver)
            if create_qa:
                STATE.add_history(gkey, pkey, f"{ver}_QA")
        self._start_task(f"Crear ramas versión {ver} (global)",
            lambda cfg, gk, pk, v, qa, emit=self.logger.line.emit: create_version_branches(cfg, gk, pk, v, qa, {}, [], emit, only_modules=None),
            _after, self.cfg, gkey, pkey, ver, create_qa)

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
        self._start_task("Reconciliar con Git (local)", reconcile_task, None, self.cfg, gkey, pkey)
