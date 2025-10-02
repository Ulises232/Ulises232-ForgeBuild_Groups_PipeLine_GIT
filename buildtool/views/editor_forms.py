from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QListWidget,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from ..ui.icons import get_icon


class SprintFormWidget(QWidget):
    """Standalone widget with the sprint editing controls."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        self.group_box = QGroupBox("Detalles del sprint")
        form = QFormLayout(self.group_box)
        form.setLabelAlignment(Qt.AlignRight)

        self.cboSprintGroup = QComboBox()
        form.addRow("Grupo", self.cboSprintGroup)

        branch_row = QHBoxLayout()
        self.txtSprintBranch = QLineEdit()
        self.txtSprintBranch.setReadOnly(True)
        branch_row.addWidget(self.txtSprintBranch, 1)
        self.btnPickBranch = QPushButton("Seleccionar rama")
        self.btnPickBranch.setIcon(get_icon("branch"))
        branch_row.addWidget(self.btnPickBranch)
        form.addRow("Rama base", branch_row)

        qa_row = QHBoxLayout()
        self.txtSprintQABranch = QLineEdit()
        self.txtSprintQABranch.setReadOnly(True)
        qa_row.addWidget(self.txtSprintQABranch, 1)
        self.btnPickQABranch = QPushButton("Seleccionar rama QA")
        self.btnPickQABranch.setIcon(get_icon("branch"))
        qa_row.addWidget(self.btnPickQABranch)
        form.addRow("Rama QA", qa_row)

        self.txtSprintName = QLineEdit()
        form.addRow("Nombre", self.txtSprintName)

        self.txtSprintVersion = QLineEdit()
        form.addRow("Versión", self.txtSprintVersion)

        self.cboCompany = QComboBox()
        form.addRow("Empresa", self.cboCompany)

        self.lblSprintSequence = QLabel("Sin empresa")
        form.addRow("Orden empresa", self.lblSprintSequence)

        self.cboSprintLead = QComboBox()
        form.addRow("Responsable", self.cboSprintLead)

        self.cboSprintQA = QComboBox()
        form.addRow("Responsable QA", self.cboSprintQA)

        self.chkSprintClosed = QCheckBox("Sprint finalizado")
        form.addRow("Estado", self.chkSprintClosed)

        self.lblSprintMeta = QLabel("")
        self.lblSprintMeta.setWordWrap(True)
        form.addRow("", self.lblSprintMeta)

        button_row = QHBoxLayout()
        button_row.addStretch(1)
        self.btnSprintDelete = QPushButton("Eliminar")
        self.btnSprintDelete.setIcon(get_icon("delete"))
        button_row.addWidget(self.btnSprintDelete)
        self.btnSprintCancel = QPushButton("Cancelar")
        button_row.addWidget(self.btnSprintCancel)
        self.btnSprintSave = QPushButton("Guardar")
        self.btnSprintSave.setIcon(get_icon("save"))
        button_row.addWidget(self.btnSprintSave)
        form.addRow("", button_row)

        layout.addWidget(self.group_box)

        self.script_box = QGroupBox("Script SQL asociado")
        script_layout = QVBoxLayout(self.script_box)
        script_layout.setContentsMargins(12, 12, 12, 12)
        script_layout.setSpacing(6)

        toolbar = QHBoxLayout()
        toolbar.setContentsMargins(0, 0, 0, 0)
        toolbar.setSpacing(6)

        self.lblCardScriptInfo = QLabel("Sin script adjunto")
        toolbar.addWidget(self.lblCardScriptInfo, 1)

        self.btnCardLoadScript = QPushButton("Cargar archivo…")
        self.btnCardLoadScript.setIcon(get_icon("cloud-upload"))
        toolbar.addWidget(self.btnCardLoadScript)

        self.btnCardDeleteScript = QPushButton("Quitar script")
        self.btnCardDeleteScript.setIcon(get_icon("delete"))
        toolbar.addWidget(self.btnCardDeleteScript)

        script_layout.addLayout(toolbar)

        self.txtCardScript = QPlainTextEdit()
        self.txtCardScript.setPlaceholderText("Escribe o pega el script SQL de la tarjeta…")
        self.txtCardScript.setTabChangesFocus(True)
        self.txtCardScript.setMinimumHeight(160)
        script_layout.addWidget(self.txtCardScript)

        layout.addWidget(self.script_box)

        self.pending_box = QGroupBox("Tarjetas pendientes sin sprint")
        pending_layout = QVBoxLayout(self.pending_box)
        self.lstUnassignedCards = QListWidget()
        self.lstUnassignedCards.setSelectionMode(QAbstractItemView.MultiSelection)
        pending_layout.addWidget(self.lstUnassignedCards)
        self.btnAssignCards = QPushButton("Asignar tarjetas seleccionadas")
        self.btnAssignCards.setIcon(get_icon("check"))
        pending_layout.addWidget(self.btnAssignCards)
        layout.addWidget(self.pending_box)


class CardFormWidget(QWidget):
    """Standalone widget with the card editing controls."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        self.group_box = QGroupBox("Detalles de la tarjeta")
        form = QFormLayout(self.group_box)
        form.setLabelAlignment(Qt.AlignRight)

        self.lblCardSprint = QLabel("-")
        form.addRow("Sprint", self.lblCardSprint)

        self.cboCardSprint = QComboBox()
        form.addRow("Mover a sprint", self.cboCardSprint)

        self.cboCardGroup = QComboBox()
        form.addRow("Grupo", self.cboCardGroup)

        self.cboCardCompany = QComboBox()
        form.addRow("Empresa", self.cboCardCompany)

        self.cboCardIncidence = QComboBox()
        form.addRow("Tipo de incidencia", self.cboCardIncidence)

        self.lblCardStatus = QLabel("Pendiente")
        form.addRow("Estado", self.lblCardStatus)

        self.txtCardTicket = QLineEdit()
        form.addRow("Ticket", self.txtCardTicket)

        self.txtCardTitle = QLineEdit()
        form.addRow("Título", self.txtCardTitle)

        branch_row = QHBoxLayout()
        self.lblCardPrefix = QLabel("")
        branch_row.addWidget(self.lblCardPrefix)
        self.txtCardBranch = QLineEdit()
        branch_row.addWidget(self.txtCardBranch, 1)
        self.lblCardBranchPreview = QLabel("")
        branch_row.addWidget(self.lblCardBranchPreview)
        form.addRow("Rama", branch_row)

        self.cboCardAssignee = QComboBox()
        form.addRow("Desarrollador", self.cboCardAssignee)

        self.cboCardQA = QComboBox()
        form.addRow("QA", self.cboCardQA)

        self.txtCardUnitUrl = QLineEdit()
        self.txtCardUnitUrl.setPlaceholderText("https://...")
        form.addRow("Link pruebas unitarias", self.txtCardUnitUrl)

        self.txtCardQAUrl = QLineEdit()
        self.txtCardQAUrl.setPlaceholderText("https://...")
        form.addRow("Link QA", self.txtCardQAUrl)

        self.lblCardChecks = QLabel("Pruebas: pendiente | QA: pendiente")
        form.addRow("Checks", self.lblCardChecks)

        status_row = QHBoxLayout()
        self.lblCardLocal = QLabel("Local: -")
        status_row.addWidget(self.lblCardLocal)
        self.lblCardOrigin = QLabel("Origen: -")
        status_row.addWidget(self.lblCardOrigin)
        self.lblCardCreator = QLabel("Creada por: -")
        status_row.addWidget(self.lblCardCreator)
        status_row.addStretch(1)
        form.addRow("Estado de rama", status_row)

        button_row = QHBoxLayout()
        self.btnCardDelete = QPushButton("Eliminar")
        self.btnCardDelete.setIcon(get_icon("delete"))
        button_row.addWidget(self.btnCardDelete)
        self.btnCardMarkUnit = QPushButton("Marcar pruebas unitarias")
        self.btnCardMarkUnit.setIcon(get_icon("build"))
        button_row.addWidget(self.btnCardMarkUnit)
        self.btnCardMarkQA = QPushButton("Marcar QA")
        self.btnCardMarkQA.setIcon(get_icon("log"))
        button_row.addWidget(self.btnCardMarkQA)
        self.btnCardCreateBranch = QPushButton("Crear rama")
        self.btnCardCreateBranch.setIcon(get_icon("branch"))
        button_row.addWidget(self.btnCardCreateBranch)
        button_row.addStretch(1)
        self.btnCardCancel = QPushButton("Cancelar")
        button_row.addWidget(self.btnCardCancel)
        self.btnCardSave = QPushButton("Guardar")
        self.btnCardSave.setIcon(get_icon("save"))
        button_row.addWidget(self.btnCardSave)
        form.addRow("", button_row)

        layout.addWidget(self.group_box)
