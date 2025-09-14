# buildtool/core/qt_silence.py
import os, warnings
try:
    from PySide6 import QtCore
except Exception:
    QtCore = None

_RULES = {
    "warn":  "*.debug=false\n*.info=false\n*.warning=true\n*.critical=true",
    "error": "*.debug=false\n*.info=false\n*.warning=false\n*.critical=true",
    "off":   "*.debug=false\n*.info=false\n*.warning=false\n*.critical=false",
}

def setup_qt_logging(level: str = "warn") -> None:
    rules = _RULES.get((level or "warn").lower(), _RULES["warn"])
    os.environ["QT_LOGGING_RULES"] = rules.replace("\n", ";")

    if QtCore and hasattr(QtCore, "QLoggingCategory"):
        QtCore.QLoggingCategory.setFilterRules(rules)

    # Silenciar warnings de Python
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", category=ResourceWarning)
