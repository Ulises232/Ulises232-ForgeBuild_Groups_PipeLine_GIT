import unittest

IMPORT_ERROR = None
try:
    from PySide6.QtWidgets import QApplication
    PYSIDE_AVAILABLE = True
except ImportError as exc:  # pragma: no cover - entorno sin Qt/GL
    QApplication = None  # type: ignore
    IMPORT_ERROR = exc
    PYSIDE_AVAILABLE = False

if PYSIDE_AVAILABLE:  # pragma: no cover - solo se importa si hay Qt
    from buildtool.ui.multi_select import MultiSelectComboBox
else:  # pragma: no cover - evita fallos en entornos sin GL
    MultiSelectComboBox = None  # type: ignore


@unittest.skipUnless(PYSIDE_AVAILABLE, f"PySide6 no disponible: {IMPORT_ERROR}")
class MultiSelectFilterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._app = QApplication.instance() or QApplication([])

    def test_filter_and_selection_persistence(self) -> None:
        combo = MultiSelectComboBox()
        combo.enable_filter()
        combo.set_items(["Alpha", "Beta", "Gamma"])
        combo.set_checked_items(["Alpha"])

        combo.apply_filter("ga")
        self.assertEqual(combo.filter_text, "ga")
        self.assertIn("Alpha", combo.checked_items())

        combo.apply_filter("")
        self.assertEqual(combo.filter_text, "")
        self.assertIn("Alpha", combo.checked_items())


if __name__ == "__main__":
    unittest.main()
