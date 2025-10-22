# FILE: db/overview/gui_main.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""GUI entrypoint – lädt Theme & startet MainWindow aus gui_widgets."""

from pathlib import Path
import sys
try:
    from PySide6.QtWidgets import QApplication
except Exception:
    from PySide2.QtWidgets import QApplication

from .gui_widgets import MainWindow

_FALLBACK_QSS = """
QWidget { background: #191c20; color: #e9edf1; }
QLineEdit, QTextEdit, QSpinBox, QComboBox, QTableView {
  background: #20262b; color: #e9edf1; border: 1px solid #2d343c; border-radius: 6px; padding: 6px;
}
"""

def launch_gui(db_path: Path, lib_dir: Path) -> None:
    app = QApplication(sys.argv)
    qss_path = Path(__file__).resolve().parent / "styles.qss"
    try:
        app.setStyleSheet(qss_path.read_text(encoding="utf-8"))
        print(f"[DEBUG][gui_main] Theme geladen: {qss_path}")
    except Exception as e:
        print(f"[WARN][gui_main] styles.qss nicht gefunden: {e} – Fallback")
        app.setStyleSheet(_FALLBACK_QSS)
    win = MainWindow(db_path=db_path, lib_dir=lib_dir)
    win.show()
    sys.exit(app.exec())