#!/usr/bin/env python3
"""
chronik_gui.py — Qt-GUI für bipartites chroniken_library↔Werke-Netz

Start:
    $ python chronik_gui.py

Funktion:
    - CSV laden oder „chroniken_library-Search“ (dein Skript) importieren/ausführen
    - Zeichnet bipartites Netz: blaue Punkte = chroniken_library, Dreiecke = PDFs
    - Steuerung: Layout (bipartite/spring/kamada_kawai), Kantenschwelle, Labels, Suche
    - Merkt sich die zuletzt gewählte CSV via QSettings und lädt sie beim Start automatisch

Abhängigkeiten:
    pip install PyQt5 pandas networkx
"""

from __future__ import annotations

import os
import sys
import traceback
import importlib.util
from typing import Optional

import pandas as pd  # nur für Typangaben
import networkx as nx  # nur für Typangaben

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import Qt

from network import (
    load_mentions_csv, resolve_columns, build_bipartite_graph,
    compute_layout, GraphCanvas, make_scene
)


def debug(msg: str) -> None:
    print(f"[DEBUG] {msg}", flush=True)


class MainWindow(QtWidgets.QMainWindow):
    SETTINGS_KEY_LAST_CSV = "last_csv_path"

    def __init__(self):
        super().__init__()
        self.setWindowTitle("chroniken_library↔Werke — Interaktives Netz")
        self.resize(1280, 820)

        self.csv_path: Optional[str] = None
        self.df: Optional[pd.DataFrame] = None
        self.G: Optional[nx.Graph] = None
        self.layout_name: str = "bipartite"
        self.min_w: int = 1
        self.show_labels: bool = True

        self._build_ui()
        # Zuletzt verwendete CSV laden, falls vorhanden
        self._load_last_csv_if_available()

    # ---------- UI ----------

    def _build_ui(self) -> None:
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)

        self.canvas = GraphCanvas()

        # Controls
        self.btn_load_csv = QtWidgets.QPushButton("CSV laden…")
        self.btn_load_csv.clicked.connect(self.on_load_csv)

        self.btn_import_run = QtWidgets.QPushButton("chroniken_library-Search importieren/ausführen…")
        self.btn_import_run.clicked.connect(self.on_import_run)

        self.cmb_layout = QtWidgets.QComboBox()
        self.cmb_layout.addItems(["bipartite", "spring", "kamada_kawai"])
        self.cmb_layout.currentTextChanged.connect(self.on_layout_change)

        self.slider = QtWidgets.QSlider(Qt.Horizontal)
        self.slider.setMinimum(1)
        self.slider.setMaximum(10)
        self.slider.setValue(self.min_w)
        self.slider.valueChanged.connect(self.on_slider)

        self.lbl_thresh = QtWidgets.QLabel(f"Kantenschwelle: ≥ {self.min_w}")

        self.chk_labels = QtWidgets.QCheckBox("Labels")
        self.chk_labels.setChecked(self.show_labels)
        self.chk_labels.stateChanged.connect(self.on_labels_toggle)

        self.search_edit = QtWidgets.QLineEdit()
        self.search_edit.setPlaceholderText("Knoten suchen…")
        self.btn_search = QtWidgets.QPushButton("Hervorheben")
        self.btn_search.clicked.connect(self.on_search)

        # Layout
        left = QtWidgets.QVBoxLayout()
        left.setSpacing(10)
        left.addWidget(self.btn_load_csv)
        left.addWidget(self.btn_import_run)
        left.addSpacing(10)

        row1 = QtWidgets.QHBoxLayout()
        row1.addWidget(QtWidgets.QLabel("Layout:"))
        row1.addWidget(self.cmb_layout, 1)
        left.addLayout(row1)

        left.addWidget(self.lbl_thresh)
        left.addWidget(self.slider)
        left.addWidget(self.chk_labels)
        left.addSpacing(10)
        left.addWidget(QtWidgets.QLabel("Suche:"))
        left.addWidget(self.search_edit)
        left.addWidget(self.btn_search)
        left.addStretch(1)

        left_box = QtWidgets.QFrame()
        left_box.setLayout(left)
        left_box.setFixedWidth(300)
        left_box.setStyleSheet("""
            QFrame { background:#0f172a; }
            QLabel, QCheckBox { color:#e2e8f0; }
            QPushButton { background:#1e293b; color:#e2e8f0; border:1px solid #334155; padding:6px; border-radius:6px; }
            QPushButton:hover { background:#273449; }
            QComboBox, QLineEdit { background:#0b1320; color:#e2e8f0; border:1px solid #334155; padding:5px; border-radius:6px; }
            QSlider::groove:horizontal { height:6px; background:#1f2937; border-radius:3px; }
            QSlider::handle:horizontal { background:#3b82f6; width:14px; height:14px; margin:-4px 0; border-radius:7px; }
        """)

        main_layout = QtWidgets.QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.addWidget(left_box)
        main_layout.addWidget(self.canvas, 1)

        # Menü
        menu = self.menuBar()
        m_file = menu.addMenu("Datei")
        act_csv = m_file.addAction("CSV laden…")
        act_csv.triggered.connect(self.on_load_csv)
        m_file.addSeparator()
        act_quit = m_file.addAction("Beenden")
        act_quit.triggered.connect(self.close)

        self.setStyleSheet("QMainWindow { background:#0b1320; } QMenuBar, QMenu { color:#e2e8f0; background:#0f172a; }")

    # ---------- Persistenz ----------

    def _settings(self) -> QtCore.QSettings:
        # Nutzt Organization/Application aus main()
        return QtCore.QSettings()

    def _save_last_csv(self, path: str) -> None:
        s = self._settings()
        s.setValue(self.SETTINGS_KEY_LAST_CSV, path)
        s.sync()
        debug(f"Zuletzt verwendete CSV gespeichert: {path}")

    def _load_last_csv_if_available(self) -> None:
        s = self._settings()
        path = s.value(self.SETTINGS_KEY_LAST_CSV, type=str)
        if path and os.path.isfile(path):
            try:
                debug(f"Letzte CSV gefunden, lade automatisch: {path}")
                self.csv_path = path
                self.df = load_mentions_csv(path)
                self._rebuild()
            except Exception as ex:
                traceback.print_exc()
                QtWidgets.QMessageBox.warning(self, "Warnung",
                    f"Letzte CSV konnte nicht geladen werden:\n{ex}")
        elif path:
            debug(f"Gespeicherter CSV-Pfad existiert nicht mehr: {path}")

    # ---------- Daten/Graph ----------

    def _rebuild(self) -> None:
        if self.df is None:
            return
        try:
            doc_col, work_col = resolve_columns(self.df)
            self.G = build_bipartite_graph(self.df, doc_col, work_col)
            pos = compute_layout(self.G, self.layout_name)
            scene = make_scene(self.G, pos, self.min_w, self.show_labels)
            self.canvas.set_graph_scene(scene)
            title_suffix = f" — {os.path.basename(self.csv_path)}" if self.csv_path else ""
            self.setWindowTitle(f"chroniken_library↔Werke — Interaktives Netz{title_suffix}")
            debug(f"Neu gezeichnet: nodes={self.G.number_of_nodes()} edges={self.G.number_of_edges()} threshold={self.min_w}")
        except Exception as ex:
            traceback.print_exc()
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Netzaufbau fehlgeschlagen:\n{ex}")

    # ---------- Actions ----------

    def on_load_csv(self) -> None:
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Mentions-CSV öffnen", os.getcwd(), "CSV Dateien (*.csv)")
        if not path:
            return
        try:
            self.csv_path = path
            self.df = load_mentions_csv(path)
            self._rebuild()
            self._save_last_csv(path)
        except Exception as ex:
            traceback.print_exc()
            QtWidgets.QMessageBox.critical(self, "Fehler", f"CSV konnte nicht geladen werden:\n{ex}")

    def on_import_run(self) -> None:
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "chroniken_library-Search.py wählen", os.getcwd(), "Python (*.py)")
        if not path:
            return
        try:
            spec = importlib.util.spec_from_file_location("chroniken_search", path)
            if not spec or not spec.loader:
                raise ImportError("Import-Spezifikation fehlgeschlagen.")
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)  # import

            func = None
            for name in ("main", "run", "generate_mentions_csv"):
                if hasattr(mod, name) and callable(getattr(mod, name)):
                    func = getattr(mod, name)
                    break
            if func:
                debug(f"Rufe {func.__name__}() in {os.path.basename(path)} auf …")
                func()  # erwartet, dass CSV geschrieben wird
            else:
                QtWidgets.QMessageBox.information(self, "Hinweis",
                    "Keine ausführbare Funktion gefunden (erwartet: main()/run()/generate_mentions_csv()).\n"
                    "CSV bitte manuell laden.")

            guess = os.path.join(os.getcwd(), "chroniken_mentions.csv")
            if os.path.isfile(guess):
                self.csv_path = guess
                self.df = load_mentions_csv(guess)
                self._rebuild()
                self._save_last_csv(guess)
            else:
                self.on_load_csv()

        except Exception as ex:
            traceback.print_exc()
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Import/Ausführung fehlgeschlagen:\n{ex}")

    def on_layout_change(self, text: str) -> None:
        self.layout_name = text
        self._rebuild()

    def on_slider(self, value: int) -> None:
        self.min_w = int(value)
        self.lbl_thresh.setText(f"Kantenschwelle: ≥ {self.min_w}")
        self._rebuild()

    def on_labels_toggle(self, state: int) -> None:
        self.show_labels = state == Qt.Checked
        self._rebuild()

    def on_search(self) -> None:
        term = self.search_edit.text().strip()
        hits = self.canvas.highlight(term, self.show_labels)
        if term and hits == 0:
            QtWidgets.QToolTip.showText(self.mapToGlobal(self.search_edit.pos()), "Kein Treffer", self.search_edit)


def main() -> None:
    debug("Starte GUI …")
    app = QtWidgets.QApplication(sys.argv)
    # QSettings-Kontext setzen (wirkt für alle _settings()-Aufrufe)
    QtCore.QCoreApplication.setOrganizationName("chroniken_library")
    QtCore.QCoreApplication.setApplicationName("ChronikenWerkeGUI")
    win = MainWindow()
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()