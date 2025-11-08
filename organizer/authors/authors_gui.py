#!/usr/bin/env python3
"""
authors_gui.py — reine GUI, nutzt vorhandene Logik aus authors_network und authors_search.

Features:
  • CSV laden (Edges: src/tgt[, weight] | Mentions: pdf_file/canonical)
  • Darstellung steuern: Layout, Bipartite-Gap, Mindestgewicht, Labels, Richtung,
    Pfeilgröße, High-Contrast
  • Suche im Labeltext
  • Optional: „Ordner wählen & analysieren“ via authors_search.run_on_folder()
  • „HTML öffnen“ (authors_report.html o. ä. im Session-Ordner)
  • QSettings merkt letzte CSV/Session

Abhängigkeiten:
  pip install PyQt5 pandas

Start:
  python authors_gui.py
"""
from __future__ import annotations

import os
import sys
import math
import webbrowser
from typing import Dict, Tuple, Optional, List

import pandas as pd
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import Qt

# -------- Import: keine Logik duplizieren --------
try:
    from authors_network import (
        GraphCanvas,
        make_scene,
        compute_layout,
        build_bipartite_graph,
        build_graph_edges,
        resolve_columns,
        Theme,
    )
except Exception as e:
    raise ImportError(f"authors_network.py fehlt oder inkompatibel: {e}")

try:
    from authors_search import run_on_folder  # optional
except Exception:
    run_on_folder = None

try:
    from authors_utils import log_info, log_warn
except Exception:
    def log_info(m: str) -> None: print(f"[INFO] {m}")
    def log_warn(m: str) -> None: print(f"[WARN] {m}")

# ----------------------- Helpers -----------------------

def debug(msg: str) -> None:
    print(f"[DEBUG] {msg}", flush=True)

def read_csv_auto(path: str) -> pd.DataFrame:
    for sep in (';', ',', None):
        try:
            return pd.read_csv(path, sep=sep, engine='python')
        except Exception:
            continue
    raise ValueError(f"CSV konnte nicht gelesen werden: {path}")

def detect_format(df: pd.DataFrame) -> Tuple[str, dict]:
    """('edges'|'mentions', mapping). Minimaler Erkenner ohne Duplikat-Logik."""
    low = {c.lower(): c for c in df.columns}
    # Mentions
    doc_c = ["pdf_file", "pdf", "document", "source", "file", "filename", "doc"]
    work_c = ["label", "canonical", "work", "title", "chronik", "edition", "name", "match", "normalized"]
    doc = next((low[c] for c in doc_c if c in low), None)
    work = next((low[c] for c in work_c if c in low), None)
    if doc and work:
        return "mentions", {"doc": doc, "work": work}
    # Edges
    src_c = ["src", "source", "from", "a"]; tgt_c = ["tgt", "target", "to", "b"]
    src = next((low[c] for c in src_c if c in low), None)
    tgt = next((low[c] for c in tgt_c if c in low), None)
    if src and tgt:
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        w = next((low[c] for c in ["weighted", "total", "weight", "w", "count", "size"] if c in low), None)
        if not w and num_cols:
            w = num_cols[0]
        return "edges", {"src": src, "tgt": tgt, "w": w}
    raise KeyError(f"CSV-Format unbekannt. Spalten: {list(df.columns)}")

# ----------------------- Controls -----------------------

class ControlPanel(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        f = QtWidgets.QFormLayout(self); f.setLabelAlignment(Qt.AlignLeft)

        # Analyse
        self.btn_pick = QtWidgets.QPushButton("Ordner wählen & analysieren")
        self.lbl_session = QtWidgets.QLabel("Session: —")
        self.btn_open_html = QtWidgets.QPushButton("HTML öffnen")

        # CSV
        self.btn_open_csv = QtWidgets.QPushButton("CSV laden")
        self.combo_weight = QtWidgets.QComboBox(); self.combo_weight.setVisible(False)

        # Darstellung
        self.combo_layout = QtWidgets.QComboBox(); self.combo_layout.addItems(["forceatlas2","spring","kamada_kawai","bipartite"])
        self.spin_gap = QtWidgets.QDoubleSpinBox(); self.spin_gap.setRange(1.0, 10.0); self.spin_gap.setSingleStep(0.5); self.spin_gap.setValue(4.0)
        self.spin_minw = QtWidgets.QSpinBox(); self.spin_minw.setRange(1, 10**9); self.spin_minw.setValue(1)
        self.chk_labels = QtWidgets.QCheckBox("Labels anzeigen"); self.chk_labels.setChecked(True)
        self.combo_dir = QtWidgets.QComboBox(); self.combo_dir.addItems(["zitierend → zitierte", "zitierte → zitierend"])
        self.spin_arrow = QtWidgets.QDoubleSpinBox(); self.spin_arrow.setRange(0.8, 2.5); self.spin_arrow.setSingleStep(0.1); self.spin_arrow.setValue(1.6)
        self.chk_contrast = QtWidgets.QCheckBox("Pfeile hoher Kontrast"); self.chk_contrast.setChecked(True)

        # Suche
        self.edit_search = QtWidgets.QLineEdit(); self.edit_search.setPlaceholderText("Suche in Labels…")
        self.btn_search = QtWidgets.QPushButton("Suchen")
        self.btn_reset = QtWidgets.QPushButton("Reset Ansicht")

        # Layout
        f.addRow(self.btn_pick); f.addRow("Session", self.lbl_session); f.addRow(self.btn_open_html)
        f.addRow(self.btn_open_csv); f.addRow("Gewicht-Spalte (Edges)", self.combo_weight)
        f.addRow("Layout", self.combo_layout); f.addRow("Bipartite-Gap", self.spin_gap)
        f.addRow("Min. Gewicht", self.spin_minw); f.addRow("Labels", self.chk_labels)
        f.addRow("Richtung", self.combo_dir); f.addRow("Pfeilgröße", self.spin_arrow); f.addRow(self.chk_contrast)
        h = QtWidgets.QHBoxLayout(); h.addWidget(self.edit_search, 1); h.addWidget(self.btn_search); f.addRow(h)
        f.addRow(self.btn_reset)

# ----------------------- Tab / Controller -----------------------

class NetworkTab(QtWidgets.QWidget):
    REBUILD_DELAY_MS = 60

    def __init__(self):
        super().__init__()
        self.view = GraphCanvas()
        self.panel = ControlPanel()
        split = QtWidgets.QSplitter(Qt.Horizontal); split.addWidget(self.panel); split.addWidget(self.view); split.setStretchFactor(1, 1)
        lay = QtWidgets.QHBoxLayout(self); lay.addWidget(split)

        # Zustand
        self._csv_path: Optional[str] = None
        self._session_dir: Optional[str] = None
        self.df: Optional[pd.DataFrame] = None
        self._kind: str = "edges"
        self.G = None
        self.positions: Dict[str, Tuple[float, float]] = {}

        # Timer: WICHTIG → QtCore.QTimer (nicht QtWidgets.QTimer)
        self._timer = QtCore.QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(self._rebuild_scene)

        self._wire()
        self._auto_load_last()

    # ---- Settings ----
    def _settings(self) -> QtCore.QSettings: return QtCore.QSettings("authors_gui", "state")
    def _save_last(self) -> None:
        s = self._settings()
        if self._csv_path: s.setValue("last_csv", self._csv_path)
        if self._session_dir: s.setValue("last_session", self._session_dir)
    def _auto_load_last(self) -> None:
        s = self._settings()
        p_csv = s.value("last_csv", "", type=str)
        if p_csv and os.path.isfile(p_csv):
            try: self.load_csv(p_csv)
            except Exception as e: debug(f"Auto-Laden CSV fehlschlug: {e}")
        p_sess = s.value("last_session", "", type=str)
        if p_sess and os.path.isdir(p_sess):
            self._session_dir = p_sess; self.panel.lbl_session.setText(p_sess)

    # ---- Wiring ----
    def _wire(self) -> None:
        p = self.panel
        p.btn_open_csv.clicked.connect(self._choose_csv)
        p.btn_open_html.clicked.connect(self._open_html)
        p.btn_search.clicked.connect(self._do_search)
        p.btn_reset.clicked.connect(self._reset)
        p.combo_layout.currentTextChanged.connect(self._schedule)
        p.combo_weight.currentIndexChanged.connect(self._schedule)
        p.spin_gap.valueChanged.connect(self._schedule)
        p.spin_minw.valueChanged.connect(self._schedule)
        p.chk_labels.toggled.connect(self._schedule)
        p.combo_dir.currentIndexChanged.connect(self._schedule)
        p.spin_arrow.valueChanged.connect(self._schedule)
        p.chk_contrast.toggled.connect(self._schedule)
        if run_on_folder:
            p.btn_pick.clicked.connect(self._pick_and_run)
        else:
            p.btn_pick.setEnabled(False); p.btn_pick.setToolTip("authors_search.run_on_folder nicht verfügbar")

        QtWidgets.QShortcut(QtGui.QKeySequence("F"), self, activated=self._fit)
        QtWidgets.QShortcut(QtGui.QKeySequence("Ctrl+F"), self, activated=lambda: p.edit_search.setFocus())

    def _schedule(self) -> None:
        self._timer.start(self.REBUILD_DELAY_MS)

    # ---- CSV laden / erkennen ----
    def _choose_csv(self) -> None:
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "CSV wählen", os.getcwd(), "CSV (*.csv);;Alle Dateien (*)")
        if path:
            try: self.load_csv(path)
            except Exception as e: QtWidgets.QMessageBox.critical(self, "Fehler", str(e))

    def load_csv(self, path: str) -> None:
        self.df = read_csv_auto(path)
        kind, mapping = detect_format(self.df); self._kind = kind
        if kind == "mentions":
            doc, work = mapping["doc"], mapping["work"]
            # Verwende vorhandene API
            self.G = build_bipartite_graph(self.df[[doc, work]].dropna(), doc, work)
            self.panel.combo_layout.setEnabled(True); self.panel.spin_gap.setEnabled(True); self.panel.combo_weight.setVisible(False)
            if self.panel.combo_layout.currentText() == "forceatlas2":
                self.panel.combo_layout.setCurrentText("bipartite")
        else:
            src, tgt, w = mapping["src"], mapping["tgt"], mapping.get("w")
            num_cols = [c for c in self.df.columns if pd.api.types.is_numeric_dtype(self.df[c])]
            self.panel.combo_weight.clear()
            if num_cols:
                self.panel.combo_weight.addItems(num_cols); self.panel.combo_weight.setVisible(True)
                if w and w in num_cols: self.panel.combo_weight.setCurrentText(w)
            else:
                self.panel.combo_weight.setVisible(False)
            self.G = build_graph_edges(self.df, src, tgt, self.panel.combo_weight.currentText() or w)
            self.panel.combo_layout.setCurrentText("forceatlas2"); self.panel.spin_gap.setEnabled(False)

        self.positions = compute_layout(self.G, self.panel.combo_layout.currentText(), bip_gap=float(self.panel.spin_gap.value()))
        self._csv_path = path; self._save_last()
        self._rebuild_scene()

    # ---- Szene neu zeichnen ----
    def _rebuild_scene(self) -> None:
        if self.G is None:
            return
        if self._kind == "edges" and self.df is not None:
            kind, mapping = detect_format(self.df)
            self.G = build_graph_edges(self.df, mapping["src"], mapping["tgt"], self.panel.combo_weight.currentText() or mapping.get("w"))
        self.positions = compute_layout(self.G, self.panel.combo_layout.currentText(), bip_gap=float(self.panel.spin_gap.value()))
        scene = make_scene(
            self.G,
            self.positions,
            int(self.panel.spin_minw.value()),
            bool(self.panel.chk_labels.isChecked()),
            Theme(),
            "citing" if self.panel.combo_dir.currentIndex() == 0 else "cited",
            float(self.panel.spin_arrow.value()),
            bool(self.panel.chk_contrast.isChecked()),
        )
        self.view.set_graph_scene(scene)

    # ---- Pipeline-Start ----
    def _pick_and_run(self) -> None:
        base = QtWidgets.QFileDialog.getExistingDirectory(self, "PDF-Ordner wählen", os.getcwd())
        if not base: return
        try:
            log_info(f"Autor↔Autor-Sucher startet… Ordner: {base}")
            session_dir = run_on_folder(base)  # type: ignore[arg-type]
            log_info(f"Session abgeschlossen: {session_dir}")
            self._session_dir = session_dir; self.panel.lbl_session.setText(session_dir)
            # Bevorzugt laden
            for name in ("authors_edges.csv", "authors_nodes.csv"):
                cand = os.path.join(session_dir, name)
                if os.path.isfile(cand):
                    self.load_csv(cand); break
            self._save_last()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Analyse fehlgeschlagen", str(e))

    # ---- HTML öffnen ----
    def _open_html(self) -> None:
        for base in [self._session_dir, os.path.dirname(self._csv_path) if self._csv_path else None]:
            if not base: continue
            for name in ("authors_report.html", "report.html", "index.html"):
                cand = os.path.join(base, name)
                if os.path.isfile(cand):
                    webbrowser.open(f"file://{cand}"); return
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "HTML wählen", os.getcwd(), "HTML (*.html);;Alle Dateien (*)")
        if path: webbrowser.open(f"file://{path}")

    # ---- UI-Kommandos ----
    def _fit(self) -> None:
        if self.view.scene():
            try: self.view.fitInView(self.view.scene().itemsBoundingRect(), Qt.KeepAspectRatio)
            except Exception: pass

    def _reset(self) -> None:
        self.panel.spin_minw.setValue(1); self.panel.chk_labels.setChecked(True)
        self.panel.combo_dir.setCurrentIndex(0); self.panel.spin_arrow.setValue(1.6); self.panel.chk_contrast.setChecked(True)
        if self._kind == "mentions":
            self.panel.combo_layout.setCurrentText("bipartite"); self.panel.spin_gap.setValue(4.0)
        else:
            self.panel.combo_layout.setCurrentText("forceatlas2")
        self.panel.edit_search.clear(); self._fit(); self._rebuild_scene()

    def _do_search(self) -> None:
        term = self.panel.edit_search.text()
        hits = self.view.highlight(term, self.panel.chk_labels.isChecked())
        QtWidgets.QToolTip.showText(QtGui.QCursor.pos(), f"Treffer: {hits}")

# ----------------------- Main Window -----------------------

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Autoren-Referenznetz — GUI")
        self.resize(1300, 850)
        tabs = QtWidgets.QTabWidget()
        tabs.addTab(NetworkTab(), "Netzwerk")
        self.setCentralWidget(tabs)

# ----------------------- Entry -----------------------

def main() -> None:
    debug("Starte GUI …")
    try:
        QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling, True)
        QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_UseHighDpiPixmaps, True)
    except Exception:
        pass
    app = QtWidgets.QApplication(sys.argv)
    # Basales Dark-QSS (Scene-Farben kommen aus Theme in authors_network)
    app.setStyleSheet("""
        QWidget { color:#e6edf6; background:#0b0f14; font-size:14px; }
        QPushButton, QComboBox, QSpinBox, QDoubleSpinBox, QLineEdit {
            background:#0f1720; border:1px solid #223041; padding:8px 12px; border-radius:7px; min-height:34px;
        }
        QPushButton:hover { background:#152233; }
        QTabWidget::pane { border:1px solid #223041; }
        QHeaderView::section { background:#0f1720; padding:6px; border:0; }
        QToolTip { background:#111827; color:#e5e7eb; border:1px solid #374151; }
    """)
    win = MainWindow(); win.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()