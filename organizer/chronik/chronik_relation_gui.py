#!/usr/bin/env python3
"""
chronik_gui.py
PySide6-GUI zur Analyse von Ähnlichkeiten zwischen Chroniken über Werke/PDFs.
- CSV laden, Spalten zuordnen, Gewichtung wählen, Analyse starten.
- Cosinus-Ähnlichkeiten, Top-Paare, Nachbarn je Chronik.
- Export: Paare-CSV, Similarity-Matrix-CSV, HTML-Report.
- UI: Dark/Glas-Style, merkt letzte CSV und Spalten in ~/.chronik_gui_config.json.
- Anzeige: PDF nur als Dateiname (optional ohne .pdf).

Abhängigkeiten:
    pip install PySide6 pandas numpy

Start:
    python chronik_gui.py
"""
from __future__ import annotations

import json
import math
import os
import sys
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from PySide6 import QtCore, QtGui, QtWidgets


# ---------------------- Debug ----------------------

def debug(msg: str) -> None:
    print(f"[DEBUG] {msg}", flush=True)


# ---------------------- Persistenz ----------------------

CONFIG_PATH = Path.home() / ".chronik_gui_config.json"


@dataclass
class AppConfig:
    last_csv: str = ""
    chronik_col: str = ""
    werk_col: str = ""
    intensity_col: str = ""

    def save(self) -> None:
        try:
            CONFIG_PATH.write_text(json.dumps(self.__dict__, ensure_ascii=False, indent=2))
            debug(f"Konfiguration gespeichert: {CONFIG_PATH}")
        except Exception as e:
            debug(f"Konfiguration konnte nicht gespeichert werden: {e}")

    @staticmethod
    def load() -> "AppConfig":
        try:
            if CONFIG_PATH.exists():
                data = json.loads(CONFIG_PATH.read_text())
                debug(f"Konfiguration geladen: {CONFIG_PATH}")
                return AppConfig(**{k: data.get(k, "") for k in AppConfig().__dict__.keys()})
        except Exception as e:
            debug(f"Konfiguration konnte nicht geladen werden: {e}")
        return AppConfig()


# ---------------------- CSV Utilities ----------------------

def read_csv_auto(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"CSV nicht gefunden: {path}")
    try:
        debug("Lese CSV mit sep=';' …")
        return pd.read_csv(path, sep=';', engine='python')
    except Exception as e:
        debug(f"sep=';' scheiterte: {e}. Versuche sep=',' …")
        try:
            return pd.read_csv(path, sep=',', engine='python')
        except Exception as e2:
            debug(f"sep=',' scheiterte: {e2}. Versuche Sniffer …")
            return pd.read_csv(path, sep=None, engine='python')


def guess_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = [c.lower() for c in df.columns]
    for cand in candidates:
        if cand.lower() in cols:
            idx = cols.index(cand.lower())
            return df.columns[idx]
    return None


# ---------------------- Mathe ----------------------

def safe_row_norms(mat: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    return norms


def cosine_similarity_from_counts(mat: np.ndarray) -> np.ndarray:
    # Kosinus-Ähnlichkeit ohne separate Vor-Normierung
    m = mat / safe_row_norms(mat)
    return m @ m.T


def tf_idf_weight(mat_counts: np.ndarray) -> np.ndarray:
    # tf: Zeilenweise Normierung auf Summe 1
    row_sums = mat_counts.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0.0] = 1.0
    tf = mat_counts / row_sums
    # idf: log((1+N)/(1+df)) + 1
    df = (mat_counts > 0).sum(axis=0, keepdims=True)
    n = mat_counts.shape[0]
    idf = np.log((1.0 + n) / (1.0 + df)) + 1.0
    return tf * idf


# ---------------------- Engine ----------------------

class ChronicleSimilarityEngine:
    @staticmethod
    def build_matrix(
        df: pd.DataFrame,
        chronik_col: str,
        werk_col: str,
        intensity_col: Optional[str],
        weighting: str,          # 'count' | 'intensity' | 'binary'
        use_tfidf: bool,
    ) -> Tuple[np.ndarray, List[str], List[str], pd.DataFrame]:
        if chronik_col not in df.columns or werk_col not in df.columns:
            raise ValueError("Chronik- oder Werkspalte fehlt.")

        cols = [chronik_col, werk_col]
        if intensity_col and intensity_col in df.columns:
            cols.append(intensity_col)
        work_df = df[cols].copy()
        work_df[chronik_col] = work_df[chronik_col].astype(str)
        work_df[werk_col] = work_df[werk_col].astype(str)

        if weighting == 'binary':
            agg = work_df.groupby([chronik_col, werk_col], as_index=False).size()
            agg['weight'] = 1.0
        elif weighting == 'intensity' and intensity_col and intensity_col in work_df.columns:
            work_df[intensity_col] = pd.to_numeric(work_df[intensity_col], errors='coerce').fillna(0.0)
            agg = work_df.groupby([chronik_col, werk_col], as_index=False)[intensity_col].sum()
            agg = agg.rename(columns={intensity_col: 'weight'})
        else:
            agg = work_df.groupby([chronik_col, werk_col], as_index=False).size()
            agg = agg.rename(columns={'size': 'weight'})

        chroniken = sorted(agg[chronik_col].unique().tolist())
        werke = sorted(agg[werk_col].unique().tolist())
        ci = {c: i for i, c in enumerate(chroniken)}
        wi = {w: i for i, w in enumerate(werke)}

        mat = np.zeros((len(chroniken), len(werke)), dtype=float)
        for _, row in agg.iterrows():
            mat[ci[row[chronik_col]], wi[row[werk_col]]] = float(row['weight'])

        if use_tfidf:
            mat = tf_idf_weight(mat)

        return mat, chroniken, werke, agg

    @staticmethod
    def pairs_from_similarity(
        mat: np.ndarray,
        chroniken: List[str],
        nonzero_mask: np.ndarray,
        top_k_per_row: int,
        min_shared: int,
        min_sim: float,
    ) -> Tuple[pd.DataFrame, np.ndarray]:
        S = cosine_similarity_from_counts(mat)
        n = S.shape[0]
        rows = []
        for i in range(n):
            sims = S[i, (i + 1):]
            idxs = np.argsort(-sims)  # absteigend
            for idx in idxs[:max(top_k_per_row, n)]:
                j = i + 1 + int(idx)
                sim = float(S[i, j])
                if sim < min_sim:
                    continue
                shared = int(np.logical_and(nonzero_mask[i], nonzero_mask[j]).sum())
                if shared < min_shared:
                    continue
                rows.append((chroniken[i], chroniken[j], sim, shared))
        df_pairs = pd.DataFrame(rows, columns=['chronik_a', 'chronik_b', 'similarity', 'shared_werke'])
        df_pairs = df_pairs.sort_values(['similarity', 'shared_werke'], ascending=[False, False], ignore_index=True)
        return df_pairs, S


# ---------------------- Anzeige-Helfer ----------------------

def basename_only(s: str, drop_ext: bool) -> str:
    base = os.path.basename(s)
    if drop_ext:
        root, ext = os.path.splitext(base)
        return root
    return base


def apply_display_names(df: pd.DataFrame, cols: List[str], drop_ext: bool) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].astype(str).map(lambda x: basename_only(x, drop_ext))
    return out


# ---------------------- Qt-Model ----------------------

class PandasModel(QtCore.QAbstractTableModel):
    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self._df = df.copy()

    def rowCount(self, parent=QtCore.QModelIndex()):
        return len(self._df)

    def columnCount(self, parent=QtCore.QModelIndex()):
        return 0 if self._df is None else len(self._df.columns)

    def data(self, index, role=QtCore.Qt.DisplayRole):
        if not index.isValid() or role not in (QtCore.Qt.DisplayRole, QtCore.Qt.ToolTipRole):
            return None
        val = self._df.iat[index.row(), index.column()]
        return "" if pd.isna(val) else str(val)

    def headerData(self, section, orientation, role=QtCore.Qt.DisplayRole):
        if role != QtCore.Qt.DisplayRole:
            return None
        if orientation == QtCore.Qt.Horizontal:
            return str(self._df.columns[section])
        return str(section)

    def set_dataframe(self, df: pd.DataFrame):
        self.beginResetModel()
        self._df = df.copy()
        self.endResetModel()


# ---------------------- GUI ----------------------

class ChronicleSimilarityDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Chronik-Ähnlichkeiten analysieren")
        self.resize(1180, 760)
        self._df: Optional[pd.DataFrame] = None
        self._chroniken: List[str] = []
        self._werke: List[str] = []
        self._mat: Optional[np.ndarray] = None
        self._sim_matrix: Optional[np.ndarray] = None
        self._config = AppConfig.load()

        self._build_ui()
        self._restore_last()

    # ---- UI ----

    def _build_ui(self) -> None:
        outer = QtWidgets.QVBoxLayout(self)

        # Glasiger Container
        container = QtWidgets.QFrame()
        container.setObjectName("glassContainer")
        container.setGraphicsEffect(self._drop_shadow(container))
        outer.addWidget(container, 1)

        lay = QtWidgets.QVBoxLayout(container)

        # Datei & Spalten
        file_lay = QtWidgets.QHBoxLayout()
        self.le_path = QtWidgets.QLineEdit()
        self.btn_browse = QtWidgets.QPushButton("CSV laden…")
        self.btn_browse.clicked.connect(self.on_browse)
        file_lay.addWidget(self.le_path, 1)
        file_lay.addWidget(self.btn_browse)
        lay.addLayout(file_lay)

        cols_lay = QtWidgets.QHBoxLayout()
        self.cb_chronik = QtWidgets.QComboBox()
        self.cb_werk = QtWidgets.QComboBox()
        self.cb_intensity = QtWidgets.QComboBox()
        self.cb_intensity.addItem("— keine —")
        cols_lay.addWidget(QtWidgets.QLabel("Chronik:"))
        cols_lay.addWidget(self.cb_chronik, 1)
        cols_lay.addWidget(QtWidgets.QLabel("Werk/PDF:"))
        cols_lay.addWidget(self.cb_werk, 1)
        cols_lay.addWidget(QtWidgets.QLabel("Intensität:"))
        cols_lay.addWidget(self.cb_intensity, 1)
        lay.addLayout(cols_lay)

        # Optionen
        opt_lay = QtWidgets.QHBoxLayout()
        self.cb_weight = QtWidgets.QComboBox()
        self.cb_weight.addItems(["count", "intensity", "binary"])
        self.chk_tfidf = QtWidgets.QCheckBox("TF-IDF")
        self.spin_min_shared = QtWidgets.QSpinBox()
        self.spin_min_shared.setRange(0, 10_000)
        self.spin_min_shared.setValue(1)
        self.dsb_min_sim = QtWidgets.QDoubleSpinBox()
        self.dsb_min_sim.setRange(0.0, 1.0)
        self.dsb_min_sim.setSingleStep(0.01)
        self.dsb_min_sim.setValue(0.2)
        self.spin_topk = QtWidgets.QSpinBox()
        self.spin_topk.setRange(1, 10_000)
        self.spin_topk.setValue(10)
        self.chk_show_basename = QtWidgets.QCheckBox("nur Dateiname")
        self.chk_show_basename.setChecked(True)
        self.chk_drop_ext = QtWidgets.QCheckBox("Endung entfernen")
        self.chk_drop_ext.setChecked(False)

        for w in [
            QtWidgets.QLabel("Gewichtung:"), self.cb_weight,
            self.chk_tfidf,
            QtWidgets.QLabel("min gemeinsame Werke:"), self.spin_min_shared,
            QtWidgets.QLabel("min Ähnlichkeit:"), self.dsb_min_sim,
            QtWidgets.QLabel("Top-K je Chronik:"), self.spin_topk,
            self.chk_show_basename, self.chk_drop_ext
        ]:
            opt_lay.addWidget(w)
        lay.addLayout(opt_lay)

        # Buttons
        btn_lay = QtWidgets.QHBoxLayout()
        self.btn_analyze = QtWidgets.QPushButton("Analyse")
        self.btn_export_pairs = QtWidgets.QPushButton("Paare exportieren")
        self.btn_export_matrix = QtWidgets.QPushButton("Matrix exportieren")
        self.btn_html_report = QtWidgets.QPushButton("HTML-Report")
        self.btn_analyze.clicked.connect(self.on_analyze)
        self.btn_export_pairs.clicked.connect(self.on_export_pairs)
        self.btn_export_matrix.clicked.connect(self.on_export_matrix)
        self.btn_html_report.clicked.connect(self.on_html_report)
        for b in [self.btn_analyze, self.btn_export_pairs, self.btn_export_matrix, self.btn_html_report]:
            btn_lay.addWidget(b)
        btn_lay.addStretch(1)
        lay.addLayout(btn_lay)

        # Tabs
        self.tabs = QtWidgets.QTabWidget()
        self.tabs.setObjectName("glassTabs")

        # Top-Paare
        w_pairs = QtWidgets.QWidget()
        lp = QtWidgets.QVBoxLayout(w_pairs)
        self.tbl_pairs = QtWidgets.QTableView()
        self.model_pairs = PandasModel(pd.DataFrame(columns=['chronik_a', 'chronik_b', 'similarity', 'shared_werke']))
        self.tbl_pairs.setModel(self.model_pairs)
        lp.addWidget(self.tbl_pairs)
        self.tabs.addTab(w_pairs, "Top-Paare")

        # Nachbarn je Chronik
        w_neighbors = QtWidgets.QWidget()
        ln = QtWidgets.QVBoxLayout(w_neighbors)
        top_n_lay = QtWidgets.QHBoxLayout()
        self.cb_chronik_pick = QtWidgets.QComboBox()
        self.spin_neighbors = QtWidgets.QSpinBox()
        self.spin_neighbors.setRange(1, 2000)
        self.spin_neighbors.setValue(10)
        self.btn_refresh_neighbors = QtWidgets.QPushButton("Nachbarn aktualisieren")
        self.btn_refresh_neighbors.clicked.connect(self.refresh_neighbors)
        for w in [QtWidgets.QLabel("Chronik:"), self.cb_chronik_pick,
                  QtWidgets.QLabel("Top-N:"), self.spin_neighbors, self.btn_refresh_neighbors]:
            top_n_lay.addWidget(w)
        ln.addLayout(top_n_lay)
        self.tbl_neighbors = QtWidgets.QTableView()
        self.model_neighbors = PandasModel(pd.DataFrame(columns=['neighbor', 'similarity', 'shared_werke']))
        self.tbl_neighbors.setModel(self.model_neighbors)
        ln.addWidget(self.tbl_neighbors)
        self.tabs.addTab(w_neighbors, "Nachbarn je Chronik")

        # Info
        w_info = QtWidgets.QWidget()
        li = QtWidgets.QFormLayout(w_info)
        self.lbl_info = QtWidgets.QLabel("Noch keine Daten geladen.")
        self.lbl_info.setWordWrap(True)
        li.addRow(self.lbl_info)
        self.tabs.addTab(w_info, "Info")

        lay.addWidget(self.tabs, 1)

        # Style
        self._apply_glass_style(self)

    def _drop_shadow(self, widget: QtWidgets.QWidget) -> QtWidgets.QGraphicsDropShadowEffect:
        sh = QtWidgets.QGraphicsDropShadowEffect(widget)
        sh.setBlurRadius(24)
        sh.setOffset(0, 6)
        sh.setColor(QtGui.QColor(0, 0, 0, 140))
        return sh

    def _apply_glass_style(self, w: QtWidgets.QWidget) -> None:
        QtWidgets.QApplication.setStyle("Fusion")
        pal = QtGui.QPalette()
        pal.setColor(QtGui.QPalette.Window, QtGui.QColor(18, 18, 18))
        pal.setColor(QtGui.QPalette.Base, QtGui.QColor(28, 28, 28, 200))
        pal.setColor(QtGui.QPalette.AlternateBase, QtGui.QColor(38, 38, 38, 200))
        pal.setColor(QtGui.QPalette.Text, QtGui.QColor(230, 230, 230))
        pal.setColor(QtGui.QPalette.Button, QtGui.QColor(40, 40, 40, 180))
        pal.setColor(QtGui.QPalette.ButtonText, QtGui.QColor(240, 240, 240))
        pal.setColor(QtGui.QPalette.Highlight, QtGui.QColor(90, 150, 255, 180))
        pal.setColor(QtGui.QPalette.HighlightedText, QtGui.QColor(0, 0, 0))
        w.setPalette(pal)
        w.setStyleSheet("""
            QDialog { background: qlineargradient(x1:0,y1:0,x2:0,y2:1,
                       stop:0 rgba(20,20,24,255), stop:1 rgba(12,12,14,255)); }
            #glassContainer { background: rgba(30,30,34,160); border-radius: 14px; padding: 10px; }
            #glassTabs::pane { border: 0; background: transparent; }
            QTabBar::tab { background: rgba(60,60,66,140); border-radius: 8px; padding: 6px 12px; margin: 4px; }
            QTabBar::tab:selected { background: rgba(90,150,255,140); color: black; }
            QLineEdit, QComboBox, QSpinBox, QDoubleSpinBox { background: rgba(24,24,28,200); border: 1px solid rgba(100,100,110,120); border-radius: 6px; padding: 4px 6px; }
            QPushButton { background: rgba(50,50,56,180); border: 1px solid rgba(110,110,120,120); border-radius: 8px; padding: 6px 12px; }
            QPushButton:hover { background: rgba(70,70,80,200); }
            QPushButton:pressed { background: rgba(90,90,100,220); }
            QTableView { background: rgba(24,24,28,200); alternate-background-color: rgba(32,32,36,200); gridline-color: rgba(120,120,130,120); }
        """)

    # ---- Restore ----

    def _restore_last(self) -> None:
        if self._config.last_csv and Path(self._config.last_csv).exists():
            self.le_path.setText(self._config.last_csv)
            try:
                self._df = read_csv_auto(self._config.last_csv)
                self.populate_column_boxes()
                # Restore column picks if present
                for cb, val in ((self.cb_chronik, self._config.chronik_col),
                                (self.cb_werk, self._config.werk_col),
                                (self.cb_intensity, self._config.intensity_col or "— keine —")):
                    if val and cb.findText(val) >= 0:
                        cb.setCurrentIndex(cb.findText(val))
                debug("Letzte CSV automatisch geladen.")
            except Exception as e:
                debug(f"Auto-Laden der letzten CSV scheiterte: {e}")

    # ---------- Slots ----------

    def on_browse(self):
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "CSV wählen", "", "CSV (*.csv);;Alle Dateien (*)")
        if not path:
            return
        try:
            df = read_csv_auto(path)
            debug(f"CSV gelesen: {path} mit {len(df)} Zeilen und {len(df.columns)} Spalten.")
            self._df = df
            self.le_path.setText(path)
            self._config.last_csv = path
            self._config.save()
            self.populate_column_boxes()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", f"CSV konnte nicht geladen werden:\n{e}")

    def populate_column_boxes(self):
        if self._df is None:
            return
        cols = list(self._df.columns)
        self.cb_chronik.clear()
        self.cb_werk.clear()
        self.cb_intensity.clear()
        self.cb_intensity.addItem("— keine —")
        self.cb_chronik.addItems(cols)
        self.cb_werk.addItems(cols)
        self.cb_intensity.addItems(cols)

        # Heuristische Vorschläge
        c_guess = guess_column(self._df, ['canonical', 'chronik', 'chronicle', 'entity', 'work'])
        w_guess = guess_column(self._df, ['pdf_file', 'pdf', 'file', 'werk', 'document', 'label'])
        i_guess = guess_column(self._df, ['intensity', 'weight', 'score'])
        if c_guess:
            self.cb_chronik.setCurrentIndex(self.cb_chronik.findText(c_guess))
        if w_guess:
            self.cb_werk.setCurrentIndex(self.cb_werk.findText(w_guess))
        if i_guess:
            self.cb_intensity.setCurrentIndex(self.cb_intensity.findText(i_guess))

    def on_analyze(self):
        if self._df is None:
            QtWidgets.QMessageBox.warning(self, "Hinweis", "Bitte zuerst eine CSV laden.")
            return
        chronik_col = self.cb_chronik.currentText()
        werk_col = self.cb_werk.currentText()
        intensity_col = self.cb_intensity.currentText()
        if intensity_col == "— keine —":
            intensity_col = None
        weighting = self.cb_weight.currentText()
        use_tfidf = self.chk_tfidf.isChecked()
        min_shared = int(self.spin_min_shared.value())
        min_sim = float(self.dsb_min_sim.value())
        topk = int(self.spin_topk.value())

        try:
            mat, chroniken, werke, agg = ChronicleSimilarityEngine.build_matrix(
                self._df, chronik_col, werk_col, intensity_col, weighting, use_tfidf
            )
            self._mat = mat
            self._chroniken = chroniken
            self._werke = werke

            nonzero_mask = (mat > 0.0)
            df_pairs, S = ChronicleSimilarityEngine.pairs_from_similarity(
                mat, chroniken, nonzero_mask, topk, min_shared, min_sim
            )
            self._sim_matrix = S

            # Anzeige ggf. nur Dateiname
            if self.chk_show_basename.isChecked():
                df_pairs = apply_display_names(df_pairs, ['chronik_a', 'chronik_b'], self.chk_drop_ext.isChecked())

            self.model_pairs.set_dataframe(df_pairs)
            self.cb_chronik_pick.clear()
            picks = [basename_only(c, self.chk_drop_ext.isChecked()) if self.chk_show_basename.isChecked() else c
                     for c in self._chroniken]
            self.cb_chronik_pick.addItems(picks)
            self.refresh_neighbors()
            self.update_info(mat, agg)

            # Speichern der Spaltenwahl
            self._config.chronik_col = chronik_col
            self._config.werk_col = werk_col
            self._config.intensity_col = intensity_col or ""
            self._config.save()

            debug(f"Analyse fertig: {len(chroniken)} Chroniken, {len(werke)} Werke, {len(df_pairs)} Paare.")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Analyse fehlgeschlagen:\n{e}")

    def refresh_neighbors(self):
        if self._sim_matrix is None or not self._chroniken:
            return
        # Index der gewählten Chronik finden, auch wenn Anzeige gekürzt ist
        shown = self.cb_chronik_pick.currentText()
        if self.chk_show_basename.isChecked():
            # map basename->index
            drop_ext = self.chk_drop_ext.isChecked()
            mapping = {basename_only(c, drop_ext): i for i, c in enumerate(self._chroniken)}
            i = mapping.get(shown, 0)
        else:
            i = self._chroniken.index(shown)

        sims = self._sim_matrix[i, :]
        order = np.argsort(-sims)
        rows = []
        for j in order:
            if j == i:
                continue
            sim = float(sims[j])
            shared = int(np.logical_and(self._mat[i, :] > 0.0, self._mat[j, :] > 0.0).sum())
            name = self._chroniken[j]
            if self.chk_show_basename.isChecked():
                name = basename_only(name, self.chk_drop_ext.isChecked())
            rows.append((name, sim, shared))
        topn = int(self.spin_neighbors.value())
        df = pd.DataFrame(rows[:topn], columns=['neighbor', 'similarity', 'shared_werke'])
        self.model_neighbors.set_dataframe(df)

    def update_info(self, mat: np.ndarray, agg: pd.DataFrame):
        nonzero = int((mat > 0.0).sum())
        sparsity = 1.0 - nonzero / float(mat.size) if mat.size else 1.0
        txt = (
            f"Chroniken: {len(self._chroniken)} | Werke: {len(self._werke)} | "
            f"Matrix: {mat.shape} | belegte Zellen: {nonzero} | Sparsity: {sparsity:.2f}"
        )
        self.lbl_info.setText(txt)

    def on_export_pairs(self):
        df = self.model_pairs._df
        if df is None or df.empty:
            QtWidgets.QMessageBox.information(self, "Hinweis", "Keine Paar-Tabelle vorhanden.")
            return
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Paare exportieren", "chronik_pairs.csv", "CSV (*.csv)")
        if not path:
            return
        try:
            df.to_csv(path, index=False)
            debug(f"Paare exportiert: {path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Konnte nicht exportieren:\n{e}")

    def on_export_matrix(self):
        if self._sim_matrix is None or not self._chroniken:
            QtWidgets.QMessageBox.information(self, "Hinweis", "Keine Similarity-Matrix vorhanden.")
            return
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Matrix exportieren",
                                                        "chronik_similarity_matrix.csv", "CSV (*.csv)")
        if not path:
            return
        try:
            # Optional Anzeige-Namen auch als Index verwenden
            idx = [basename_only(c, self.chk_drop_ext.isChecked()) if self.chk_show_basename.isChecked() else c
                   for c in self._chroniken]
            df = pd.DataFrame(self._sim_matrix, index=idx, columns=idx)
            df.to_csv(path, index=True)
            debug(f"Matrix exportiert: {path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Konnte nicht exportieren:\n{e}")

    def on_html_report(self):
        if self.model_pairs._df is None or self.model_pairs._df.empty:
            QtWidgets.QMessageBox.information(self, "Hinweis", "Bitte zuerst Analyse ausführen.")
            return
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "HTML-Report speichern",
                                                        "chronik_report.html", "HTML (*.html)")
        if not path:
            return
        try:
            top_pairs = self.model_pairs._df.copy()
            # kleine Kürzung für Bericht
            top_pairs = top_pairs.head(200)
            html = self._build_html_report(top_pairs)
            Path(path).write_text(html, encoding="utf-8")
            debug(f"Report gespeichert: {path}")
            webbrowser.open(f"file://{Path(path).absolute()}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Report konnte nicht erstellt werden:\n{e}")

    def _build_html_report(self, top_pairs: pd.DataFrame) -> str:
        drop_ext = self.chk_drop_ext.isChecked()
        show_base = self.chk_show_basename.isChecked()
        if show_base:
            top_pairs = apply_display_names(top_pairs, ['chronik_a', 'chronik_b'], drop_ext)

        # Nachbarn für die ersten 15 Chroniken
        neighbor_sections = []
        n_each = 10
        for idx, c in enumerate(self._chroniken[:15]):
            sims = self._sim_matrix[idx, :]
            order = np.argsort(-sims)
            rows = []
            for j in order:
                if j == idx:
                    continue
                name = self._chroniken[j]
                if show_base:
                    name = basename_only(name, drop_ext)
                rows.append((name, float(sims[j])))
            rows = rows[:n_each]
            cname = basename_only(c, drop_ext) if show_base else c
            neighbor_sections.append((cname, rows))

        # HTML
        def esc(s: str) -> str:
            return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

        tp_rows = "\n".join(
            f"<tr><td>{esc(a)}</td><td>{esc(b)}</td><td>{sim:.3f}</td><td>{shared}</td></tr>"
            for a, b, sim, shared in top_pairs[['chronik_a', 'chronik_b', 'similarity', 'shared_werke']].values
        )
        neighbor_html = ""
        for cname, rows in neighbor_sections:
            nr = "\n".join(f"<tr><td>{esc(name)}</td><td>{sim:.3f}</td></tr>" for name, sim in rows)
            neighbor_html += f"""
            <h3>{esc(cname)}</h3>
            <table class="t">
                <thead><tr><th>Nachbar</th><th>Similarity</th></tr></thead>
                <tbody>{nr}</tbody>
            </table>
            """

        return f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<title>Chronik-Report</title>
<style>
body {{ font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; background: #0d0f12; color: #e6e6e6; margin: 24px; }}
h1,h2,h3 {{ color: #cfe2ff; }}
.t {{ width: 100%; border-collapse: collapse; margin: 12px 0; background: #151820; border: 1px solid #2a2f3a; }}
.t th, .t td {{ padding: 8px 10px; border-bottom: 1px solid #2a2f3a; }}
.t thead th {{ background: #1b2030; text-align: left; }}
.small {{ opacity: .8 }}
.code {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
</style>
</head>
<body>
<h1>Chronik-Ähnlichkeiten</h1>
<p class="small">Datei: <span class="code">{esc(self.le_path.text())}</span></p>

<h2>Top-Paare</h2>
<table class="t">
  <thead><tr><th>chronik_a</th><th>chronik_b</th><th>similarity</th><th>shared_werke</th></tr></thead>
  <tbody>
  {tp_rows}
  </tbody>
</table>

<h2>Nachbarn je Chronik (erste 15)</h2>
{neighbor_html}

</body>
</html>"""

# ---------------------- Main-Fenster ----------------------

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Chronik-Analyse")
        self.resize(1024, 280)

        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        lay = QtWidgets.QVBoxLayout(central)

        info = QtWidgets.QLabel("Analyse von Ähnlichkeiten zwischen Chroniken über Werke/PDFs.")
        info.setAlignment(QtCore.Qt.AlignCenter)
        info.setObjectName("heroLabel")
        lay.addWidget(info)

        btn = QtWidgets.QPushButton("Chronik-Ähnlichkeiten analysieren …")
        btn.setMinimumHeight(44)
        btn.setGraphicsEffect(self._drop_shadow(btn))
        btn.clicked.connect(self.open_dialog)
        lay.addWidget(btn, alignment=QtCore.Qt.AlignCenter)

        self._apply_style(self)

    def _drop_shadow(self, widget: QtWidgets.QWidget) -> QtWidgets.QGraphicsDropShadowEffect:
        sh = QtWidgets.QGraphicsDropShadowEffect(widget)
        sh.setBlurRadius(20)
        sh.setOffset(0, 6)
        sh.setColor(QtGui.QColor(0, 0, 0, 140))
        return sh

    def _apply_style(self, w: QtWidgets.QWidget) -> None:
        QtWidgets.QApplication.setStyle("Fusion")
        w.setStyleSheet("""
            QMainWindow { background: qlineargradient(x1:0,y1:0,x2:0,y2:1,
                         stop:0 #13151a, stop:1 #0d0f12); }
            #heroLabel { font-size: 18px; color: #d7e3ff; }
            QPushButton { background: rgba(60,60,70,200); border-radius: 10px; padding: 10px 16px;
                          border: 1px solid rgba(110,110,120,120); }
            QPushButton:hover { background: rgba(80,80,92,220); }
            QPushButton:pressed { background: rgba(100,100,118,240); }
        """)

    def open_dialog(self):
        dlg = ChronicleSimilarityDialog(self)
        dlg.exec()


# ---------------------- main ----------------------

def main() -> None:
    debug("Starte Chronik-Ähnlichkeits-GUI …")
    # Hohe DPI
    QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling, True)
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()