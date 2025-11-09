"""
chronik_main_gui.py
GUI mit drei Tabs:
  • Scan: externes Finder-Script starten, Log live, letzte CSV merken
  • Netzwerk: Darstellung wie network.py (GraphCanvas)
  • Ähnlichkeit: Kosinusähnlichkeiten, Sortiermodi, Details-Modal mit „genauen Stellen“
Abhängigkeiten:
  pip install PyQt5 pandas numpy networkx
Optional (Finder-Script): pymupdf pytesseract pillow
Start:
  python chronik_main_gui.py
"""
from __future__ import annotations

import math
import os
import re
import sys
import webbrowser
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import networkx as nx
import numpy as np
import pandas as pd
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import Qt

# ---- network.py-API 1:1 nutzen (liegt extern) ----
from network import (
    load_mentions_csv,
    resolve_columns,
    build_bipartite_graph,
    compute_layout,
    make_scene,
    GraphCanvas,
)

# --------------------- Utils / Persistenz ---------------------

def debug(msg: str) -> None:
    print(f"[DEBUG] {msg}", flush=True)

ORG = "chroniken_suite"
APP = "chronik_main_gui"

def _settings() -> QtCore.QSettings:
    QtCore.QCoreApplication.setOrganizationName(ORG)
    QtCore.QCoreApplication.setApplicationName(APP)
    return QtCore.QSettings()

def get_last_csv() -> str:
    val = _settings().value("last_csv", type=str)
    return val or ""

def set_last_csv(path: str) -> None:
    s = _settings()
    s.setValue("last_csv", path)
    s.sync()
    debug(f"last_csv={path}")

def get_last_session() -> str:
    val = _settings().value("last_session", type=str)
    return val or ""

def set_last_session(path: str) -> None:
    s = _settings()
    s.setValue("last_session", path)
    s.sync()
    debug(f"last_session={path}")

def read_csv_auto(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    try:
        return pd.read_csv(path, sep=";", engine="python")
    except Exception:
        return pd.read_csv(path, sep=None, engine="python")

def guess_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower:
            return lower[c]
    return None

def _base_name(s: str, drop_ext: bool) -> str:
    b = os.path.basename(str(s))
    return os.path.splitext(b)[0] if drop_ext else b

def _looks_like_path(s: str) -> bool:
    s = str(s)
    return ("/" in s or "\\" in s) or s.lower().endswith(".pdf")

# --------------------- Netzwerk-Tab ---------------------

class NetworkTab(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.df: Optional[pd.DataFrame] = None
        self.G: Optional[nx.Graph] = None
        self.pos: Dict[str, Tuple[float, float]] = {}
        self._build_ui()

    def _build_ui(self) -> None:
        lay = QtWidgets.QVBoxLayout(self)
        controls = QtWidgets.QHBoxLayout()
        self.btn_load = QtWidgets.QPushButton("CSV laden…")
        self.cmb_layout = QtWidgets.QComboBox()
        self.cmb_layout.addItems(["bipartite", "spring", "kamada_kawai", "forceatlas2"])
        self.spin_w = QtWidgets.QSpinBox()
        self.spin_w.setRange(1, 10**9)
        self.spin_w.setValue(1)
        self.chk_labels = QtWidgets.QCheckBox("Labels")
        self.chk_labels.setChecked(True)
        self.search = QtWidgets.QLineEdit()
        self.search.setPlaceholderText("Knoten suchen…")
        self.btn_search = QtWidgets.QPushButton("Hervorheben")
        self.btn_export = QtWidgets.QPushButton("PNG exportieren")
        for w in [
            self.btn_load,
            QtWidgets.QLabel("Layout:"),
            self.cmb_layout,
            QtWidgets.QLabel("min Kantengew.:"),
            self.spin_w,
            self.chk_labels,
            self.search,
            self.btn_search,
            self.btn_export,
        ]:
            controls.addWidget(w)
        lay.addLayout(controls)

        self.view = GraphCanvas()
        lay.addWidget(self.view, 1)

        self.btn_load.clicked.connect(self._choose_csv)
        self.cmb_layout.currentTextChanged.connect(lambda _: self._rebuild())
        self.spin_w.valueChanged.connect(lambda _: self._rebuild())
        self.chk_labels.toggled.connect(lambda _: self._rebuild())
        self.btn_search.clicked.connect(self._do_search)
        self.btn_export.clicked.connect(self._export_png)

    def _choose_csv(self) -> None:
        p, _ = QtWidgets.QFileDialog.getOpenFileName(self, "CSV öffnen", os.getcwd(), "CSV (*.csv)")
        if p:
            self.set_csv_from_outside(p)

    def set_csv_from_outside(self, path: str) -> None:
        try:
            self.df = load_mentions_csv(path)
            set_last_csv(path)
            self._rebuild()
        except Exception as ex:
            QtWidgets.QMessageBox.critical(self, "CSV-Fehler", str(ex))

    @staticmethod
    def _build_general_graph(df: pd.DataFrame) -> nx.Graph:
        lower = {c.lower(): c for c in df.columns}
        s_col = lower.get("src") or lower.get("source")
        t_col = lower.get("tgt") or lower.get("target")
        if not s_col or not t_col:
            raise KeyError("Allgemeines Edge-Format nicht erkannt (erwartet Spalten src,tgt).")
        w_col = None
        for cand in ("weighted", "weight", "total", "w", "count"):
            if cand in lower:
                w_col = lower[cand]
                break
        G = nx.Graph(name="Kantenliste")
        nid = lambda x: f"C|{x}"
        for _, r in df[[s_col, t_col] + ([w_col] if w_col else [])].dropna().iterrows():
            a = str(r[s_col]).strip()
            b = str(r[t_col]).strip()
            if not a or not b:
                continue
            u, v = nid(a), nid(b)
            if not G.has_node(u):
                G.add_node(u, role="chronik", label=a, mentions=1)
            if not G.has_node(v):
                G.add_node(v, role="chronik", label=b, mentions=1)
            w = float(r[w_col]) if w_col and pd.notna(r[w_col]) else 1.0
            if G.has_edge(u, v):
                G[u][v]["weight"] += w
            else:
                G.add_edge(u, v, weight=w)
        return G

    def _rebuild(self) -> None:
        if self.df is None:
            return
        use_general = False
        try:
            doc_col, work_col = resolve_columns(self.df)
            self.G = build_bipartite_graph(self.df, doc_col, work_col)
        except Exception:
            try:
                self.G = self._build_general_graph(self.df)
                use_general = True
            except Exception as ex2:
                QtWidgets.QMessageBox.warning(
                    self,
                    "CSV-Format unbekannt",
                    f"Keine Mentions-CSV und keine Kantenliste erkannt:\n{ex2}",
                )
                return
        mode = self.cmb_layout.currentText()
        if use_general and mode == "bipartite":
            mode = "spring"
        self.pos = compute_layout(self.G, mode)
        scn = make_scene(self.G, self.pos, int(self.spin_w.value()), self.chk_labels.isChecked())
        self.view.set_graph_scene(scn)
        debug(
            f"Netz aktualisiert: nodes={self.G.number_of_nodes()} edges={self.G.number_of_edges()} general={use_general}"
        )

    def _do_search(self) -> None:
        hits = self.view.highlight(self.search.text(), self.chk_labels.isChecked())
        QtWidgets.QToolTip.showText(self.mapToGlobal(self.search.pos()), f"Treffer: {hits}", self.search)

    def _export_png(self) -> None:
        csv = get_last_csv()
        if not csv:
            QtWidgets.QMessageBox.information(self, "Hinweis", "CSV zuerst laden.")
            return
        base = os.path.splitext(os.path.basename(csv))[0]
        out = os.path.join(os.path.dirname(csv), f"{base}_graph.png")
        try:
            self.view.export_png(out)
            QtWidgets.QMessageBox.information(self, "Export", f"Exportiert: {out}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", str(e))

# --------------------- Ähnlichkeit: Mathe + Anzeige + Details-Dialog ---------------------

def _tfidf(mat: np.ndarray) -> np.ndarray:
    rs = mat.sum(axis=1, keepdims=True)
    rs[rs == 0] = 1.0
    tf = mat / rs
    df = (mat > 0).sum(axis=0, keepdims=True)
    n = mat.shape[0]
    idf = np.log((1.0 + n) / (1.0 + df)) + 1.0
    return tf * idf

def _cosine(mat: np.ndarray) -> np.ndarray:
    nrm = np.linalg.norm(mat, axis=1, keepdims=True)
    nrm[nrm == 0] = 1.0
    m = mat / nrm
    return m @ m.T

class PandasModel(QtCore.QAbstractTableModel):
    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self._df = df.copy()

    def rowCount(self, parent=QtCore.QModelIndex()):
        return len(self._df)

    def columnCount(self, parent=QtCore.QModelIndex()):
        return len(self._df.columns)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or role not in (Qt.DisplayRole, Qt.ToolTipRole):
            return None
        v = self._df.iat[index.row(), index.column()]
        return "" if pd.isna(v) else str(v)

    def headerData(self, s, orient, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        return str(self._df.columns[s]) if orient == Qt.Horizontal else str(s)

    def set_dataframe(self, df: pd.DataFrame):
        self.beginResetModel()
        self._df = df.copy()
        self.endResetModel()

class SortHelpDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Sortierung – Hilfe")
        self.resize(560, 360)
        lay = QtWidgets.QVBoxLayout(self)
        txt = QtWidgets.QTextBrowser()
        txt.setReadOnly(True)
        txt.setOpenExternalLinks(False)
        txt.setHtml(
            "<h3>Sortiermodi</h3>"
            "<ul>"
            "<li><b>similarity</b>: absteigend nach Kosinusähnlichkeit.</li>"
            "<li><b>shared</b>: zuerst Anzahl gemeinsamer Werke, dann similarity.</li>"
            "<li><b>combo</b>: similarity × log(1 + shared). Balanciert Qualität und Masse.</li>"
            "</ul>"
            "<h4>Beispiele</h4>"
            "<pre>"
            "Paar X: sim=0,60, shared=20 → combo=0,60×log(21)=~1,82\n"
            "Paar Y: sim=0,80, shared=4  → combo=0,80×log(5)=~1,29\n"
            "→ combo bevorzugt X, similarity bevorzugt Y, shared bevorzugt 20.\n"
            "</pre>"
        )
        lay.addWidget(txt)
        btn = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok)
        btn.accepted.connect(self.accept)
        lay.addWidget(btn)

class DetailsDialog(QtWidgets.QDialog):
    def __init__(
        self,
        df: pd.DataFrame,
        col_chronik: str,
        col_werk: str,
        a_key: str,
        b_key: str,
        intensity_col: Optional[str],
        parent=None,
    ):
        super().__init__(parent)
        self._df = df
        self._c = col_chronik
        self._w = col_werk
        self._ic = intensity_col
        self._a = a_key
        self._b = b_key
        self.setWindowTitle(f"Details: {os.path.basename(a_key)} ↔ {os.path.basename(b_key)}")
        self.resize(1100, 720)

        lay = QtWidgets.QVBoxLayout(self)

        # Shared works
        a_w = set(df.loc[df[self._c] == a_key, self._w].dropna().astype(str))
        b_w = set(df.loc[df[self._c] == b_key, self._w].dropna().astype(str))
        shared = sorted(a_w & b_w)

        # Summary dataframe for shared works
        def _counts_for(name: str) -> pd.Series:
            sub = df[df[self._c] == name]
            cnt = sub[self._w].value_counts()
            if self._ic and self._ic in sub.columns:
                inten = sub.groupby(self._w)[self._ic].apply(lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum())
            else:
                inten = pd.Series(dtype=float)
            return pd.DataFrame({"count": cnt, "intensity": inten}).fillna(0)["count"]

        cnt_a = df[df[self._c] == a_key][self._w].value_counts()
        cnt_b = df[df[self._c] == b_key][self._w].value_counts()

        rows = []
        for w in shared:
            ca = int(cnt_a.get(w, 0))
            cb = int(cnt_b.get(w, 0))
            rows.append((os.path.basename(w) if _looks_like_path(w) else w, ca, cb, ca + cb, w))
        df_sum = pd.DataFrame(rows, columns=["werk", "count_a", "count_b", "count_total", "_werk_key"]).sort_values(
            ["count_total", "werk"], ascending=[False, True], ignore_index=True
        )

        # Build UI
        tabs = QtWidgets.QTabWidget()
        # Tab 1: Gemeinsame Werke
        w1 = QtWidgets.QWidget()
        l1 = QtWidgets.QVBoxLayout(w1)
        self.tbl_shared = QtWidgets.QTableView()
        self.model_shared = PandasModel(df_sum.drop(columns=["_werk_key"]))
        self.tbl_shared.setModel(self.model_shared)
        self.tbl_shared.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.tbl_shared.doubleClicked.connect(self._filter_to_selected_werk)
        l1.addWidget(QtWidgets.QLabel("Gemeinsame Werke (Doppelklick filtert Rohdaten unten)"))
        l1.addWidget(self.tbl_shared, 1)

        # Tab 2: Rohdaten
        w2 = QtWidgets.QWidget()
        l2 = QtWidgets.QVBoxLayout(w2)
        self.le_filter = QtWidgets.QLineEdit()
        self.le_filter.setPlaceholderText("Werk-Filter (exact oder Teilstring) …")
        self.le_filter.textChanged.connect(self._apply_text_filter)
        l2.addWidget(self.le_filter)

        split = QtWidgets.QSplitter(Qt.Horizontal)
        self.tbl_a = QtWidgets.QTableView()
        self.tbl_b = QtWidgets.QTableView()
        self.tbl_a.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.tbl_b.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        split.addWidget(self.tbl_a)
        split.addWidget(self.tbl_b)
        split.setSizes([550, 550])

        self._df_a_full = df[(df[self._c] == a_key) & (df[self._w].isin(shared))].copy()
        self._df_b_full = df[(df[self._c] == b_key) & (df[self._w].isin(shared))].copy()

        # Reorder columns: Werk, evtl. Seite/Kontext, dann Rest
        def reorder_cols(d: pd.DataFrame) -> List[str]:
            cols = list(d.columns)
            pri = [self._w]
            heur = [x for x in ["page", "seite", "page_num", "page_index", "line", "context", "snippet", "text"] if x in cols]
            rest = [c for c in cols if c not in pri + heur]
            return pri + heur + rest

        self.model_a = PandasModel(self._df_a_full[reorder_cols(self._df_a_full)])
        self.model_b = PandasModel(self._df_b_full[reorder_cols(self._df_b_full)])
        self.tbl_a.setModel(self.model_a)
        self.tbl_b.setModel(self.model_b)

        # Doppelklick: PDF öffnen, wenn Pfad vorhanden
        self.tbl_a.doubleClicked.connect(lambda _: self._open_pdf_from_table(self.tbl_a, self._df_a_full))
        self.tbl_b.doubleClicked.connect(lambda _: self._open_pdf_from_table(self.tbl_b, self._df_b_full))

        l2.addWidget(QtWidgets.QLabel(f"Rohdaten: {os.path.basename(a_key)} / {os.path.basename(b_key)}  – Doppelklick öffnet PDF"))
        l2.addWidget(split, 1)

        tabs.addTab(w1, "Werke")
        tabs.addTab(w2, "Vorkommen")
        lay.addWidget(tabs, 1)

        # Buttons
        btns = QtWidgets.QDialogButtonBox()
        self.btn_export_shared = btns.addButton("Shared CSV", QtWidgets.QDialogButtonBox.ActionRole)
        self.btn_export_a = btns.addButton("A CSV", QtWidgets.QDialogButtonBox.ActionRole)
        self.btn_export_b = btns.addButton("B CSV", QtWidgets.QDialogButtonBox.ActionRole)
        btns.addButton(QtWidgets.QDialogButtonBox.Close)
        self.btn_export_shared.clicked.connect(lambda: self._export_df(df_sum.drop(columns=["_werk_key"]), "shared"))
        self.btn_export_a.clicked.connect(lambda: self._export_df(self._df_a_view, "A"))
        self.btn_export_b.clicked.connect(lambda: self._export_df(self._df_b_view, "B"))
        btns.rejected.connect(self.reject)
        lay.addWidget(btns)

        # Views for filtering
        self._df_a_view = self._df_a_full.copy()
        self._df_b_view = self._df_b_full.copy()

    def _find_path_col(self, df: pd.DataFrame) -> Optional[str]:
        # Prefer the Werk-Spalte, wenn sie wie Pfad aussieht
        sample = df[self._w].dropna().astype(str).head(10).tolist()
        if any(_looks_like_path(x) for x in sample):
            return self._w
        # Sonst heuristisch andere Pfadspalten suchen
        for cand in ["pdf_file", "pdf", "file", "document", "path"]:
            if cand in df.columns:
                return cand
        return None

    def _open_pdf_from_table(self, table: QtWidgets.QTableView, backing: pd.DataFrame) -> None:
        row = table.currentIndex().row()
        if row < 0:
            return
        # Map view row to dataframe row
        try:
            disp_df = table.model()._df
            rec = disp_df.iloc[row]
            # Bestimme Originalreihe über eindeutiges Matching
            key_cols = [c for c in backing.columns if c in disp_df.columns]
            if not key_cols:
                key_cols = [self._w]
            # Fallback: nimm Ansicht selbst
            cand = rec
            df = backing
            path_col = self._find_path_col(df)
            if path_col and pd.notna(rec.get(path_col, None)):
                p = str(rec[path_col])
            else:
                # Wenn nur der Werk-Name da ist, suche die erste passende Zeile in backing
                p = None
                if self._w in rec.index:
                    val = rec[self._w]
                    hit = df[df[self._w] == val].head(1)
                    if not hit.empty and path_col and path_col in hit.columns:
                        p = str(hit.iloc[0][path_col])
            if p and os.path.isfile(p):
                QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(p))
            else:
                QtWidgets.QMessageBox.information(self, "Hinweis", "Kein gültiger PDF-Pfad gefunden.")
        except Exception as ex:
            QtWidgets.QMessageBox.information(self, "Hinweis", f"Öffnen fehlgeschlagen: {ex}")

    def _filter_to_selected_werk(self, index: QtCore.QModelIndex) -> None:
        if not index.isValid():
            return
        werk_disp = self.model_shared._df.iloc[index.row(), 0]
        # Rekonstruiere Key: wir hatten Original in df_sum._werk_key
        # Einfach per Basename matchen
        base = str(werk_disp)
        def _filter(df: pd.DataFrame) -> pd.DataFrame:
            vals = df[self._w].astype(str)
            return df[vals.map(lambda x: os.path.basename(x) if _looks_like_path(x) else x) == base]
        self._df_a_view = _filter(self._df_a_full)
        self._df_b_view = _filter(self._df_b_full)
        self.model_a.set_dataframe(self._df_a_view)
        self.model_b.set_dataframe(self._df_b_view)

    def _apply_text_filter(self, text: str) -> None:
        t = text.strip().lower()
        def _f(df: pd.DataFrame) -> pd.DataFrame:
            if not t:
                return df
            vals = df[self._w].astype(str).str.lower()
            return df[vals.str.contains(t, na=False)]
        self.model_a.set_dataframe(_f(self._df_a_full))
        self.model_b.set_dataframe(_f(self._df_b_full))

    def _export_df(self, df: pd.DataFrame, tag: str) -> None:
        p, _ = QtWidgets.QFileDialog.getSaveFileName(self, f"Export {tag}", f"details_{tag}.csv", "CSV (*.csv)")
        if p:
            df.to_csv(p, index=False)

class SimilarityTab(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.df: Optional[pd.DataFrame] = None
        self._chroniken: List[str] = []
        self._werke: List[str] = []
        self._M: Optional[np.ndarray] = None
        self._S: Optional[np.ndarray] = None
        self._col_c: Optional[str] = None
        self._col_w: Optional[str] = None
        self._col_ic: Optional[str] = None
        self._build_ui()

    # --- QSettings helpers ---
    def _sim_set(self, k: str, v) -> None:
        s = _settings()
        s.setValue(f"sim/{k}", v)
        s.sync()

    def _sim_get(self, k: str, typ=str, default=None):
        v = _settings().value(f"sim/{k}", type=typ)
        return v if v is not None else default

    def _build_ui(self) -> None:
        lay = QtWidgets.QVBoxLayout(self)

        top = QtWidgets.QHBoxLayout()
        self.btn_load = QtWidgets.QPushButton("CSV laden…")
        top.addWidget(self.btn_load)
        self.cb_ch = QtWidgets.QComboBox()
        self.cb_wk = QtWidgets.QComboBox()
        self.cb_int = QtWidgets.QComboBox()
        self.cb_int.addItem("— keine —")
        for w in [
            QtWidgets.QLabel("Chronik:"),
            self.cb_ch,
            QtWidgets.QLabel("Werk/PDF:"),
            self.cb_wk,
            QtWidgets.QLabel("Intensität:"),
            self.cb_int,
        ]:
            top.addWidget(w)
        lay.addLayout(top)

        opts = QtWidgets.QHBoxLayout()
        self.cmb_weight = QtWidgets.QComboBox()
        self.cmb_weight.addItems(["count", "intensity", "binary"])
        self.chk_tfidf = QtWidgets.QCheckBox("TF-IDF")
        self.spin_shared = QtWidgets.QSpinBox()
        self.spin_shared.setRange(0, 10000)
        self.spin_shared.setValue(1)
        self.sim_min = QtWidgets.QDoubleSpinBox()
        self.sim_min.setRange(0.0, 1.0)
        self.sim_min.setSingleStep(0.01)
        self.sim_min.setValue(0.2)
        self.topk = QtWidgets.QSpinBox()
        self.topk.setRange(1, 10000)
        self.topk.setValue(10)
        # Anzeige: immer nur Dateiname, Endung behalten
        self.chk_basename = QtWidgets.QCheckBox("nur Dateiname")
        self.chk_basename.setChecked(True)
        self.chk_basename.setVisible(False)
        self.chk_dropext = QtWidgets.QCheckBox("Endung entfernen")
        self.chk_dropext.setChecked(False)
        self.chk_dropext.setVisible(False)
        self.cmb_sort = QtWidgets.QComboBox()
        self.cmb_sort.addItems(["similarity", "shared", "combo"])
        self.btn_sort_help = QtWidgets.QToolButton()
        self.btn_sort_help.setText("?")
        self.btn_sort_help.setToolTip("Erklärt die Sortiermodi")
        self.btn_sort_help.clicked.connect(lambda: SortHelpDialog(self).exec_())

        for w in [
            QtWidgets.QLabel("Gewichtung:"),
            self.cmb_weight,
            self.chk_tfidf,
            QtWidgets.QLabel("min gemeinsame Werke:"),
            self.spin_shared,
            QtWidgets.QLabel("min Ähnlichkeit:"),
            self.sim_min,
            QtWidgets.QLabel("Top-K je Chronik:"),
            self.topk,
            QtWidgets.QLabel("Sortierung:"),
            self.cmb_sort,
            self.btn_sort_help,
        ]:
            opts.addWidget(w)
        lay.addLayout(opts)

        btns = QtWidgets.QHBoxLayout()
        self.btn_run = QtWidgets.QPushButton("Analyse")
        self.btn_pairs = QtWidgets.QPushButton("Paare CSV")
        self.btn_mat = QtWidgets.QPushButton("Matrix CSV")
        self.btn_html = QtWidgets.QPushButton("HTML-Report")
        for b in [self.btn_run, self.btn_pairs, self.btn_mat, self.btn_html]:
            btns.addWidget(b)
        btns.addStretch(1)
        lay.addLayout(btns)

        tabs = QtWidgets.QTabWidget()
        # Top-Paare
        w1 = QtWidgets.QWidget()
        v1 = QtWidgets.QVBoxLayout(w1)
        self.tbl_pairs = QtWidgets.QTableView()
        self.model_pairs = PandasModel(
            pd.DataFrame(columns=["chronik_a", "chronik_b", "similarity", "shared_werke", "chronik_a_key", "chronik_b_key"])
        )
        self.tbl_pairs.setModel(self.model_pairs)
        self.tbl_pairs.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.tbl_pairs.doubleClicked.connect(self._open_pair_details)
        v1.addWidget(self.tbl_pairs)
        tabs.addTab(w1, "Top-Paare")

        # Nachbarn je Chronik
        w2 = QtWidgets.QWidget()
        v2 = QtWidgets.QVBoxLayout(w2)
        row = QtWidgets.QHBoxLayout()
        self.cb_pick = QtWidgets.QComboBox()
        self.nei_n = QtWidgets.QSpinBox()
        self.nei_n.setRange(1, 1000)
        self.nei_n.setValue(10)
        row.addWidget(QtWidgets.QLabel("Chronik:"))
        row.addWidget(self.cb_pick, 1)
        row.addWidget(QtWidgets.QLabel("Top-N:"))
        row.addWidget(self.nei_n)
        v2.addLayout(row)
        self.tbl_neighbors = QtWidgets.QTableView()
        self.model_neighbors = PandasModel(
            pd.DataFrame(columns=["neighbor", "similarity", "shared_werke", "neighbor_key"])
        )
        self.tbl_neighbors.setModel(self.model_neighbors)
        self.tbl_neighbors.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.tbl_neighbors.doubleClicked.connect(self._open_neighbor_details)
        v2.addWidget(self.tbl_neighbors)
        tabs.addTab(w2, "Nachbarn je Chronik")
        lay.addWidget(tabs, 1)

        # Signals
        self.btn_load.clicked.connect(self._choose_csv)
        self.btn_run.clicked.connect(self._run)
        self.btn_pairs.clicked.connect(self._export_pairs)
        self.btn_mat.clicked.connect(self._export_matrix)
        self.btn_html.clicked.connect(self._report)
        self.cb_pick.currentTextChanged.connect(lambda _: self._neighbors())
        self.nei_n.valueChanged.connect(lambda _: self._neighbors())

        # Restore options
        self.cmb_weight.setCurrentText(self._sim_get("weighting", str, "count"))
        self.chk_tfidf.setChecked(bool(self._sim_get("tfidf", bool, False)))
        self.spin_shared.setValue(int(self._sim_get("min_shared", int, 1)))
        self.sim_min.setValue(float(self._sim_get("min_sim", float, 0.2)))
        self.topk.setValue(int(self._sim_get("topk", int, 10)))
        self.nei_n.setValue(int(self._sim_get("nei_n", int, 10)))
        self.chk_basename.setChecked(True)
        self.chk_dropext.setChecked(False)
        self.cmb_sort.setCurrentText(self._sim_get("sort_by", str, "similarity"))

    def _choose_csv(self) -> None:
        p, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Mentions-CSV", os.getcwd(), "CSV (*.csv)")
        if p:
            self.load_csv(p)

    def load_csv(self, path: str) -> None:
        self.df = read_csv_auto(path)
        set_last_csv(path)
        self._sim_set("last_csv", path)
        cols = list(self.df.columns)
        self.cb_ch.blockSignals(True)
        self.cb_wk.blockSignals(True)
        self.cb_int.blockSignals(True)
        self.cb_ch.clear(); self.cb_wk.clear(); self.cb_int.clear()
        self.cb_int.addItem("— keine —")
        self.cb_ch.addItems(cols); self.cb_wk.addItems(cols); self.cb_int.addItems(cols)
        saved_ch = self._sim_get("chronik_col", str, None)
        saved_wk = self._sim_get("werk_col", str, None)
        saved_ic = self._sim_get("intensity_col", str, None)
        c_guess = saved_ch if saved_ch in cols else guess_column(self.df, ["canonical", "label", "chronik", "work"])
        w_guess = saved_wk if saved_wk in cols else guess_column(self.df, ["pdf_file", "pdf", "file", "document", "label"])
        if c_guess: self.cb_ch.setCurrentIndex(self.cb_ch.findText(c_guess))
        if w_guess: self.cb_wk.setCurrentIndex(self.cb_wk.findText(w_guess))
        if saved_ic and saved_ic in cols: self.cb_int.setCurrentIndex(self.cb_int.findText(saved_ic))
        self.cb_ch.blockSignals(False); self.cb_wk.blockSignals(False); self.cb_int.blockSignals(False)
        if c_guess and w_guess:
            self._run()

    def _run(self) -> None:
        if self.df is None:
            QtWidgets.QMessageBox.information(self, "Hinweis", "CSV zuerst laden.")
            return
        c = self.cb_ch.currentText().strip()
        w = self.cb_wk.currentText().strip()
        if not c or not w or c not in self.df.columns or w not in self.df.columns:
            QtWidgets.QMessageBox.information(self, "Hinweis", "Spalten für Chronik und Werk/PDF wählen.")
            return
        ic = self.cb_int.currentText().strip()
        ic = None if ic == "— keine —" or not ic else ic
        weight = self.cmb_weight.currentText()
        use_tfidf = self.chk_tfidf.isChecked()
        ms, smin, topk = int(self.spin_shared.value()), float(self.sim_min.value()), int(self.topk.value())
        sort_by = self.cmb_sort.currentText()

        self._col_c, self._col_w, self._col_ic = c, w, ic

        data = self.df[[c, w] + ([ic] if ic else [])].copy()
        if weight == "binary":
            agg = data.groupby([c, w]).size().reset_index(name="val")
            agg["val"] = 1.0
        elif weight == "intensity" and ic:
            data[ic] = pd.to_numeric(data[ic], errors="coerce").fillna(0.0)
            agg = data.groupby([c, w])[ic].sum().reset_index(name="val")
        else:
            agg = data.groupby([c, w]).size().reset_index(name="val")

        chroniken = sorted(agg[c].unique())
        werke = sorted(agg[w].unique())
        if len(chroniken) == 0 or len(werke) == 0:
            QtWidgets.QMessageBox.information(self, "Hinweis", "Zu wenig Daten für Analyse.")
            return

        ci = {x: i for i, x in enumerate(chroniken)}
        wi = {x: i for i, x in enumerate(werke)}
        M = np.zeros((len(chroniken), len(werke)), dtype=float)
        for _, r in agg.iterrows():
            M[ci[r[c]], wi[r[w]]] = float(r["val"])
        if use_tfidf:
            M = _tfidf(M)
        self._M = M
        self._chroniken = chroniken
        self._werke = werke
        S = _cosine(M)
        self._S = S
        nz = (M > 0)

        # Paare erzeugen
        rows = []
        n = S.shape[0]
        for i in range(n):
            sims = S[i, i + 1 :]
            order = np.argsort(-sims)
            for idx in order[: max(topk, n)]:
                j = i + 1 + int(idx)
                sim = float(S[i, j])
                if sim < smin:
                    continue
                shared = int(np.logical_and(nz[i], nz[j]).sum())
                if shared < ms:
                    continue
                if sort_by == "combo":
                    score = sim * math.log1p(shared)
                elif sort_by == "shared":
                    score = float(shared)
                else:
                    score = sim
                rows.append((chroniken[i], chroniken[j], sim, shared, score))

        df_pairs = pd.DataFrame(
            rows, columns=["chronik_a_key", "chronik_b_key", "similarity", "shared_werke", "_score"]
        )

        # Anzeige: immer Basename ohne Pfad
        drop = self.chk_dropext.isChecked()
        df_pairs["chronik_a"] = df_pairs["chronik_a_key"].map(lambda x: _base_name(x, drop))
        df_pairs["chronik_b"] = df_pairs["chronik_b_key"].map(lambda x: _base_name(x, drop))

        # Sortierung
        if sort_by == "shared":
            df_pairs = df_pairs.sort_values(
                ["shared_werke", "similarity"], ascending=[False, False], ignore_index=True
            )
        else:
            df_pairs = df_pairs.sort_values(
                ["_score", "shared_werke"], ascending=[False, False], ignore_index=True
            )
        df_pairs = df_pairs[["chronik_a", "chronik_b", "similarity", "shared_werke", "chronik_a_key", "chronik_b_key"]]
        self.model_pairs.set_dataframe(df_pairs)
        # Interne Key-Spalten verbergen
        self._hide_internal_columns(self.tbl_pairs, ["chronik_a_key", "chronik_b_key"])

        # Nachbarn-Auswahl
        self.cb_pick.blockSignals(True)
        self.cb_pick.clear()
        for lab in self._chroniken:
            disp = _base_name(lab, drop)
            self.cb_pick.addItem(disp, userData=lab)
        last_pick = self._sim_get("last_pick", str, None)
        if last_pick in self._chroniken:
            for i in range(self.cb_pick.count()):
                if self.cb_pick.itemData(i) == last_pick:
                    self.cb_pick.setCurrentIndex(i); break
        elif self.cb_pick.count() > 0:
            self.cb_pick.setCurrentIndex(0)
        self.cb_pick.blockSignals(False)

        self._neighbors()

        # Präferenzen
        self._sim_set("chronik_col", c)
        self._sim_set("werk_col", w)
        self._sim_set("intensity_col", ic or "")
        self._sim_set("weighting", weight)
        self._sim_set("tfidf", use_tfidf)
        self._sim_set("min_shared", ms)
        self._sim_set("min_sim", smin)
        self._sim_set("topk", topk)
        self._sim_set("nei_n", int(self.nei_n.value()))
        self._sim_set("basename", True)
        self._sim_set("dropext", False)
        self._sim_set("sort_by", sort_by)

    def _neighbors(self) -> None:
        if self._S is None or not self._chroniken:
            return
        a_orig = self.cb_pick.currentData()
        if not a_orig or a_orig not in self._chroniken:
            if self.cb_pick.count() == 0:
                return
            a_orig = self.cb_pick.itemData(0)
        self._sim_set("last_pick", a_orig)
        i = self._chroniken.index(a_orig)
        sims = self._S[i, :]
        order = np.argsort(-sims)
        rows = []
        drop = self.chk_dropext.isChecked()
        for j in order:
            if j == i:
                continue
            shared = int(np.logical_and(self._M[i, :] > 0, self._M[j, :] > 0).sum())
            rows.append((_base_name(self._chroniken[j], drop), float(sims[j]), shared, self._chroniken[j]))
        df = pd.DataFrame(rows, columns=["neighbor", "similarity", "shared_werke", "neighbor_key"])
        self.model_neighbors.set_dataframe(df)
        self._hide_internal_columns(self.tbl_neighbors, ["neighbor_key"])

    def _hide_internal_columns(self, table: QtWidgets.QTableView, names: List[str]) -> None:
        df = table.model()._df
        for name in names:
            if name in df.columns:
                ix = df.columns.get_loc(name)
                table.setColumnHidden(ix, True)

    def _open_pair_details(self, index: QtCore.QModelIndex) -> None:
        if not index.isValid():
            return
        r = self.model_pairs._df.iloc[index.row()]
        a_key = str(r["chronik_a_key"]); b_key = str(r["chronik_b_key"])
        dlg = DetailsDialog(self.df, self._col_c, self._col_w, a_key, b_key, self._col_ic, self)
        dlg.exec_()

    def _open_neighbor_details(self, index: QtCore.QModelIndex) -> None:
        if not index.isValid():
            return
        a_key = self.cb_pick.currentData()
        b_key = str(self.model_neighbors._df.iloc[index.row()]["neighbor_key"])
        dlg = DetailsDialog(self.df, self._col_c, self._col_w, a_key, b_key, self._col_ic, self)
        dlg.exec_()

    def _export_pairs(self) -> None:
        if self.model_pairs._df.empty:
            return
        p, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Paare CSV", "chronik_pairs.csv", "CSV (*.csv)")
        if p:
            # Export ohne Key-Spalten
            cols = ["chronik_a", "chronik_b", "similarity", "shared_werke"]
            self.model_pairs._df[cols].to_csv(p, index=False)

    def _export_matrix(self) -> None:
        if self._S is None:
            return
        idx = [_base_name(x, self.chk_dropext.isChecked()) for x in self._chroniken]
        p, _ = QtWidgets.QFileDialog.getSaveFileName(
            self, "Matrix CSV", "chronik_similarity_matrix.csv", "CSV (*.csv)"
        )
        if p:
            pd.DataFrame(self._S, index=idx, columns=idx).to_csv(p, index=True)

    def _report(self) -> None:
        if self.model_pairs._df.empty or not get_last_csv():
            return
        p, _ = QtWidgets.QFileDialog.getSaveFileName(
            self, "HTML-Report", "chronik_report.html", "HTML (*.html)"
        )
        if not p:
            return
        pairs = self.model_pairs._df.head(200).copy()

        def esc(s: str) -> str:
            return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        rows = "\n".join(
            f"<tr><td>{esc(a)}</td><td>{esc(b)}</td><td>{sim:.3f}</td><td>{shared}</td></tr>"
            for a, b, sim, shared in pairs[["chronik_a","chronik_b","similarity","shared_werke"]].values
        )
        html = f"""<!doctype html><html><head><meta charset="utf-8"><title>Chronik-Report</title>
<style>body{{background:#0d0f12;color:#e6e6e6;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:24px}}
h1,h2{{color:#cfe2ff}} table{{border-collapse:collapse;width:100%}} th,td{{border:1px solid #2a2f3a;padding:8px}} th{{background:#1b2030}}</style>
</head><body><h1>Chronik-Ähnlichkeiten</h1><p style="opacity:.8">Quelle: {esc(get_last_csv())}</p>
<h2>Top-Paare</h2><table><thead><tr><th>chronik_a</th><th>chronik_b</th><th>similarity</th><th>shared_werke</th></tr></thead><tbody>{rows}</tbody></table></body></html>"""
        Path(p).write_text(html, encoding="utf-8")
        webbrowser.open(f"file://{Path(p).absolute()}")

# --------------------- Scan-Tab (QProcess) ---------------------

class ScanTab(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.proc: Optional[QtCore.QProcess] = None
        self._build_ui()

    def _build_ui(self) -> None:
        lay = QtWidgets.QVBoxLayout(self)
        row = QtWidgets.QHBoxLayout()
        self.le_script = QtWidgets.QLineEdit()
        self.le_script.setPlaceholderText("Pfad zu chronik-search.py …")
        self.btn_script = QtWidgets.QPushButton("Script…")
        self.le_folder = QtWidgets.QLineEdit()
        self.le_folder.setPlaceholderText("PDF-Ordner (optional)")
        self.btn_folder = QtWidgets.QPushButton("Ordner…")
        self.btn_run = QtWidgets.QPushButton("Scan starten")
        self.btn_stop = QtWidgets.QPushButton("Stop")
        for w in [
            self.le_script,
            self.btn_script,
            self.le_folder,
            self.btn_folder,
            self.btn_run,
            self.btn_stop,
        ]:
            row.addWidget(w)
        lay.addLayout(row)

        row2 = QtWidgets.QHBoxLayout()
        self.lbl_last = QtWidgets.QLabel(f"Letzte CSV: {get_last_csv() or '—'}")
        self.btn_open_session = QtWidgets.QPushButton("Session öffnen")
        self.btn_open_csv = QtWidgets.QPushButton("CSV wählen…")
        for w in [self.lbl_last, self.btn_open_session, self.btn_open_csv]:
            row2.addWidget(w)
        lay.addLayout(row2)

        self.log = QtWidgets.QPlainTextEdit()
        self.log.setReadOnly(True)
        self.log.setStyleSheet("QPlainTextEdit{background:#0b1320;color:#e6e6e6;border:1px solid #334155}")
        lay.addWidget(self.log, 1)

        self.btn_script.clicked.connect(self._choose_script)
        self.btn_folder.clicked.connect(self._choose_folder)
        self.btn_run.clicked.connect(self._run)
        self.btn_stop.clicked.connect(self._stop)
        self.btn_open_session.clicked.connect(self._open_session)
        self.btn_open_csv.clicked.connect(self._choose_csv)

        last_script = _settings().value("last_script", type=str)
        if last_script:
            self.le_script.setText(last_script)

    def _choose_script(self) -> None:
        p, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Finder-Script wählen", os.getcwd(), "Python (*.py)")
        if p:
            self.le_script.setText(p)
            s = _settings(); s.setValue("last_script", p); s.sync()

    def _choose_folder(self) -> None:
        p = QtWidgets.QFileDialog.getExistingDirectory(self, "PDF-Ordner wählen", os.getcwd())
        if p:
            self.le_folder.setText(p)

    def _append(self, s: str) -> None:
        self.log.appendPlainText(s.rstrip())
        self.log.verticalScrollBar().setValue(self.log.verticalScrollBar().maximum())

    def _run(self) -> None:
        if self.proc:
            self._stop()
        script = self.le_script.text().strip()
        if not script or not os.path.isfile(script):
            QtWidgets.QMessageBox.information(self, "Hinweis", "Bitte Finder-Script angeben.")
            return
        env = QtCore.QProcessEnvironment.systemEnvironment()
        folder = self.le_folder.text().strip()
        if folder:
            env.insert("CHRONIK_PDF_DIR", folder)
        self.proc = QtCore.QProcess(self)
        self.proc.setProcessEnvironment(env)
        self.proc.setProgram(sys.executable)
        self.proc.setArguments([script])
        self.proc.setWorkingDirectory(os.path.dirname(script))
        self.proc.readyReadStandardOutput.connect(self._on_out)
        self.proc.readyReadStandardError.connect(self._on_err)
        self.proc.finished.connect(self._on_finished)
        self._append(f"> Starte: {sys.executable} {script}")
        self.proc.start()

    def _stop(self) -> None:
        if self.proc and self.proc.state() == QtCore.QProcess.Running:
            self.proc.kill()
            self._append("> Prozess beendet.")

    def _parse_artifacts(self, line: str) -> None:
        m = re.search(r"CSV geschrieben:\s*(.+?chroniken_mentions\.csv)", line)
        if m:
            path = m.group(1).strip()
            if os.path.isfile(path):
                set_last_csv(path)
                self.lbl_last.setText(f"Letzte CSV: {path}")
        s = re.search(r"Session-Ordner:\s*(.+)$", line)
        if s:
            set_last_session(s.group(1).strip())

    def _on_out(self) -> None:
        if not self.proc:
            return
        text = bytes(self.proc.readAllStandardOutput()).decode("utf-8", errors="ignore")
        for line in text.splitlines():
            self._append(line)
            self._parse_artifacts(line)

    def _on_err(self) -> None:
        if not self.proc:
            return
        text = bytes(self.proc.readAllStandardError()).decode("utf-8", errors="ignore")
        for line in text.splitlines():
            self._append(line)
            self._parse_artifacts(line)

    def _on_finished(self) -> None:
        self._append("> Fertig.")

    def _open_session(self) -> None:
        p = get_last_session()
        if not p or not os.path.isdir(p):
            QtWidgets.QMessageBox.information(self, "Hinweis", "Keine Session bekannt.")
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(p))

    def _choose_csv(self) -> None:
        p, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Mentions-CSV", os.getcwd(), "CSV (*.csv)")
        if p:
            set_last_csv(p)
            self.lbl_last.setText(f"Letzte CSV: {p}")

# --------------------- Hauptfenster ---------------------

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Chroniken — Main GUI")
        self.resize(1400, 900)
        tabs = QtWidgets.QTabWidget()
        self.scan = ScanTab()
        self.net = NetworkTab()
        self.sim = SimilarityTab()
        tabs.addTab(self.scan, "Scan")
        tabs.addTab(self.net, "Netzwerk")
        tabs.addTab(self.sim, "Ähnlichkeit")
        self.setCentralWidget(tabs)

        last = get_last_csv()
        if last and os.path.isfile(last):
            try:
                self.net.set_csv_from_outside(last)
                df_last = read_csv_auto(last)
                if guess_column(df_last, ["pdf_file", "pdf", "file", "document"]) and guess_column(
                    df_last, ["canonical", "label", "chronik", "work"]
                ):
                    self.sim.load_csv(last)
            except Exception as ex:
                debug(f"Autoload übersprungen: {ex}")

# --------------------- main ---------------------

def main() -> None:
    debug("Starte Chronik Main GUI …")
    try:
        QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling, True)
        QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_UseHighDpiPixmaps, True)
    except Exception:
        pass
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()