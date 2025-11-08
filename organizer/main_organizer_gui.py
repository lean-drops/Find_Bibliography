#!/usr/bin/env python3
"""
main_organizer_gui.py
Zentrale PySide6-GUI zum Verknüpfen von:
  • Scan: chroniken_library-Finder (externes Script) ausführen, Log anzeigen
  • Netzwerk: bipartites PDF↔Chronik-Netz visualisieren
  • Ähnlichkeit: Chronik-Ähnlichkeiten aus Mentions-CSV analysieren

Voraussetzungen:
  pip install PySide6 pandas numpy networkx
  optional: pip install pymupdf pytesseract pillow   # nur für dein Finder-Script

Start:
  python main_organizer_gui.py

Hinweise:
  - Letzte CSV/Session wird gemerkt (QSettings).
  - Netzwerk und Analyse verwenden die zuletzt aktive CSV aus dem Scan-Tab oder manuell gewählte CSV.
"""

from __future__ import annotations

import json
import math
import os
import re
import sys
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import networkx as nx
from PySide6 import QtCore, QtGui, QtWidgets
from PySide6.QtCore import Qt


# --------------------- Utility / Persistenz ---------------------

def debug(msg: str) -> None:
    print(f"[DEBUG] {msg}", flush=True)


ORG = "chroniken_suite"
APP = "chronik_main_gui"


def settings() -> QtCore.QSettings:
    QtCore.QCoreApplication.setOrganizationName(ORG)
    QtCore.QCoreApplication.setApplicationName(APP)
    return QtCore.QSettings()


def get_last_csv() -> str:
    return settings().value("last_csv", type=str) or ""


def set_last_csv(path: str) -> None:
    s = settings(); s.setValue("last_csv", path); s.sync(); debug(f"last_csv={path}")


def get_last_session() -> str:
    return settings().value("last_session", type=str) or ""


def set_last_session(path: str) -> None:
    s = settings(); s.setValue("last_session", path); s.sync(); debug(f"last_session={path}")


def read_csv_auto(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"CSV nicht gefunden: {path}")
    try:
        return pd.read_csv(path, sep=';', engine='python')
    except Exception:
        return pd.read_csv(path, sep=None, engine='python')


def guess_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower:
            return lower[c]
    return None


# --------------------- Netzwerk: Graph + View ---------------------

def build_bipartite_from_mentions(df: pd.DataFrame) -> nx.Graph:
    doc_col = guess_column(df, ["pdf_file", "pdf", "file", "document", "filename", "source", "label"])
    work_col = guess_column(df, ["canonical", "label", "chronik", "work", "title", "name"])
    if not doc_col or not work_col:
        raise KeyError("Spalten nicht erkannt. Erwartet z.B. pdf_file und canonical/label.")
    d = df[[doc_col, work_col]].dropna()
    d[doc_col] = d[doc_col].astype(str).str.strip()
    d[work_col] = d[work_col].astype(str).str.strip()

    pairs = d.groupby([doc_col, work_col]).size().reset_index(name="w")
    G = nx.Graph(name="PDF↔Chronik")
    for c, cnt in pairs.groupby(work_col)[doc_col].nunique().items():
        G.add_node(f"C|{c}", role="chronik", label=c, mentions=int(cnt), bipartite=0)
    for p, cnt in pairs.groupby(doc_col)[work_col].nunique().items():
        base = os.path.basename(p) or p
        G.add_node(f"P|{base}", role="pdf", label=base, items=int(cnt), bipartite=1)
    for _, r in pairs.iterrows():
        G.add_edge(f"P|{os.path.basename(r[doc_col])}", f"C|{r[work_col]}", weight=float(r["w"]))
    return G


def _mass(G: nx.Graph, n: str) -> float:
    d = G.nodes[n]
    return 1.0 + math.sqrt(max(1, int(d.get("mentions", d.get("items", 1)))))


def _layout_bipartite(G: nx.Graph) -> Dict[str, Tuple[float, float]]:
    left = [n for n, d in G.nodes(data=True) if d.get("role") == "pdf"]
    right = [n for n, d in G.nodes(data=True) if d.get("role") == "chronik"]
    pos: Dict[str, Tuple[float, float]] = {}
    for side, x in ((left, -1.0), (right, 1.0)):
        n = max(1, len(side))
        for i, node in enumerate(sorted(side)):
            y = (i - (n - 1) / 2.0) * 0.6
            pos[node] = (x, y)
    return pos


def compute_layout(G: nx.Graph, mode: str) -> Dict[str, Tuple[float, float]]:
    if mode == "bipartite":
        pos = _layout_bipartite(G)
    elif mode == "kamada_kawai":
        pos = nx.kamada_kawai_layout(G, weight="weight")
    else:
        pos = nx.spring_layout(G, seed=42, weight="weight", iterations=300)
    return {n: (float(x) * 280.0, float(y) * 280.0) for n, (x, y) in pos.items()}


class GraphView(QtWidgets.QGraphicsView):
    def __init__(self):
        super().__init__()
        self.setRenderHint(QtGui.QPainter.Antialiasing, True)
        self.setDragMode(QtWidgets.QGraphicsView.ScrollHandDrag)
        self.setViewportUpdateMode(QtWidgets.QGraphicsView.SmartViewportUpdate)
        self.setTransformationAnchor(QtWidgets.QGraphicsView.AnchorUnderMouse)
        self._nodes_by_label: Dict[str, QtWidgets.QGraphicsItem] = {}
        self._theme_dark()

    def _theme_dark(self):
        self.setStyleSheet("QGraphicsView{background:#0d0f12;border:0}")

    def wheelEvent(self, e: QtGui.QWheelEvent) -> None:
        self.scale(1.15 if e.angleDelta().y() > 0 else 1/1.15, 1.15 if e.angleDelta().y() > 0 else 1/1.15)

    def set_scene_for_graph(self, G: nx.Graph, pos: Dict[str, Tuple[float, float]],
                            min_w: int, show_labels: bool) -> None:
        scene = QtWidgets.QGraphicsScene()
        self._nodes_by_label.clear()
        # nodes
        for n, d in G.nodes(data=True):
            role = d.get("role"); label = d.get("label", n); x, y = pos.get(n, (0.0, 0.0))
            if role == "chronik":
                r = 8 + 2.5 * math.sqrt(max(1, int(d.get("mentions", 1))))
                item = QtWidgets.QGraphicsEllipseItem(-r, -r, 2*r, 2*r)
                item.setBrush(QtGui.QColor("#1fb6ff"))
                pen = QtGui.QPen(QtGui.QColor("#083344")); pen.setWidth(1); pen.setCosmetic(True); item.setPen(pen)
            else:
                r = 10 + 3.0 * math.sqrt(max(1, int(d.get("items", 1))))
                poly = QtGui.QPolygonF([QtCore.QPointF(0, -r), QtCore.QPointF(-0.866*r, 0.5*r),
                                        QtCore.QPointF(0.866*r, 0.5*r)])
                item = QtWidgets.QGraphicsPolygonItem(poly)
                item.setBrush(QtGui.QColor("#fbbf24"))
                pen = QtGui.QPen(QtGui.QColor("#3a2c0b")); pen.setWidth(1); pen.setCosmetic(True); item.setPen(pen)
            item.setPos(x, y)
            item.setFlag(QtWidgets.QGraphicsItem.ItemIsMovable, True)
            item.setToolTip(f"{'Chronik' if role=='chronik' else 'PDF'}: {label}")
            scene.addItem(item)
            if show_labels:
                t = scene.addSimpleText(label)
                t.setBrush(QtGui.QColor("#e6e6e6"))
                t.setFlag(QtWidgets.QGraphicsItem.ItemIgnoresTransformations, True)
                t.setPos(x + r + 6, y - 8)
                self._nodes_by_label[label.lower()] = t
            self._nodes_by_label[label.lower()] = item
        # edges
        for u, v, ed in G.edges(data=True):
            w = float(ed.get("weight", 1.0))
            if w < float(min_w):
                continue
            x1, y1 = pos.get(u, (0, 0)); x2, y2 = pos.get(v, (0, 0))
            path = QtGui.QPainterPath(QtCore.QPointF(x1, y1))
            dx = x2 - x1; k = 0.15*abs(dx)
            path.cubicTo(QtCore.QPointF(x1+0.25*dx, y1-k),
                         QtCore.QPointF(x2-0.25*dx, y2+k),
                         QtCore.QPointF(x2, y2))
            p = QtWidgets.QGraphicsPathItem(path)
            alpha = max(60, min(220, int(60 + 50 * math.log1p(w))))
            pen = QtGui.QPen(QtGui.QColor(200, 200, 220, alpha)); pen.setWidthF(max(1.0, 0.6+math.sqrt(w))); pen.setCosmetic(True)
            p.setPen(pen); p.setZValue(-1)
            scene.addItem(p)
        self.setScene(scene)
        self.fitInView(scene.itemsBoundingRect(), Qt.KeepAspectRatio)

    def highlight(self, term: str) -> int:
        term = term.strip().lower()
        hits = 0
        for key, item in self._nodes_by_label.items():
            on = term and term in key
            if isinstance(item, (QtWidgets.QGraphicsEllipseItem, QtWidgets.QGraphicsPolygonItem)):
                pen = item.pen(); pen.setWidth(3 if on else 1); pen.setColor(QtGui.QColor("#ef4444") if on else pen.color()); item.setPen(pen)
                hits += int(on)
            elif isinstance(item, QtWidgets.QGraphicsSimpleTextItem):
                item.setBrush(QtGui.QColor("#ef4444") if on else QtGui.QColor("#e6e6e6"))
        return hits


class NetworkTab(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.df: Optional[pd.DataFrame] = None
        self.G: Optional[nx.Graph] = None
        self.pos: Dict[str, Tuple[float, float]] = {}
        self._build_ui()

    def _build_ui(self):
        lay = QtWidgets.QVBoxLayout(self)
        controls = QtWidgets.QHBoxLayout()
        self.btn_load = QtWidgets.QPushButton("CSV laden…")
        self.cmb_layout = QtWidgets.QComboBox(); self.cmb_layout.addItems(["bipartite", "spring", "kamada_kawai"])
        self.spin_w = QtWidgets.QSpinBox(); self.spin_w.setRange(1, 10_000); self.spin_w.setValue(1)
        self.chk_labels = QtWidgets.QCheckBox("Labels"); self.chk_labels.setChecked(True)
        self.edit_search = QtWidgets.QLineEdit(); self.edit_search.setPlaceholderText("Knoten suchen…")
        self.btn_search = QtWidgets.QPushButton("Suchen")
        self.btn_export = QtWidgets.QPushButton("PNG exportieren")
        for w in [self.btn_load, QtWidgets.QLabel("Layout:"), self.cmb_layout,
                  QtWidgets.QLabel("min Kantengew.:"), self.spin_w, self.chk_labels,
                  self.edit_search, self.btn_search, self.btn_export]:
            controls.addWidget(w)
        lay.addLayout(controls)
        self.view = GraphView()
        lay.addWidget(self.view, 1)
        # signals
        self.btn_load.clicked.connect(self._choose_csv)
        self.cmb_layout.currentTextChanged.connect(lambda _: self._rebuild())
        self.spin_w.valueChanged.connect(lambda _: self._rebuild())
        self.chk_labels.toggled.connect(lambda _: self._rebuild())
        self.btn_search.clicked.connect(self._do_search)
        self.btn_export.clicked.connect(self._export_png)

    def _choose_csv(self):
        p, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Mentions-CSV", os.getcwd(), "CSV (*.csv)")
        if p:
            self.load_csv(p)

    def load_csv(self, path: str):
        self.df = read_csv_auto(path)
        set_last_csv(path)
        self._rebuild()

    def set_csv_from_outside(self, path: str):
        if path and os.path.isfile(path):
            self.load_csv(path)

    def _rebuild(self):
        if self.df is None:
            return
        self.G = build_bipartite_from_mentions(self.df)
        self.pos = compute_layout(self.G, self.cmb_layout.currentText())
        self.view.set_scene_for_graph(self.G, self.pos, self.spin_w.value(), self.chk_labels.isChecked())

    def _do_search(self):
        hits = self.view.highlight(self.edit_search.text())
        QtWidgets.QToolTip.showText(self.mapToGlobal(self.edit_search.pos()), f"Treffer: {hits}", self.edit_search)

    def _export_png(self):
        if not get_last_csv():
            QtWidgets.QMessageBox.information(self, "Hinweis", "CSV zuerst laden.")
            return
        base = os.path.splitext(os.path.basename(get_last_csv()))[0]
        out = os.path.join(os.path.dirname(get_last_csv()), f"{base}_graph.png")
        scene = self.view.scene()
        if not scene:
            return
        rect = scene.itemsBoundingRect().adjusted(-20, -20, 20, 20)
        img = QtGui.QImage(int(rect.width()), int(rect.height()), QtGui.QImage.Format_ARGB32)
        img.fill(QtGui.QColor("#0d0f12"))
        p = QtGui.QPainter(img); p.setRenderHint(QtGui.QPainter.Antialiasing, True); scene.render(p, QtCore.QRectF(img.rect()), rect); p.end()
        img.save(out); debug(f"PNG exportiert: {out}")


# --------------------- Ähnlichkeit: Engine + Tab ---------------------

def tf_idf_weight(mat_counts: np.ndarray) -> np.ndarray:
    row_sums = mat_counts.sum(axis=1, keepdims=True); row_sums[row_sums == 0] = 1.0
    tf = mat_counts / row_sums
    df = (mat_counts > 0).sum(axis=0, keepdims=True)
    n = mat_counts.shape[0]
    idf = np.log((1.0 + n) / (1.0 + df)) + 1.0
    return tf * idf


def cosine_similarity(mat: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(mat, axis=1, keepdims=True); norms[norms == 0] = 1.0
    m = mat / norms
    return m @ m.T


class SimilarityTab(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.df: Optional[pd.DataFrame] = None
        self._chroniken: List[str] = []
        self._werke: List[str] = []
        self._mat: Optional[np.ndarray] = None
        self._S: Optional[np.ndarray] = None
        self._build_ui()

    def _build_ui(self):
        lay = QtWidgets.QVBoxLayout(self)
        row = QtWidgets.QHBoxLayout()
        self.btn_load = QtWidgets.QPushButton("CSV laden…"); row.addWidget(self.btn_load)
        self.cb_chronik = QtWidgets.QComboBox(); self.cb_werk = QtWidgets.QComboBox(); self.cb_int = QtWidgets.QComboBox(); self.cb_int.addItem("— keine —")
        for w in [QtWidgets.QLabel("Chronik:"), self.cb_chronik, QtWidgets.QLabel("Werk/PDF:"), self.cb_werk, QtWidgets.QLabel("Intensität:"), self.cb_int]:
            row.addWidget(w)
        lay.addLayout(row)

        opts = QtWidgets.QHBoxLayout()
        self.cmb_weight = QtWidgets.QComboBox(); self.cmb_weight.addItems(["count", "intensity", "binary"])
        self.chk_tfidf = QtWidgets.QCheckBox("TF-IDF")
        self.spin_shared = QtWidgets.QSpinBox(); self.spin_shared.setRange(0, 10000); self.spin_shared.setValue(1)
        self.dsb_min_sim = QtWidgets.QDoubleSpinBox(); self.dsb_min_sim.setRange(0.0, 1.0); self.dsb_min_sim.setValue(0.2); self.dsb_min_sim.setSingleStep(0.01)
        self.spin_topk = QtWidgets.QSpinBox(); self.spin_topk.setRange(1, 10000); self.spin_topk.setValue(10)
        for w in [QtWidgets.QLabel("Gewichtung:"), self.cmb_weight, self.chk_tfidf,
                  QtWidgets.QLabel("min gemeinsame Werke:"), self.spin_shared,
                  QtWidgets.QLabel("min Ähnlichkeit:"), self.dsb_min_sim,
                  QtWidgets.QLabel("Top-K je Chronik:"), self.spin_topk]:
            opts.addWidget(w)
        lay.addLayout(opts)

        btns = QtWidgets.QHBoxLayout()
        self.btn_run = QtWidgets.QPushButton("Analyse"); self.btn_export_pairs = QtWidgets.QPushButton("Paare CSV"); self.btn_export_matrix = QtWidgets.QPushButton("Matrix CSV"); self.btn_report = QtWidgets.QPushButton("HTML-Report")
        for b in [self.btn_run, self.btn_export_pairs, self.btn_export_matrix, self.btn_report]:
            btns.addWidget(b)
        btns.addStretch(1); lay.addLayout(btns)

        tabs = QtWidgets.QTabWidget()
        self.tbl_pairs = QtWidgets.QTableView(); self.model_pairs = PandasModel(pd.DataFrame(columns=["chronik_a","chronik_b","similarity","shared_werke"])); self.tbl_pairs.setModel(self.model_pairs)
        w1 = QtWidgets.QWidget(); v1 = QtWidgets.QVBoxLayout(w1); v1.addWidget(self.tbl_pairs); tabs.addTab(w1, "Top-Paare")

        w2 = QtWidgets.QWidget(); v2 = QtWidgets.QVBoxLayout(w2)
        self.cb_pick = QtWidgets.QComboBox(); self.spin_neighbors = QtWidgets.QSpinBox(); self.spin_neighbors.setRange(1, 1000); self.spin_neighbors.setValue(10)
        top = QtWidgets.QHBoxLayout(); top.addWidget(QtWidgets.QLabel("Chronik:")); top.addWidget(self.cb_pick, 1); top.addWidget(QtWidgets.QLabel("Top-N:")); top.addWidget(self.spin_neighbors); v2.addLayout(top)
        self.tbl_neighbors = QtWidgets.QTableView(); self.model_neighbors = PandasModel(pd.DataFrame(columns=["neighbor","similarity","shared_werke"])); self.tbl_neighbors.setModel(self.model_neighbors); v2.addWidget(self.tbl_neighbors)
        tabs.addTab(w2, "Nachbarn je Chronik")
        lay.addWidget(tabs, 1)

        # Signals
        self.btn_load.clicked.connect(self._choose_csv)
        self.btn_run.clicked.connect(self._run)
        self.btn_export_pairs.clicked.connect(self._export_pairs)
        self.btn_export_matrix.clicked.connect(self._export_matrix)
        self.btn_report.clicked.connect(self._report)

        self._apply_glass_style(self)

    def _apply_glass_style(self, w: QtWidgets.QWidget):
        QtWidgets.QApplication.setStyle("Fusion")
        pal = QtGui.QPalette()
        pal.setColor(QtGui.QPalette.Window, QtGui.QColor(18, 18, 18))
        pal.setColor(QtGui.QPalette.Base, QtGui.QColor(28, 28, 28))
        pal.setColor(QtGui.QPalette.Text, QtGui.QColor(230, 230, 230))
        w.setPalette(pal)
        w.setStyleSheet("""
            QWidget { background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #13151a, stop:1 #0d0f12); color:#e6e6e6; }
            QLineEdit,QComboBox,QSpinBox,QDoubleSpinBox { background:#1b2030; border:1px solid #34405a; border-radius:6px; padding:4px 6px;}
            QPushButton { background:#273449; border:1px solid #3b4a66; border-radius:8px; padding:6px 12px;}
            QPushButton:hover { background:#31425a; }
            QTableView { background:#151a24; gridline-color:#3a455c; }
        """)

    def _choose_csv(self):
        p, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Mentions-CSV", os.getcwd(), "CSV (*.csv)")
        if p:
            self.load_csv(p)

    def load_csv(self, path: str):
        self.df = read_csv_auto(path); set_last_csv(path)
        cols = list(self.df.columns)
        self.cb_chronik.clear(); self.cb_werk.clear(); self.cb_int.clear(); self.cb_int.addItem("— keine —")
        self.cb_chronik.addItems(cols); self.cb_werk.addItems(cols); self.cb_int.addItems(cols)
        c_guess = guess_column(self.df, ["canonical","label","chronik","work"])
        w_guess = guess_column(self.df, ["pdf_file","pdf","file","document","label"])
        if c_guess: self.cb_chronik.setCurrentIndex(self.cb_chronik.findText(c_guess))
        if w_guess: self.cb_werk.setCurrentIndex(self.cb_werk.findText(w_guess))

    def _run(self):
        if self.df is None:
            QtWidgets.QMessageBox.information(self, "Hinweis", "CSV zuerst laden.")
            return
        chronik_col = self.cb_chronik.currentText()
        werk_col = self.cb_werk.currentText()
        intensity_col = self.cb_int.currentText(); intensity_col = None if intensity_col == "— keine —" else intensity_col
        weighting = self.cmb_weight.currentText(); use_tfidf = self.chk_tfidf.isChecked()
        min_shared = int(self.spin_shared.value()); min_sim = float(self.dsb_min_sim.value()); topk = int(self.spin_topk.value())

        dfw = self.df[[chronik_col, werk_col] + ([intensity_col] if intensity_col else [])].copy()
        if weighting == "binary":
            agg = dfw.groupby([chronik_col, werk_col]).size().reset_index(name="w"); agg["w"] = 1.0
        elif weighting == "intensity" and intensity_col:
            dfw[intensity_col] = pd.to_numeric(dfw[intensity_col], errors="coerce").fillna(0.0)
            agg = dfw.groupby([chronik_col, werk_col])[intensity_col].sum().reset_index(name="w")
        else:
            agg = dfw.groupby([chronik_col, werk_col]).size().reset_index(name="w")

        chroniken = sorted(agg[chronik_col].unique()); werke = sorted(agg[werk_col].unique())
        ci = {c:i for i,c in enumerate(chroniken)}; wi = {w:i for i,w in enumerate(werke)}
        M = np.zeros((len(chroniken), len(werke)), dtype=float)
        for _, r in agg.iterrows(): M[ci[r[chronik_col]], wi[r[werk_col]]] = float(r["w"])
        if use_tfidf: M = tf_idf_weight(M)
        self._mat = M; self._chroniken = chroniken; self._werke = werke
        S = cosine_similarity(M); self._S = S
        nz = (M>0)
        rows = []
        n = S.shape[0]
        for i in range(n):
            sims = S[i, i+1:]
            order = np.argsort(-sims)
            for idx in order[:max(topk, n)]:
                j = i + 1 + int(idx); sim = float(S[i, j])
                if sim < min_sim: continue
                shared = int(np.logical_and(nz[i], nz[j]).sum())
                if shared < min_shared: continue
                rows.append((chroniken[i], chroniken[j], sim, shared))
        df_pairs = pd.DataFrame(rows, columns=["chronik_a","chronik_b","similarity","shared_werke"]).sort_values(["similarity","shared_werke"], ascending=[False,False], ignore_index=True)
        self.model_pairs.set_dataframe(df_pairs)
        self.cb_pick.clear(); self.cb_pick.addItems(self._chroniken)
        self._refresh_neighbors()

        self.cb_pick.currentTextChanged.connect(lambda _: self._refresh_neighbors())
        self.spin_neighbors.valueChanged.connect(lambda _: self._refresh_neighbors())

    def _refresh_neighbors(self):
        if self._S is None or not self._chroniken: return
        a = self.cb_pick.currentText(); i = self._chroniken.index(a)
        sims = self._S[i, :]; order = np.argsort(-sims)
        rows = []
        for j in order:
            if j == i: continue
            shared = int(np.logical_and(self._mat[i,:]>0, self._mat[j,:]>0).sum())
            rows.append((self._chroniken[j], float(sims[j]), shared))
        df = pd.DataFrame(rows[:int(self.spin_neighbors.value())], columns=["neighbor","similarity","shared_werke"])
        self.model_neighbors.set_dataframe(df)

    def _export_pairs(self):
        if self.model_pairs._df.empty: return
        p, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Paare CSV", "chronik_pairs.csv", "CSV (*.csv)")
        if p: self.model_pairs._df.to_csv(p, index=False)

    def _export_matrix(self):
        if self._S is None: return
        idx = self._chroniken
        p, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Matrix CSV", "chronik_similarity_matrix.csv", "CSV (*.csv)")
        if p: pd.DataFrame(self._S, index=idx, columns=idx).to_csv(p, index=True)

    def _report(self):
        if self.model_pairs._df.empty or not get_last_csv(): return
        p, _ = QtWidgets.QFileDialog.getSaveFileName(self, "HTML-Report", "chronik_report.html", "HTML (*.html)")
        if not p: return
        pairs = self.model_pairs._df.copy().head(200)

        def esc(s: str) -> str: return (s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;"))
        rows = "\n".join(f"<tr><td>{esc(a)}</td><td>{esc(b)}</td><td>{sim:.3f}</td><td>{shared}</td></tr>"
                         for a,b,sim,shared in pairs[["chronik_a","chronik_b","similarity","shared_werke"]].values)
        html = f"""<!doctype html><html><head><meta charset="utf-8"><title>Chronik-Report</title>
<style>body{{background:#0d0f12;color:#e6e6e6;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:24px}}
h1,h2{{color:#cfe2ff}} table{{border-collapse:collapse;width:100%}} th,td{{border:1px solid #2a2f3a;padding:8px}} th{{background:#1b2030}}
</style></head><body>
<h1>Chronik-Ähnlichkeiten</h1><p style="opacity:.8">Quelle: {esc(get_last_csv())}</p>
<h2>Top-Paare</h2><table><thead><tr><th>chronik_a</th><th>chronik_b</th><th>similarity</th><th>shared_werke</th></tr></thead>
<tbody>{rows}</tbody></table></body></html>"""
        Path(p).write_text(html, encoding="utf-8"); webbrowser.open(f"file://{Path(p).absolute()}")


class PandasModel(QtCore.QAbstractTableModel):
    def __init__(self, df: pd.DataFrame): super().__init__(); self._df = df.copy()
    def rowCount(self, parent=QtCore.QModelIndex()): return len(self._df)
    def columnCount(self, parent=QtCore.QModelIndex()): return len(self._df.columns)
    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or role not in (Qt.DisplayRole, Qt.ToolTipRole): return None
        v = self._df.iat[index.row(), index.column()]; return "" if pd.isna(v) else str(v)
    def headerData(self, s, orient, role=Qt.DisplayRole):
        if role != Qt.DisplayRole: return None
        return str(self._df.columns[s]) if orient == Qt.Horizontal else str(s)
    def set_dataframe(self, df: pd.DataFrame):
        self.beginResetModel(); self._df = df.copy(); self.endResetModel()


# --------------------- Scan-Tab (QProcess) ---------------------

class ScanTab(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.proc: Optional[QtCore.QProcess] = None
        self._build_ui()

    def _build_ui(self):
        lay = QtWidgets.QVBoxLayout(self)
        top = QtWidgets.QHBoxLayout()
        self.le_script = QtWidgets.QLineEdit()
        self.le_script.setPlaceholderText("Pfad zu chronik-search.py / chroniken_Finder.py …")
        self.btn_script = QtWidgets.QPushButton("Script…")
        self.le_folder = QtWidgets.QLineEdit()
        self.le_folder.setPlaceholderText("PDF-Ordner (optional; sonst Script-Default)")
        self.btn_folder = QtWidgets.QPushButton("Ordner…")
        self.btn_run = QtWidgets.QPushButton("Scan starten")
        self.btn_stop = QtWidgets.QPushButton("Stop")
        for w in [self.le_script, self.btn_script, self.le_folder, self.btn_folder, self.btn_run, self.btn_stop]:
            top.addWidget(w)
        lay.addLayout(top)
        mid = QtWidgets.QHBoxLayout()
        self.lbl_last = QtWidgets.QLabel(f"Letzte CSV: {get_last_csv() or '—'}")
        self.btn_open_session = QtWidgets.QPushButton("Session öffnen"); self.btn_open_csv = QtWidgets.QPushButton("CSV wählen…")
        for w in [self.lbl_last, self.btn_open_session, self.btn_open_csv]: mid.addWidget(w)
        lay.addLayout(mid)
        self.log = QtWidgets.QPlainTextEdit(); self.log.setReadOnly(True); self.log.setStyleSheet("QPlainTextEdit{background:#0b1320;color:#e6e6e6;border:1px solid #334155}")
        lay.addWidget(self.log, 1)

        self.btn_script.clicked.connect(self._choose_script)
        self.btn_folder.clicked.connect(self._choose_folder)
        self.btn_run.clicked.connect(self._run)
        self.btn_stop.clicked.connect(self._stop)
        self.btn_open_session.clicked.connect(self._open_session)
        self.btn_open_csv.clicked.connect(self._choose_csv)

        # preload last script if saved
        self.le_script.setText(settings().value("last_script", type=str) or "")

    def _choose_script(self):
        p, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Finder-Script wählen", os.getcwd(), "Python (*.py)")
        if p:
            self.le_script.setText(p); settings().setValue("last_script", p); settings().sync()

    def _choose_folder(self):
        p = QtWidgets.QFileDialog.getExistingDirectory(self, "PDF-Ordner wählen", os.getcwd())
        if p: self.le_folder.setText(p)

    def _append(self, s: str):
        self.log.appendPlainText(s.rstrip())
        self.log.verticalScrollBar().setValue(self.log.verticalScrollBar().maximum())

    def _run(self):
        if self.proc: self._stop()
        script = self.le_script.text().strip()
        if not script or not os.path.isfile(script):
            QtWidgets.QMessageBox.information(self, "Hinweis", "Bitte Finder-Script angeben.")
            return
        py = sys.executable
        args = [script]
        folder = self.le_folder.text().strip()
        env = QtCore.QProcessEnvironment.systemEnvironment()
        if folder: env.insert("CHRONIK_PDF_DIR", folder)  # falls dein Script env liest
        self.proc = QtCore.QProcess(self)
        self.proc.setProcessEnvironment(env)
        self.proc.setProgram(py); self.proc.setArguments(args)
        self.proc.setWorkingDirectory(os.path.dirname(script))
        self.proc.readyReadStandardOutput.connect(self._on_out)
        self.proc.readyReadStandardError.connect(self._on_err)
        self.proc.finished.connect(self._on_finished)
        self._append(f"> Starte: {py} {script}")
        self.proc.start()

    def _stop(self):
        if self.proc and self.proc.state() == QtCore.QProcess.Running:
            self.proc.kill(); self._append("> Prozess beendet.")

    def _parse_for_artifacts(self, line: str):
        m = re.search(r"CSV geschrieben:\s*(.+?chroniken_mentions\.csv)", line)
        if m:
            path = m.group(1).strip()
            if os.path.isfile(path):
                set_last_csv(path); self.lbl_last.setText(f"Letzte CSV: {path}")
        s = re.search(r"Session-Ordner:\s*(.+)$", line)
        if s:
            p = s.group(1).strip(); set_last_session(p)

    def _on_out(self):
        if not self.proc: return
        text = bytes(self.proc.readAllStandardOutput()).decode("utf-8", errors="ignore")
        for line in text.splitlines():
            self._append(line); self._parse_for_artifacts(line)

    def _on_err(self):
        if not self.proc: return
        text = bytes(self.proc.readAllStandardError()).decode("utf-8", errors="ignore")
        for line in text.splitlines():
            self._append(line); self._parse_for_artifacts(line)

    def _on_finished(self):
        self._append("> Fertig.")

    def _open_session(self):
        path = get_last_session()
        if not path or not os.path.isdir(path):
            QtWidgets.QMessageBox.information(self, "Hinweis", "Keine Session bekannt.")
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(path))

    def _choose_csv(self):
        p, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Mentions-CSV", os.getcwd(), "CSV (*.csv)")
        if p:
            set_last_csv(p); self.lbl_last.setText(f"Letzte CSV: {p}")


# --------------------- Hauptfenster ---------------------

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Chroniken — Main GUI")
        self.resize(1400, 900)
        tabs = QtWidgets.QTabWidget()
        self.scan_tab = ScanTab()
        self.net_tab = NetworkTab()
        self.sim_tab = SimilarityTab()
        tabs.addTab(self.scan_tab, "Scan")
        tabs.addTab(self.net_tab, "Netzwerk")
        tabs.addTab(self.sim_tab, "Ähnlichkeit")
        self.setCentralWidget(tabs)

        bar = self.menuBar().addMenu("Datei")
        act_load_csv = bar.addAction("Letzte CSV laden")
        act_load_csv.triggered.connect(self._load_last_into_tabs)

        # Auto-vorbelegen, falls vorhanden
        last = get_last_csv()
        if last and os.path.isfile(last):
            self.net_tab.set_csv_from_outside(last)
            self.sim_tab.load_csv(last)

    def _load_last_into_tabs(self):
        last = get_last_csv()
        if not last or not os.path.isfile(last):
            QtWidgets.QMessageBox.information(self, "Hinweis", "Keine zuletzt genutzte CSV gefunden.")
            return
        self.net_tab.set_csv_from_outside(last)
        self.sim_tab.load_csv(last)


# --------------------- main ---------------------

def main() -> None:
    debug("Starte Chronik Main GUI …")
    QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling, True)
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()