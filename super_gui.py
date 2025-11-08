#!/usr/bin/env python3
"""
super_gui.py — Ein Qt-Viewer, der das Autoren-Netz (aus authors_nodes/edges.csv
oder via authors_search.run_on_folder) und das Chronik-Netz (aus Mentions-CSV
via organizer.chronik.network) in EINER interaktiven Ansicht zusammenführt.

Voraussetzungen:
  pip install pandas networkx PyQt5 matplotlib pymupdf
  optional: pytesseract pillow

Start:
  python super_gui.py

Features:
  • Re-Use statt Duplikate: nutzt Funktionen/Klassen aus organizer.authors.* und organizer.chronik.network
  • Lädt Autoren-CSV-Ordner oder startet Autoren-Pipeline und übernimmt deren Session-Ergebnis
  • Lädt Chronik-Mentions-CSV und baut bipartites Netz per network.py
  • Kombiniert beide Layer in einem QGraphicsView mit Hover-Highlight, Sticky-Selection, Zoom/Pan
  • Layer-Schalter, Label-Schalter, Min-Gewicht pro Layer, Layoutwahl
  • Export PNG; optional Autoren-HTML-Report via authors_utils.write_html_report

Hinweise:
  • Robustere CSV-Lader: Semikolon oder Auto-Sniff
  • Import-Fix für authors_search: aliasiert organizer.authors.authors_utils → 'authors_utils',
    damit top-level-Imports in authors_search funktionieren.
"""

from __future__ import annotations

import os
import sys
import inspect
import importlib
from dataclasses import dataclass
from typing import Optional, Dict, Tuple, List, Any

import pandas as pd
import networkx as nx

# --- PyQt5 (Design wie network.py) ---
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import Qt

# ---- Projektpfade ----
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# ---- Chronik: network.py importieren und wiederverwenden ----
# Erwartete öffentliche Symbole:
#   Theme, GraphCanvas, EdgeItem, ChronikNode, WorkNode, compute_layout,
#   load_mentions_csv, resolve_columns, build_bipartite_graph
try:
    from organizer.chronik.network import (
        Theme, GraphCanvas as C_GraphCanvas, EdgeItem as C_EdgeItem,
        ChronikNode as C_ChronikNode, WorkNode as C_WorkNode,
        compute_layout as chr_compute_layout,
        load_mentions_csv as chr_load_mentions_csv,
        resolve_columns as chr_resolve_columns,
        build_bipartite_graph as chr_build_graph,
    )
except Exception as e:
    raise RuntimeError("Konnte organizer.chronik.network nicht importieren. Prüfe Projektstruktur.") from e

# ---- Autoren: Utils + Pipeline behutsam importieren ----
def _import_authors_modules():
    """Sorgt dafür, dass 'authors_search' seine Top-Level-Imports findet."""
    au_mod = None
    try:
        au_mod = importlib.import_module("organizer.authors.authors_utils")
        # Alias, damit 'from authors_utils import ...' in authors_search klappt:
        sys.modules.setdefault("authors_utils", au_mod)
    except Exception as e:
        print(f"[DEBUG] authors_utils-Importwarnung: {e}", flush=True)
    run_on_folder = None
    try:
        mod = importlib.import_module("organizer.authors.authors_search")
        run_on_folder = getattr(mod, "run_on_folder", None)
        if run_on_folder:
            print("[DEBUG] authors_search.run_on_folder importiert", flush=True)
    except Exception as e:
        print(f"[DEBUG] authors_search nicht verfügbar: {e}", flush=True)
    return au_mod, run_on_folder

AU_MOD, RUN_ON_FOLDER = _import_authors_modules()

# ---- Datenträger für geladene DFs ----
@dataclass
class DataBundle:
    nodes: Optional[pd.DataFrame] = None
    edges: Optional[pd.DataFrame] = None
    source_dir: Optional[str] = None

# ---- CSV Utilities (robust) ----
def read_csv_smart(path: str, prefer_semicolon: bool = True) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    err: Optional[Exception] = None
    if prefer_semicolon:
        try:
            return pd.read_csv(path, sep=";", engine="python")
        except Exception as e:
            err = e
    try:
        return pd.read_csv(path, sep=None, engine="python")
    except Exception:
        if err:
            raise err
        raise

# ---- Autoren-CSV Laden (nodes/edges) ----
def load_authors_session(directory: str) -> DataBundle:
    nodes = os.path.join(directory, "authors_nodes.csv")
    edges = os.path.join(directory, "authors_edges.csv")
    if not (os.path.exists(nodes) and os.path.exists(edges)):
        raise FileNotFoundError(f"Fehlt: {nodes} oder {edges}")
    df_nodes = read_csv_smart(nodes, prefer_semicolon=True)
    df_edges = read_csv_smart(edges, prefer_semicolon=True)
    return DataBundle(nodes=df_nodes, edges=df_edges, source_dir=directory)

# ---- Autoren → Graph ----
def authors_to_graph(nodes: pd.DataFrame, edges: pd.DataFrame) -> nx.Graph:
    G = nx.Graph(name="authors")
    # ID + Label ermitteln
    ncols = {c.lower(): c for c in nodes.columns}
    id_col = ncols.get("author_id") or ncols.get("id") or ncols.get("name") or ncols.get("label")
    label_col = ncols.get("display") or ncols.get("label") or ncols.get("name") or id_col
    if not id_col:
        raise KeyError("Autoren-Nodes: keine ID-Spalte gefunden.")
    for _, r in nodes.iterrows():
        nid = f"A|{str(r[id_col])}"
        lab = str(r[label_col]) if label_col else str(r[id_col])
        G.add_node(nid, label=lab, role="author")
    # Kanten
    ecols = {c.lower(): c for c in edges.columns}
    src_col = ecols.get("src") or ecols.get("source") or ecols.get("from")
    tgt_col = ecols.get("tgt") or ecols.get("target") or ecols.get("to")
    w_col = ecols.get("weighted") or ecols.get("weight") or ecols.get("value") or ecols.get("total") or ecols.get("count")
    if not (src_col and tgt_col):
        raise KeyError("Autoren-Edges: src/tgt nicht gefunden.")
    for _, r in edges.iterrows():
        u = f"A|{str(r[src_col])}"
        v = f"A|{str(r[tgt_col])}"
        if u == v:
            continue
        w = float(r[w_col]) if w_col in edges.columns else 1.0
        if G.has_edge(u, v):
            G[u][v]["weight"] = G[u][v].get("weight", 0.0) + w
        else:
            G.add_edge(u, v, weight=w)
    return G

# ---- Chronik → Graph (Reuse network.py) ----
def chronik_csv_to_graph(mentions_csv: str) -> nx.Graph:
    df = chr_load_mentions_csv(mentions_csv)
    doc_col, work_col = chr_resolve_columns(df)
    return chr_build_graph(df, doc_col, work_col)

# ---- AuthorNode (neue Form) + kombinierte Scene-Erzeugung (Reuse Klassen) ----
# Wir erweitern die network.py-Ansicht um einen dritten Knotentyp "author" (Quadrat)
try:
    from organizer.chronik.network import _BaseLabelMixin as _BaseLabelMixin  # type: ignore
except Exception:
    # Fallback minimaler MixIn falls private Klasse nicht exportiert
    class _BaseLabelMixin:  # type: ignore
        def set_highlight(self, on: bool) -> None:  # pragma: no cover
            pass
        def apply_emphasis(self, scale: float = 1.0, outline: Optional[QtGui.QColor] = None, brighten: int = 100) -> None:  # pragma: no cover
            pass
        def toggle_label(self, show: bool) -> None:  # pragma: no cover
            pass

class AuthorNode(QtWidgets.QGraphicsRectItem, _BaseLabelMixin):  # Quadrat
    def __init__(self, name: str, pos: Tuple[float, float], size: float, theme: Theme, show_label: bool):
        super().__init__(-size, -size, 2 * size, 2 * size)
        self.name = name
        self._base_stroke = "#25364a"
        self._base_brush = QtGui.QColor("#10b981")  # grün
        self.setAcceptHoverEvents(True)
        self.setPos(pos[0] * 300.0, pos[1] * 300.0)
        self.setBrush(QtGui.QBrush(self._base_brush))
        pen = QtGui.QPen(QtGui.QColor(self._base_stroke))
        pen.setWidth(1); pen.setCosmetic(True)
        self.setPen(pen)
        self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)
        self.setFlag(QtWidgets.QGraphicsItem.ItemIsMovable, True)
        self.setFlag(QtWidgets.QGraphicsItem.ItemSendsGeometryChanges, True)
        self.setFlag(QtWidgets.QGraphicsItem.ItemIsSelectable, True)
        self.edges: List[C_EdgeItem] = []

        # Label mit Hintergrund wie in network.py
        self._label_bg_rect = QtWidgets.QGraphicsRectItem(self)
        self._label_bg_rect.setBrush(QtGui.QBrush(QtGui.QColor("#0e1116")))
        self._label_bg_rect.setPen(QtGui.QPen(Qt.NoPen))
        self._label_bg_rect.setFlag(QtWidgets.QGraphicsItem.ItemIgnoresTransformations, True)
        self._label_bg_rect.setVisible(show_label)

        self.label = QtWidgets.QGraphicsSimpleTextItem(name if show_label else "", self)
        self.label.setBrush(QtGui.QBrush(QtGui.QColor("#e6edf6")))
        self.label.setFlag(QtWidgets.QGraphicsItem.ItemIgnoresTransformations, True)
        self.label.setPos(size + 6, -8)
        # Hilfsfunktionen stammen aus _BaseLabelMixin
        try:
            self._update_label_bg()  # type: ignore[attr-defined]
        except Exception:
            pass
        self.setToolTip(f"Autor: {name}")

    def itemChange(self, change, value):
        if change == QtWidgets.QGraphicsItem.ItemPositionHasChanged:
            for e in list(self.edges):
                try:
                    e.update_position()
                except Exception:
                    pass
        return super().itemChange(change, value)

# Kombi-Canvas: akzeptiert AuthorNode zusätzlich
class ComboCanvas(C_GraphCanvas):
    def __init__(self):
        super().__init__()
        self._extra_node_types = (AuthorNode,)

    # überschreibe nur, wo Typprüfung hart ist
    def _apply_focus(self, node: QtWidgets.QGraphicsItem, sticky: bool) -> None:  # type: ignore[override]
        types = (C_ChronikNode, C_WorkNode) + self._extra_node_types
        scn = self.scene()
        if not scn or not self._belongs_here(node):
            return
        show_labels = bool(getattr(scn, "_show_labels", True))
        theme: Theme = getattr(scn, "_theme", self._theme)

        if sticky:
            self._clear_sticky_focus()
        else:
            self._clear_temp_focus()

        nodes, edges = self._cluster_edges_and_neighbors(node)
        for idx, it in enumerate(nodes):
            if not self._belongs_here(it):
                continue
            if isinstance(it, types):
                if idx == 0:
                    it.apply_emphasis(scale=1.20, outline=QtGui.QColor("#ef4444"), brighten=120)  # type: ignore[attr-defined]
                else:
                    it.apply_emphasis(scale=1.10, outline=QtGui.QColor("#ef4444"), brighten=110)  # type: ignore[attr-defined]
                if not show_labels:
                    it.toggle_label(True)  # type: ignore[attr-defined]
                (self._sticky_nodes if sticky else self._temp_nodes).append(it)
        for e in edges:
            e.set_highlight(True, theme)  # type: ignore[arg-type]
            (self._sticky_edges if sticky else self._temp_edges).append(e)
        self.viewport().update()

    def highlight(self, term: str, show_labels: bool) -> int:  # type: ignore[override]
        term = term.strip().lower()
        types = (C_ChronikNode, C_WorkNode) + self._extra_node_types
        hits = 0
        for n, item in list(self._nodes.items()):
            hit = bool(term and term in n.lower())
            if isinstance(item, types) and self._belongs_here(item):
                item.set_highlight(hit)  # type: ignore[attr-defined]
                item.toggle_label(show_labels or hit)  # type: ignore[attr-defined]
            hits += int(hit)
        return hits

# Kombinierte Scene bauen
def make_combined_scene(
    G_auth: Optional[nx.Graph],
    pos_auth: Dict[str, Tuple[float, float]],
    min_w_auth: int,
    G_chr: Optional[nx.Graph],
    pos_chr: Dict[str, Tuple[float, float]],
    min_w_chr: int,
    show_labels: bool,
    theme: Optional[Theme] = None,
) -> QtWidgets.QGraphicsScene:
    theme = theme or Theme.dark()
    scene = QtWidgets.QGraphicsScene()
    scene._theme = theme  # wie in network.py verwendet
    scene._show_labels = bool(show_labels)
    scene.setBackgroundBrush(QtGui.QBrush(QtGui.QColor(theme.bg)))

    node_items: Dict[str, QtWidgets.QGraphicsItem] = {}

    # --- Chronik-Knoten (Reuse)
    if G_chr is not None:
        for n, d in G_chr.nodes(data=True):
            role = d.get("role")
            label = d.get("label", str(n))
            pos = pos_chr.get(n, (0.0, 0.0))
            if role == "chronik":
                size = 7.0
                item = C_ChronikNode(label, pos, size, theme, show_label=show_labels)
            else:  # work
                size = 11.0
                item = C_WorkNode(label, pos, size, theme, show_label=show_labels)
            node_items[n] = item
            scene.addItem(item)

        for u, v, ed in G_chr.edges(data=True):
            w = float(ed.get("weight", 1.0))
            if w < float(min_w_chr):
                continue
            a = node_items.get(u); b = node_items.get(v)
            if a is None or b is None:
                continue
            e = C_EdgeItem(a, b, w, theme)
            e.setZValue(-100)
            if hasattr(a, "edges"): a.edges.append(e)  # type: ignore[attr-defined]
            if hasattr(b, "edges"): b.edges.append(e)  # type: ignore[attr-defined]
            scene.addItem(e)

    # --- Autoren-Knoten (neu)
    if G_auth is not None:
        for n, d in G_auth.nodes(data=True):
            label = d.get("label", str(n))
            pos = pos_auth.get(n, (0.0, 0.0))
            size = 9.0
            item = AuthorNode(label, pos, size, theme, show_label=show_labels)
            node_items[n] = item
            scene.addItem(item)

        for u, v, ed in G_auth.edges(data=True):
            w = float(ed.get("weight", 1.0))
            if w < float(min_w_auth):
                continue
            a = node_items.get(u); b = node_items.get(v)
            if a is None or b is None:
                continue
            e = C_EdgeItem(a, b, w, theme)
            e.setZValue(-100)
            if hasattr(a, "edges"): a.edges.append(e)  # type: ignore[attr-defined]
            if hasattr(b, "edges"): b.edges.append(e)  # type: ignore[attr-defined]
            scene.addItem(e)

    return scene

# ---- Hauptfenster ----
class ControlPanel(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        lay = QtWidgets.QFormLayout(self)
        lay.setLabelAlignment(Qt.AlignLeft)

        # Autoren
        self.btn_auth_run = QtWidgets.QPushButton("Autoren: Analyse starten")
        self.btn_auth_load = QtWidgets.QPushButton("Autoren: CSV-Ordner laden")
        self.spin_auth_w = QtWidgets.QSpinBox(); self.spin_auth_w.setRange(1, 10**9); self.spin_auth_w.setValue(1)

        # Chronik
        self.btn_chr_loadcsv = QtWidgets.QPushButton("Chronik: Mentions-CSV laden")
        self.combo_chr_layout = QtWidgets.QComboBox(); self.combo_chr_layout.addItems(["bipartite", "spring", "kamada_kawai", "forceatlas2"])
        self.spin_chr_w = QtWidgets.QSpinBox(); self.spin_chr_w.setRange(1, 10**9); self.spin_chr_w.setValue(1)

        # Global
        self.chk_labels = QtWidgets.QCheckBox("Labels anzeigen"); self.chk_labels.setChecked(True)
        self.edit_search = QtWidgets.QLineEdit(); self.edit_search.setPlaceholderText("Suche in Labels…")
        self.btn_search = QtWidgets.QPushButton("Suchen")
        self.btn_export_png = QtWidgets.QPushButton("PNG exportieren")
        self.btn_html_report = QtWidgets.QPushButton("Autoren HTML-Report")
        self.btn_fit = QtWidgets.QPushButton("Ansicht fitten")

        lay.addRow("Autoren", self.btn_auth_run)
        lay.addRow("", self.btn_auth_load)
        lay.addRow("Min Gewicht Autoren", self.spin_auth_w)
        lay.addRow(QtWidgets.QLabel("—"))
        lay.addRow("Chronik CSV", self.btn_chr_loadcsv)
        lay.addRow("Layout Chronik", self.combo_chr_layout)
        lay.addRow("Min Gewicht Chronik", self.spin_chr_w)
        lay.addRow(QtWidgets.QLabel("—"))
        lay.addRow(self.chk_labels)
        h = QtWidgets.QHBoxLayout()
        h.addWidget(self.edit_search, 1)
        h.addWidget(self.btn_search)
        lay.addRow(h)
        lay.addRow(self.btn_fit)
        lay.addRow(self.btn_export_png)
        lay.addRow(self.btn_html_report)

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Super GUI — Autoren + Chronik")
        self.resize(1400, 880)
        self._theme = Theme.dark()

        # Daten
        self.authors = DataBundle()
        self.chronik_csv: Optional[str] = None
        self.G_auth: Optional[nx.Graph] = None
        self.G_chr: Optional[nx.Graph] = None
        self.pos_auth: Dict[str, Tuple[float, float]] = {}
        self.pos_chr: Dict[str, Tuple[float, float]] = {}

        # UI
        self.view = ComboCanvas()
        self.panel = ControlPanel()
        splitter = QtWidgets.QSplitter(Qt.Horizontal)
        splitter.addWidget(self.panel)
        splitter.addWidget(self.view)
        splitter.setStretchFactor(1, 1)
        self.setCentralWidget(splitter)
        self.status = self.statusBar()
        self.status.showMessage("Bereit")

        self._connect()
        self._apply_theme_stylesheet()

    # ---- Events ----
    def _connect(self) -> None:
        p = self.panel
        p.btn_auth_run.clicked.connect(self._run_authors_pipeline)
        p.btn_auth_load.clicked.connect(self._load_authors_csv_dir)
        p.btn_chr_loadcsv.clicked.connect(self._load_chronik_csv)
        p.spin_auth_w.valueChanged.connect(lambda _: self._rebuild_scene())
        p.spin_chr_w.valueChanged.connect(lambda _: self._rebuild_scene())
        p.chk_labels.toggled.connect(lambda _: self._rebuild_scene())
        p.combo_chr_layout.currentTextChanged.connect(lambda _: self._layout_chronik_then_rebuild())
        p.btn_export_png.clicked.connect(self._export_png)
        p.btn_html_report.clicked.connect(self._export_html_authors)
        p.btn_fit.clicked.connect(self._fit)
        p.btn_search.clicked.connect(self._do_search)

        # Shortcuts
        QtWidgets.QShortcut(QtGui.QKeySequence("F"), self, activated=self._fit)
        QtWidgets.QShortcut(QtGui.QKeySequence("Ctrl+F"), self, activated=lambda: p.edit_search.setFocus())

    # ---- Autoren ----
    def _run_authors_pipeline(self) -> None:
        if RUN_ON_FOLDER is None:
            QtWidgets.QMessageBox.critical(self, "Fehler", "authors_search.run_on_folder nicht verfügbar.")
            return
        base = QtWidgets.QFileDialog.getExistingDirectory(self, "PDF-Ordner wählen", os.getcwd())
        if not base:
            return
        print(f"[DEBUG] Autoren-Pipeline startet in: {base}", flush=True)
        session_dir = RUN_ON_FOLDER(base)
        print(f"[DEBUG] Autoren-Session: {session_dir}", flush=True)
        self._load_authors_from_dir(session_dir)

    def _load_authors_csv_dir(self) -> None:
        directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Ordner mit authors_nodes/edges.csv", os.getcwd())
        if not directory:
            return
        self._load_authors_from_dir(directory)

    def _load_authors_from_dir(self, directory: str) -> None:
        try:
            self.authors = load_authors_session(directory)
            n = 0 if self.authors.nodes is None else self.authors.nodes.shape[0]
            m = 0 if self.authors.edges is None else self.authors.edges.shape[0]
            self.status.showMessage(f"Autoren geladen: Nodes={n} Edges={m}")
            self._build_authors_graph()
            self._rebuild_scene()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Autoren laden", str(e))

    def _build_authors_graph(self) -> None:
        if self.authors.nodes is None or self.authors.edges is None:
            self.G_auth = None
            self.pos_auth = {}
            return
        self.G_auth = authors_to_graph(self.authors.nodes, self.authors.edges)
        # Layout Autoren: einfache Feder
        pos = nx.spring_layout(self.G_auth, weight="weight", seed=42)
        # In float dict konvertieren
        self.pos_auth = {n: (float(x), float(y)) for n, (x, y) in pos.items()}

    # ---- Chronik ----
    def _load_chronik_csv(self) -> None:
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Chronik Mentions-CSV wählen", os.getcwd(), "CSV (*.csv);;Alle Dateien (*)")
        if not path:
            return
        try:
            self.chronik_csv = path
            self.G_chr = chronik_csv_to_graph(path)
            # initiales Layout nach Auswahl
            self._layout_chronik_then_rebuild()
            self.status.showMessage(f"Chronik CSV geladen: {os.path.basename(path)} — Nodes {self.G_chr.number_of_nodes()} | Edges {self.G_chr.number_of_edges()}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Chronik laden", str(e))

    def _layout_chronik_then_rebuild(self) -> None:
        if self.G_chr is not None:
            mode = self.panel.combo_chr_layout.currentText()
            self.pos_chr = chr_compute_layout(self.G_chr, mode)
        self._rebuild_scene()

    # ---- Szene/Zeichnung ----
    def _rebuild_scene(self) -> None:
        min_w_auth = int(self.panel.spin_auth_w.value())
        min_w_chr = int(self.panel.spin_chr_w.value())
        show_labels = bool(self.panel.chk_labels.isChecked())

        # Positionen leicht separieren, damit Layer nicht überlappen:
        pos_auth = {n: (x + 3.0, y) for n, (x, y) in self.pos_auth.items()}  # Autoren nach rechts schieben
        pos_chr = {n: (x - 3.0, y) for n, (x, y) in self.pos_chr.items()}    # Chronik nach links schieben

        scene = make_combined_scene(
            self.G_auth, pos_auth, min_w_auth,
            self.G_chr, pos_chr, min_w_chr,
            show_labels, theme=self._theme
        )
        self.view.set_graph_scene(scene)

    def _fit(self) -> None:
        if self.view.scene():
            self.view.fitInView(self.view.scene().itemsBoundingRect(), Qt.KeepAspectRatio)

    # ---- Suche/Export/Report ----
    def _do_search(self) -> None:
        term = self.panel.edit_search.text()
        hits = self.view.highlight(term, self.panel.chk_labels.isChecked())
        self.status.showMessage(f"Suche '{term}' → Treffer: {hits}")

    def _export_png(self) -> None:
        if not self.view.scene():
            self.status.showMessage("Keine Szene für Export.")
            return
        out, _ = QtWidgets.QFileDialog.getSaveFileName(self, "PNG exportieren", os.path.join(os.getcwd(), "combined_graph.png"), "PNG (*.png)")
        if not out:
            return
        try:
            self.view.export_png(out)  # Reuse aus network.py
            self.status.showMessage(f"Exportiert: {out}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Export", str(e))

    def _export_html_authors(self) -> None:
        if AU_MOD is None or self.authors.edges is None:
            QtWidgets.QMessageBox.information(self, "Report", "authors_utils oder Autoren-Edges fehlen.")
            return
        if not hasattr(AU_MOD, "write_html_report"):
            QtWidgets.QMessageBox.information(self, "Report", "write_html_report nicht verfügbar.")
            return
        out_dir = QtWidgets.QFileDialog.getExistingDirectory(self, "Zielordner für Autoren-Report", os.getcwd())
        if not out_dir:
            return
        try:
            # Minimales Mapping: Author(author_id, display, surnames, pattern_strs, folded_surnames)
            authors_map: Dict[str, Any] = {}
            if hasattr(AU_MOD, "Author") and self.authors.nodes is not None:
                ncols = {c.lower(): c for c in self.authors.nodes.columns}
                id_col = ncols.get("author_id") or ncols.get("id")
                disp_col = ncols.get("display") or ncols.get("label") or ncols.get("name") or id_col
                sur_col = ncols.get("surnames")
                if id_col:
                    for _, r in self.authors.nodes.iterrows():
                        aid = str(r[id_col])
                        disp = str(r[disp_col]) if disp_col else aid
                        surn = tuple(str(r[sur_col]).split("|")) if sur_col else tuple()
                        authors_map[aid] = AU_MOD.Author(aid, disp, surn, tuple(), tuple())  # type: ignore
            AU_MOD.write_html_report(out_dir, authors_map, self.authors.edges)  # type: ignore
            self.status.showMessage(f"HTML-Report geschrieben: {out_dir}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Report", str(e))

    # ---- Theme ----
    def _apply_theme_stylesheet(self) -> None:
        pal = self.palette()
        pal.setColor(QtGui.QPalette.Window, QtGui.QColor(self._theme.bg))
        pal.setColor(QtGui.QPalette.WindowText, QtGui.QColor(self._theme.label))
        self.setPalette(pal)
        self.setStyleSheet(f"QMainWindow {{ background:{self._theme.bg}; }}")

# ---- main ----
def main() -> None:
    print("[DEBUG] Starte Super GUI", flush=True)
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