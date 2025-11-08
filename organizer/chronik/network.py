#!/usr/bin/env python3
"""
network.py — Bipartites chroniken_library↔Werke-Netz als Qt-Canvas mit radialem Degree-Push
und label collision avoidance.

Neu:
- Chroniken (role='chronik') mit hohem (gewichteten) Grad werden radial nach außen geschoben.
- Labels werden radial nach außen platziert und kollisionsfrei iterativ auseinandergezogen.

API (unverändert):
    load_mentions_csv(path)
    resolve_columns(df) -> (doc_col, work_col)
    build_bipartite_graph(df, doc_col, work_col) -> nx.Graph
    compute_layout(G, mode='bipartite'|'spring'|'kamada_kawai'|'forceatlas2') -> pos-dict
    make_scene(G, positions, min_edge_weight, show_labels) -> QGraphicsScene
    GraphCanvas

Start-Demo:
    python network.py
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, List

import pandas as pd
import networkx as nx
from networkx.algorithms import bipartite as nx_bipartite  # noqa

# Qt
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import Qt

try:
    import sip  # type: ignore
except Exception:
    sip = None


# ------------------------ Debug ------------------------

def debug(msg: str) -> None:
    print(f"[DEBUG] {msg}", flush=True)


def _is_dead(obj: object) -> bool:
    if obj is None:
        return True
    if sip is not None:
        try:
            return sip.isdeleted(obj)  # type: ignore[attr-defined]
        except Exception:
            return True
    try:
        return getattr(obj, "scene", None) is None and False
    except Exception:
        return True


# ------------------------ CSV / Graphbau ------------------------

def load_mentions_csv(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"CSV nicht gefunden: {path}")
    try:
        df = pd.read_csv(path, sep=';', engine='python')
        if df.empty:
            raise ValueError("CSV ist leer.")
        return df
    except Exception as e1:
        debug(f"sep=';' scheiterte → {e1}. Versuche Auto-Sniff.")
        df = pd.read_csv(path, sep=None, engine='python')
        if df.empty:
            raise ValueError("CSV ist leer.")
        return df


def resolve_columns(df: pd.DataFrame) -> Tuple[str, str]:
    doc_candidates = ["pdf_file", "pdf", "document", "source", "file", "filename", "doc"]
    work_candidates = ["label", "canonical", "work", "title", "chronik", "edition", "name", "match", "normalized"]
    lower = {c.lower(): c for c in df.columns}
    doc_col = next((lower[c] for c in doc_candidates if c in lower), None)
    work_col = next((lower[c] for c in work_candidates if c in lower), None)
    if not doc_col:
        raise KeyError(f"Dokumentspalte nicht erkannt. Erwartet eine aus {doc_candidates}. Vorhanden: {list(df.columns)}")
    if not work_col:
        raise KeyError(f"Werkspalte nicht erkannt. Erwartet eine aus {work_candidates}. Vorhanden: {list(df.columns)}")
    return doc_col, work_col


def build_bipartite_graph(df: pd.DataFrame, doc_col: str, work_col: str) -> nx.Graph:
    df = df[[doc_col, work_col]].dropna()
    df[doc_col] = df[doc_col].astype(str).str.strip()
    df[work_col] = df[work_col].astype(str).str.strip()

    pairs = df.groupby([doc_col, work_col]).size().reset_index(name="w")

    chronik_docs = pairs.groupby(work_col)[doc_col].nunique().to_dict()
    pdf_chroniks = pairs.groupby(doc_col)[work_col].nunique().to_dict()

    G = nx.Graph(name="chroniken_library↔Werke")

    for ch, n_docs in chronik_docs.items():
        nid = _id_chronik(ch)
        G.add_node(nid, label=ch, role="chronik", bipartite=0, mentions=int(n_docs))

    for pdf, n_ch in pdf_chroniks.items():
        nid = _id_pdf(pdf)
        G.add_node(nid, label=os.path.basename(pdf) or pdf, role="work", bipartite=1, items=int(n_ch))

    for _, row in pairs.iterrows():
        u = _id_pdf(row[doc_col])
        v = _id_chronik(row[work_col])
        w = float(row["w"])
        if u in G and v in G:
            G.add_edge(u, v, weight=w)

    debug(f"Graph (bipartit) gebaut: nodes={G.number_of_nodes()} edges={G.number_of_edges()}")
    return G


def _id_pdf(path: str) -> str:
    return f"P|{os.path.basename(path)}"


def _id_chronik(name: str) -> str:
    return f"C|{name}"


# ------------------------ Layout-Modelle ------------------------

def _weighted_degree(G: nx.Graph, n: str) -> float:
    return sum(float(d.get("weight", 1.0)) for _u, _v, d in G.edges(n, data=True))


def _node_mass(G: nx.Graph, n: str) -> float:
    d = G.nodes[n]
    if d.get("role") == "chronik":
        return 1.0 + math.sqrt(max(1, int(d.get("mentions", 1))))
    else:
        return 1.0 + math.sqrt(max(1, int(d.get("items", 1))))


def _bipartite_ordered_layout(G: nx.Graph, scale: float = 2.0) -> Dict[str, Tuple[float, float]]:
    left = [n for n, d in G.nodes(data=True) if d.get("role") == "work"]
    right = [n for n, d in G.nodes(data=True) if d.get("role") == "chronik"]

    def _order(side: List[str], other: List[str]) -> List[str]:
        idx = {n: i for i, n in enumerate(other)}
        wsum = {}
        for n in side:
            neigh = list(G.neighbors(n))
            if not neigh:
                wsum[n] = (0.0, 1e-9)
            else:
                s = 0.0
                w = 0.0
                for m in neigh:
                    s += idx.get(m, 0) * _node_mass(G, m)
                    w += _node_mass(G, m)
                wsum[n] = (s, w)
        return sorted(side, key=lambda n: wsum[n][0] / wsum[n][1])

    for _ in range(2):
        left = _order(left, right)
        right = _order(right, left)

    pos: Dict[str, Tuple[float, float]] = {}

    def _coords(lst: List[str], xval: float) -> None:
        n = max(1, len(lst))
        for i, node in enumerate(lst):
            y = (i - (n - 1) / 2.0) * 1.0
            pos[node] = (xval, y)

    _coords(left, -1.0)
    _coords(right, 1.0)
    return {n: (x * scale, y * scale) for n, (x, y) in pos.items()}


def _forceatlas2(G: nx.Graph,
                 init_pos: Optional[Dict[str, Tuple[float, float]]] = None,
                 iterations: int = 300,
                 gravity: float = 0.05,
                 scaling: float = 1.0,
                 dt: float = 0.1) -> Dict[str, Tuple[float, float]]:
    import random
    nodes = list(G.nodes())
    if not nodes:
        return {}
    pos = {n: list(init_pos[n]) if init_pos and n in init_pos else [random.uniform(-1, 1), random.uniform(-1, 1)] for n in nodes}
    mass = {n: _node_mass(G, n) for n in nodes}

    for it in range(iterations):
        fx = {n: 0.0 for n in nodes}
        fy = {n: 0.0 for n in nodes}

        for i in range(len(nodes)):
            n1 = nodes[i]
            x1, y1 = pos[n1]
            for j in range(i + 1, len(nodes)):
                n2 = nodes[j]
                x2, y2 = pos[n2]
                dx = x1 - x2
                dy = y1 - y2
                dist = math.hypot(dx, dy) + 1e-9
                f = (scaling * mass[n1] * mass[n2]) / dist
                rx = f * (dx / dist)
                ry = f * (dy / dist)
                fx[n1] += rx; fy[n1] += ry
                fx[n2] -= rx; fy[n2] -= ry

        for u, v, ed in G.edges(data=True):
            w = float(ed.get("weight", 1.0))
            x1, y1 = pos[u]; x2, y2 = pos[v]
            dx = x2 - x1; dy = y2 - y1
            dist = math.hypot(dx, dy) + 1e-9
            f = w * dist
            ax = f * (dx / dist)
            ay = f * (dy / dist)
            fx[u] += ax; fy[u] += ay
            fx[v] -= ax; fy[v] -= ay

        for n in nodes:
            x, y = pos[n]
            fx[n] += -gravity * x
            fy[n] += -gravity * y

        max_disp = 0.0
        for n in nodes:
            inv_m = 1.0 / mass[n]
            dx = dt * fx[n] * inv_m
            dy = dt * fy[n] * inv_m
            pos[n][0] += dx
            pos[n][1] += dy
            max_disp = max(max_disp, abs(dx) + abs(dy))

        if it % 50 == 0:
            debug(f"[FA2] iter={it} max_disp={max_disp:.4f}")
        if max_disp < 1e-4:
            break

    return {n: (float(x), float(y)) for n, (x, y) in pos.items()}


def _relax_positions(G: nx.Graph,
                     pos: Dict[str, Tuple[float, float]],
                     iterations: int = 160,
                     base_dist: float = 0.35,
                     step: float = 0.06) -> Dict[str, Tuple[float, float]]:
    nodes = list(pos.keys())
    if len(nodes) <= 1:
        return pos
    radius = {n: 0.05 + 0.02 * math.sqrt(_node_mass(G, n)) for n in nodes}
    p = {n: [float(x), float(y)] for n, (x, y) in pos.items()}

    for _ in range(iterations):
        moved = 0
        for i, n1 in enumerate(nodes):
            x1, y1 = p[n1]
            fx = fy = 0.0
            for j, n2 in enumerate(nodes):
                if i == j:
                    continue
                x2, y2 = p[n2]
                dx = x1 - x2
                dy = y1 - y2
                dist = math.hypot(dx, dy) + 1e-6
                want = base_dist + radius[n1] + radius[n2]
                if dist < want:
                    f = (want - dist) / want
                    fx += (dx / dist) * f
                    fy += (dy / dist) * f
            if fx or fy:
                x1 += fx * step
                y1 += fy * step
                p[n1] = [x1, y1]
                moved += 1
        if moved == 0:
            break
    return {n: (float(x), float(y)) for n, (x, y) in p.items()}


def _radial_degree_push(G: nx.Graph,
                        pos: Dict[str, Tuple[float, float]],
                        role: str = "chronik",
                        alpha: float = 0.6) -> Dict[str, Tuple[float, float]]:
    """Schiebe Nodes eines role radial nach außen proportional zum gewichteten Grad."""
    if not pos:
        return pos
    cx = sum(x for x, _ in pos.values()) / len(pos)
    cy = sum(y for _, y in pos.values()) / len(pos)
    # Normierung
    candidates = [n for n, d in G.nodes(data=True) if d.get("role") == role]
    if not candidates:
        return pos
    degs = {n: _weighted_degree(G, n) for n in candidates}
    m = max(degs.values()) or 1.0
    out: Dict[str, Tuple[float, float]] = dict(pos)
    for n, (x, y) in pos.items():
        if G.nodes[n].get("role") != role:
            continue
        dx = x - cx; dy = y - cy
        r = math.hypot(dx, dy) or 1e-9
        scale = 1.0 + alpha * (degs.get(n, 0.0) / m)
        out[n] = (cx + dx * scale, cy + dy * scale)
    return out


def compute_layout(G, mode: str = "bipartite") -> dict[str, tuple[float, float]]:
    if G.number_of_nodes() == 0:
        return {}
    if mode == "bipartite":
        pos0 = _bipartite_ordered_layout(G, scale=2.0)
        pos = _relax_positions(G, pos0, iterations=160, base_dist=0.35, step=0.06)
    elif mode == "kamada_kawai":
        pos0 = nx.kamada_kawai_layout(G, weight="weight")
        pos = _relax_positions(G, {str(n): (float(x), float(y)) for n, (x, y) in pos0.items()},
                               iterations=120, base_dist=0.3, step=0.05)
        pos = _radial_degree_push(G, pos, role="chronik", alpha=0.6)
    elif mode == "forceatlas2":
        init = _bipartite_ordered_layout(G, scale=1.2)
        pos0 = _forceatlas2(G, init_pos=init, iterations=350, gravity=0.06, scaling=1.2, dt=0.08)
        pos = _relax_positions(G, pos0, iterations=140, base_dist=0.32, step=0.05)
        pos = _radial_degree_push(G, pos, role="chronik", alpha=0.6)
    else:
        pos0 = nx.spring_layout(G, weight="weight", iterations=350, seed=42)
        pos = _relax_positions(G, {str(n): (float(x), float(y)) for n, (x, y) in pos0.items()},
                               iterations=140, base_dist=0.32, step=0.05)
        pos = _radial_degree_push(G, pos, role="chronik", alpha=0.6)
    return pos


# ------------------------ Farben / Theme ------------------------

@dataclass
class Theme:
    bg: str = "#0e1116"
    chronik_fill: str = "#1fb6ff"
    chronik_stroke: str = "#0a1f33"
    work_fill: str = "#fbbf24"
    work_stroke: str = "#3a2c0b"
    label: str = "#e6edf6"
    label_bg: str = "#0e1116"
    edge: str = "#6b7280"
    edge_sel: str = "#ef4444"

    @staticmethod
    def dark() -> "Theme":
        return Theme()

    @staticmethod
    def light() -> "Theme":
        return Theme(
            bg="#ffffff",
            chronik_fill="#2563eb",
            chronik_stroke="#0b1a38",
            work_fill="#f59e0b",
            work_stroke="#43330c",
            label="#111827",
            label_bg="#ffffff",
            edge="#737373",
            edge_sel="#be123c",
        )


def _qcolor(hex_code: str, alpha: Optional[int] = None) -> QtGui.QColor:
    c = QtGui.QColor(hex_code)
    if alpha is not None:
        c.setAlpha(max(0, min(255, int(alpha))))
    return c


# ------------------------ Interaktive Items ------------------------

class EdgeItem(QtWidgets.QGraphicsPathItem):
    def __init__(self, a: QtWidgets.QGraphicsItem, b: QtWidgets.QGraphicsItem, weight: float, theme: Theme):
        super().__init__()
        self.setZValue(-100)
        self.a = a
        self.b = b
        self.weight = float(max(1.0, weight))
        self._theme = theme
        alpha = max(40, min(220, int(60 + 50 * math.log1p(self.weight))))
        pen = QtGui.QPen(_qcolor(theme.edge, alpha))
        pen.setWidthF(max(1.0, 0.6 + math.sqrt(self.weight)))
        pen.setCosmetic(True)
        self.setPen(pen)
        self.setToolTip(f"Verbindungen im Werk: {int(self.weight)}")
        self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)
        self.update_position()

    def update_position(self) -> None:
        if _is_dead(self.a) or _is_dead(self.b):
            return
        p1 = self.a.pos()
        p2 = self.b.pos()
        dx = p2.x() - p1.x()
        k = 0.15 * abs(dx)
        c1 = QtCore.QPointF(p1.x() + 0.25 * dx, p1.y() - k)
        c2 = QtCore.QPointF(p2.x() - 0.25 * dx, p2.y() + k)
        path = QtGui.QPainterPath(p1)
        path.cubicTo(c1, c2, p2)
        self.setPath(path)

    def set_highlight(self, on: bool, theme: Theme) -> None:
        if _is_dead(self):
            return
        pen = self.pen()
        try:
            if on:
                pen.setColor(_qcolor(theme.edge_sel, 230))
                pen.setWidthF(self.pen().widthF() + 1.0)
            else:
                c = self.pen().color()
                base = _qcolor(self._theme.edge, c.alpha())
                pen.setColor(base)
                pen.setWidthF(max(1.0, 0.6 + math.sqrt(self.weight)))
            self.setPen(pen)
        except RuntimeError:
            pass


class _BaseLabelMixin:
    name: str
    _base_stroke: str
    _base_brush: QtGui.QColor
    label: QtWidgets.QGraphicsSimpleTextItem
    _label_bg_rect: QtWidgets.QGraphicsRectItem
    edges: List[EdgeItem]

    def set_highlight(self, on: bool) -> None:
        if _is_dead(self):
            return
        pen = self.pen()
        try:
            pen.setWidth(3 if on else 1)
            pen.setColor(_qcolor("#ef4444") if on else _qcolor(self._base_stroke))
            pen.setCosmetic(True)
            self.setPen(pen)
            for e in getattr(self, "edges", []):
                if _is_dead(e):
                    continue
                scene = self.scene()
                theme: Theme = getattr(scene, "_theme", Theme.dark()) if scene else Theme.dark()
                e.set_highlight(on, theme)
        except RuntimeError:
            pass

    def apply_emphasis(self, scale: float = 1.0, outline: Optional[QtGui.QColor] = None, brighten: int = 100) -> None:
        if _is_dead(self):
            return
        try:
            self.setScale(max(0.1, float(scale)))
            pen = self.pen()
            pen.setWidth(2 if outline else 1)
            pen.setColor(outline if outline else _qcolor(self._base_stroke))
            pen.setCosmetic(True)
            self.setPen(pen)
            b = QtGui.QColor(self._base_brush)
            b = b.lighter(max(1, int(brighten)))
            self.setBrush(QtGui.QBrush(b))
        except RuntimeError:
            pass

    def toggle_label(self, show: bool) -> None:
        if _is_dead(self.label) or _is_dead(self._label_bg_rect):
            return
        self.label.setText(self.name if show else "")
        self._label_bg_rect.setVisible(show)
        if show:
            self._update_label_bg()

    def _update_label_bg(self) -> None:
        if _is_dead(self.label) or _is_dead(self._label_bg_rect):
            return
        br = self.label.boundingRect()
        pad = 3.0
        r = QtCore.QRectF(br.x() - pad, br.y() - pad, br.width() + 2 * pad, br.height() + 2 * pad)
        self._label_bg_rect.setRect(r)

    def _set_label_offset(self, dx: float, dy: float) -> None:
        """Setzt Label-Offset relativ zum Knoten und aktualisiert die Hintergrundbox."""
        if _is_dead(self.label) or _is_dead(self._label_bg_rect):
            return
        self.label.setPos(dx, dy)
        self._update_label_bg()


class ChronikNode(QtWidgets.QGraphicsEllipseItem, _BaseLabelMixin):
    def __init__(self, name: str, pos: Tuple[float, float], size: float, theme: Theme, show_label: bool):
        super().__init__(-size, -size, 2 * size, 2 * size)
        self.name = name
        self._base_stroke = theme.chronik_stroke
        self._base_brush = QtGui.QColor(theme.chronik_fill)
        self.setAcceptHoverEvents(True)
        self.setPos(pos[0] * 300.0, pos[1] * 300.0)
        self.setBrush(QtGui.QBrush(self._base_brush))
        pen = QtGui.QPen(QtGui.QColor(theme.chronik_stroke))
        pen.setWidth(1); pen.setCosmetic(True)
        self.setPen(pen)
        self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)
        self.setFlag(QtWidgets.QGraphicsItem.ItemIsMovable, True)
        self.setFlag(QtWidgets.QGraphicsItem.ItemSendsGeometryChanges, True)
        self.setFlag(QtWidgets.QGraphicsItem.ItemIsSelectable, True)
        self.edges: List[EdgeItem] = []
        self._label_bg_rect = QtWidgets.QGraphicsRectItem(self)
        self._label_bg_rect.setBrush(QtGui.QBrush(_qcolor(theme.label_bg, 180)))
        self._label_bg_rect.setPen(QtGui.QPen(Qt.NoPen))
        self._label_bg_rect.setFlag(QtWidgets.QGraphicsItem.ItemIgnoresTransformations, True)
        self._label_bg_rect.setVisible(show_label)
        self.label = QtWidgets.QGraphicsSimpleTextItem(name if show_label else "", self)
        self.label.setBrush(QtGui.QBrush(QtGui.QColor(theme.label)))
        self.label.setFlag(QtWidgets.QGraphicsItem.ItemIgnoresTransformations, True)
        self._set_label_offset(size + 6, -8)
        self.setToolTip(f"Chronik: {name}")

    def itemChange(self, change, value):
        if change == QtWidgets.QGraphicsItem.ItemPositionHasChanged:
            for e in list(self.edges):
                if not _is_dead(e):
                    e.update_position()
        return super().itemChange(change, value)

    def hoverEnterEvent(self, event: QtWidgets.QGraphicsSceneHoverEvent) -> None:
        view = self.scene().views()[0] if self.scene() and self.scene().views() else None
        if view and hasattr(view, "_has_sticky") and view._has_sticky():
            return
        if view and hasattr(view, "_apply_focus"):
            view._apply_focus(self, sticky=False)

    def hoverLeaveEvent(self, event: QtWidgets.QGraphicsSceneHoverEvent) -> None:
        view = self.scene().views()[0] if self.scene() and self.scene().views() else None
        if view and hasattr(view, "_has_sticky") and view._has_sticky():
            return
        if view and hasattr(view, "_clear_temp_focus"):
            view._clear_temp_focus()


class WorkNode(QtWidgets.QGraphicsPolygonItem, _BaseLabelMixin):
    def __init__(self, name: str, pos: Tuple[float, float], size: float, theme: Theme, show_label: bool):
        super().__init__()
        self.name = name
        self._base_stroke = theme.work_stroke
        self._base_brush = QtGui.QColor(theme.work_fill)
        self.setAcceptHoverEvents(True)
        r = size
        points = [QtCore.QPointF(0, -r), QtCore.QPointF(-0.866 * r, 0.5 * r), QtCore.QPointF(0.866 * r, 0.5 * r)]
        poly = QtGui.QPolygonF(points)
        self.setPolygon(poly)
        self.setPos(pos[0] * 300.0, pos[1] * 300.0)
        self.setBrush(QtGui.QBrush(self._base_brush))
        pen = QtGui.QPen(QtGui.QColor(theme.work_stroke))
        pen.setWidth(1); pen.setCosmetic(True)
        self.setPen(pen)
        self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)
        self.setFlag(QtWidgets.QGraphicsItem.ItemIsMovable, True)
        self.setFlag(QtWidgets.QGraphicsItem.ItemSendsGeometryChanges, True)
        self.setFlag(QtWidgets.QGraphicsItem.ItemIsSelectable, True)
        self.edges: List[EdgeItem] = []
        self._label_bg_rect = QtWidgets.QGraphicsRectItem(self)
        self._label_bg_rect.setBrush(QtGui.QBrush(_qcolor(theme.label_bg, 180)))
        self._label_bg_rect.setPen(QtGui.QPen(Qt.NoPen))
        self._label_bg_rect.setFlag(QtWidgets.QGraphicsItem.ItemIgnoresTransformations, True)
        self._label_bg_rect.setVisible(show_label)
        self.label = QtWidgets.QGraphicsSimpleTextItem(name if show_label else "", self)
        self.label.setBrush(QtGui.QBrush(QtGui.QColor(theme.label)))
        self.label.setFlag(QtWidgets.QGraphicsItem.ItemIgnoresTransformations, True)
        self._set_label_offset(r + 8, -10)
        self.setToolTip(f"Werk/PDF: {name}")

    def itemChange(self, change, value):
        if change == QtWidgets.QGraphicsItem.ItemPositionHasChanged:
            for e in list(self.edges):
                if not _is_dead(e):
                    e.update_position()
        return super().itemChange(change, value)

    def hoverEnterEvent(self, event: QtWidgets.QGraphicsSceneHoverEvent) -> None:
        view = self.scene().views()[0] if self.scene() and self.scene().views() else None
        if view and hasattr(view, "_has_sticky") and view._has_sticky():
            return
        if view and hasattr(view, "_apply_focus"):
            view._apply_focus(self, sticky=False)

    def hoverLeaveEvent(self, event: QtWidgets.QGraphicsSceneHoverEvent) -> None:
        view = self.scene().views()[0] if self.scene() and self.scene().views() else None
        if view and hasattr(view, "_has_sticky") and view._has_sticky():
            return
        if view and hasattr(view, "_clear_temp_focus"):
            view._clear_temp_focus()


# ------------------------ Scene / Labels ------------------------

def _center_from_pos(pos: Dict[str, Tuple[float, float]]) -> Tuple[float, float]:
    if not pos:
        return (0.0, 0.0)
    cx = sum(x for x, _ in pos.values()) / len(pos) * 300.0
    cy = sum(y for _, y in pos.values()) / len(pos) * 300.0
    return (cx, cy)


def _place_labels_radially(node_items: Dict[str, QtWidgets.QGraphicsItem],
                           center: Tuple[float, float]) -> None:
    cx, cy = center
    for it in node_items.values():
        if not isinstance(it, (ChronikNode, WorkNode)):
            continue
        px, py = it.pos().x(), it.pos().y()
        dx = px - cx; dy = py - cy
        r = math.hypot(dx, dy)
        if r < 1e-6:
            dx, dy = 1.0, 0.0
            r = 1.0
        ux, uy = dx / r, dy / r
        if isinstance(it, ChronikNode):
            base = it.boundingRect().width() * 0.6 + 10
        else:
            base = it.boundingRect().height() * 0.6 + 12
        it._set_label_offset(ux * base, uy * base * 0.6)  # leichte vertikale Kompression


def _resolve_label_collisions(node_items: Dict[str, QtWidgets.QGraphicsItem],
                              center: Tuple[float, float],
                              iterations: int = 80,
                              step: float = 4.0) -> None:
    """Iteratives, einfaches Label-Repelling in Szenenkoordinaten."""
    cx, cy = center
    labels: List[QtWidgets.QGraphicsSimpleTextItem] = []
    owners: List[QtWidgets.QGraphicsItem] = []
    for it in node_items.values():
        if isinstance(it, (ChronikNode, WorkNode)) and not _is_dead(it.label) and it.label.text():
            labels.append(it.label)
            owners.append(it)

    def _rect(lbl: QtWidgets.QGraphicsSimpleTextItem) -> QtCore.QRectF:
        return lbl.mapToScene(lbl.boundingRect()).boundingRect()

    for _ in range(iterations):
        moved = 0
        for i in range(len(labels)):
            for j in range(i + 1, len(labels)):
                li, lj = labels[i], labels[j]
                ri, rj = _rect(li), _rect(lj)
                if not ri.intersects(rj):
                    continue
                oi, oj = owners[i], owners[j]
                # Richtung: auseinander
                ci = ri.center(); cj = rj.center()
                dx = cj.x() - ci.x(); dy = cj.y() - ci.y()
                dist = math.hypot(dx, dy) or 1.0
                ux, uy = dx / dist, dy / dist
                # leicht radial nach außen schieben (weg vom Zentrum)
                pix, piy = oi.pos().x(), oi.pos().y()
                pjx, pjy = oj.pos().x(), oj.pos().y()
                rix, riy = (ci.x() - cx, ci.y() - cy); rjx, rjy = (cj.x() - cx, cj.y() - cy)
                nrix = math.hypot(rix, riy) or 1.0; nrjx = math.hypot(rjx, rjy) or 1.0
                rx_i, ry_i = rix / nrix, riy / nrix
                rx_j, ry_j = rjx / nrjx, rjy / nrjx

                # Delta für beide Labels (lokale Koordinaten = Szenendelta, da Eltern nur Translation)
                dxi = (-ux + rx_i) * step
                dyi = (-uy + ry_i) * step
                dxj = (ux + rx_j) * step
                dyj = (uy + ry_j) * step

                li.setPos(li.pos().x() + dxi, li.pos().y() + dyi)
                lj.setPos(lj.pos().x() + dxj, lj.pos().y() + dyj)
                if isinstance(owners[i], (ChronikNode, WorkNode)):
                    owners[i]._update_label_bg()
                if isinstance(owners[j], (ChronikNode, WorkNode)):
                    owners[j]._update_label_bg()
                moved += 1
        if moved == 0:
            break


def make_scene(
    G: nx.Graph,
    positions: Dict[str, Tuple[float, float]],
    min_edge_weight: int,
    show_labels: bool,
    theme: Optional[Theme] = None
) -> QtWidgets.QGraphicsScene:
    theme = theme or Theme.dark()
    scene = QtWidgets.QGraphicsScene()
    scene._theme = theme
    scene._show_labels = bool(show_labels)
    scene.setBackgroundBrush(QtGui.QBrush(QtGui.QColor(theme.bg)))

    node_items: Dict[str, QtWidgets.QGraphicsItem] = {}

    # Knoten
    for n, d in G.nodes(data=True):
        role = d.get("role")
        label = d.get("label", str(n))
        pos = positions.get(n, (0.0, 0.0))

        if role == "chronik":
            size = 7.0 + 2.8 * math.sqrt(max(1, int(d.get("mentions", 1))))
            item = ChronikNode(label, pos, size, theme, show_label=show_labels)
        else:
            size = 11.0 + 3.2 * math.sqrt(max(1, int(d.get("items", 1))))
            item = WorkNode(label, pos, size, theme, show_label=show_labels)

        node_items[n] = item
        scene.addItem(item)

    # Kanten
    kept_edges = 0
    for u, v, ed in G.edges(data=True):
        w = float(ed.get("weight", 1.0))
        if w < float(min_edge_weight):
            continue
        a = node_items[u]
        b = node_items[v]
        e = EdgeItem(a, b, w, theme)
        e.setZValue(-100)
        if isinstance(a, (ChronikNode, WorkNode)):
            a.edges.append(e)
        if isinstance(b, (ChronikNode, WorkNode)):
            b.edges.append(e)
        scene.addItem(e)
        kept_edges += 1

    # Label-Initialisierung radial + Kollisionsauflösung
    center = _center_from_pos(positions)
    _place_labels_radially(node_items, center)
    if show_labels:
        _resolve_label_collisions(node_items, center, iterations=100, step=4.0)

    debug(f"Scene erstellt: nodes={len(node_items)} edges_visible={kept_edges}")
    return scene


# ------------------------ Canvas ------------------------

class GraphCanvas(QtWidgets.QGraphicsView):
    def __init__(self):
        super().__init__()
        self.setRenderHint(QtGui.QPainter.Antialiasing, True)
        self.setDragMode(QtWidgets.QGraphicsView.ScrollHandDrag)
        self.setViewportUpdateMode(QtWidgets.QGraphicsView.SmartViewportUpdate)
        self.setTransformationAnchor(QtWidgets.QGraphicsView.AnchorUnderMouse)
        self.setStyleSheet("border: 0px;")
        self._nodes: Dict[str, QtWidgets.QGraphicsItem] = {}
        self._theme = Theme.dark()
        self._temp_nodes: List[QtWidgets.QGraphicsItem] = []
        self._temp_edges: List[EdgeItem] = []
        self._sticky_nodes: List[QtWidgets.QGraphicsItem] = []
        self._sticky_edges: List[EdgeItem] = []

    def _has_sticky(self) -> bool:
        return len(self._sticky_nodes) > 0

    def _belongs_here(self, it: QtWidgets.QGraphicsItem) -> bool:
        try:
            return not _is_dead(it) and it.scene() is self.scene()
        except Exception:
            return False

    def _cluster_edges_and_neighbors(self, node: QtWidgets.QGraphicsItem) -> Tuple[List[QtWidgets.QGraphicsItem], List[EdgeItem]]:
        neigh_nodes: List[QtWidgets.QGraphicsItem] = [node]
        edges: List[EdgeItem] = []
        if hasattr(node, "edges"):
            for e in list(node.edges):  # type: ignore[attr-defined]
                if _is_dead(e):
                    continue
                edges.append(e)
                other = e.a if e.b is node else e.b
                if isinstance(other, QtWidgets.QGraphicsItem):
                    neigh_nodes.append(other)
        seen = set(); uniq_nodes = []
        for it in neigh_nodes:
            if id(it) not in seen:
                uniq_nodes.append(it); seen.add(id(it))
        return uniq_nodes, edges

    def _apply_focus(self, node: QtWidgets.QGraphicsItem, sticky: bool) -> None:
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
            if isinstance(it, (ChronikNode, WorkNode)):
                if idx == 0:
                    it.apply_emphasis(scale=1.20, outline=_qcolor(theme.edge_sel), brighten=120)
                else:
                    it.apply_emphasis(scale=1.10, outline=_qcolor(theme.edge_sel, 200), brighten=110)
                if not show_labels:
                    it.toggle_label(True)
                (self._sticky_nodes if sticky else self._temp_nodes).append(it)

        for e in edges:
            if not self._belongs_here(e):
                continue
            e.set_highlight(True, theme)
            (self._sticky_edges if sticky else self._temp_edges).append(e)

        self.viewport().update()

    def _clear_nodes(self, items: List[QtWidgets.QGraphicsItem], restore_labels: bool) -> None:
        scn = self.scene()
        if not scn:
            return
        show_labels = bool(getattr(scn, "_show_labels", True))
        for it in list(items):
            if not self._belongs_here(it):
                continue
            if isinstance(it, (ChronikNode, WorkNode)):
                it.apply_emphasis(scale=1.0, outline=None, brighten=100)
                if not show_labels and restore_labels:
                    it.toggle_label(False)

    def _clear_edges(self, edges: List[EdgeItem]) -> None:
        theme: Theme = getattr(self.scene(), "_theme", self._theme) if self.scene() else self._theme
        for e in list(edges):
            if not self._belongs_here(e):
                continue
            e.set_highlight(False, theme)

    def _clear_temp_focus(self) -> None:
        self._clear_edges(self._temp_edges)
        self._clear_nodes(self._temp_nodes, restore_labels=True)
        self._temp_edges.clear(); self._temp_nodes.clear()
        self.viewport().update()

    def _clear_sticky_focus(self) -> None:
        self._clear_edges(self._sticky_edges)
        restore_labels = not bool(getattr(self.scene(), "_show_labels", True)) if self.scene() else True
        self._clear_nodes(self._sticky_nodes, restore_labels=restore_labels)
        self._sticky_edges.clear(); self._sticky_nodes.clear()
        self.viewport().update()

    def _safe_clear_all_before_scene_swap(self) -> None:
        try:
            self._clear_temp_focus()
            self._clear_sticky_focus()
        except Exception:
            self._temp_edges.clear(); self._temp_nodes.clear()
            self._sticky_edges.clear(); self._sticky_nodes.clear()

    def set_graph_scene(self, scene: QtWidgets.QGraphicsScene) -> None:
        self._safe_clear_all_before_scene_swap()
        super().setScene(scene)
        self._nodes.clear()
        node_count = 0; edge_count = 0
        for it in scene.items():
            if isinstance(it, (ChronikNode, WorkNode)):
                self._nodes[it.name] = it  # type: ignore[attr-defined]
                node_count += 1
            elif isinstance(it, (QtWidgets.QGraphicsPathItem, QtWidgets.QGraphicsLineItem)):
                edge_count += 1
        self._theme = getattr(scene, "_theme", Theme.dark())
        self.setStyleSheet(f"QGraphicsView {{ background:{self._theme.bg}; }}")
        self.fitInView(scene.itemsBoundingRect(), Qt.KeepAspectRatio)
        debug(f"Neu gezeichnet: nodes={node_count} edges={edge_count}")

    def wheelEvent(self, event: QtGui.QWheelEvent) -> None:
        factor = 1.15 if event.angleDelta().y() > 0 else 1 / 1.15
        self.scale(factor, factor)

    def mousePressEvent(self, event: QtGui.QMouseEvent) -> None:
        scn = self.scene()
        if scn and event.button() == Qt.LeftButton:
            pos = self.mapToScene(event.pos())
            clicked_items = scn.items(pos)
            target = next((it for it in clicked_items if isinstance(it, (ChronikNode, WorkNode))), None)
            if target is not None and self._belongs_here(target):
                self._apply_focus(target, sticky=True)
            else:
                self._clear_sticky_focus(); self._clear_temp_focus()
        super().mousePressEvent(event)

    def keyPressEvent(self, event: QtGui.QKeyEvent) -> None:
        k = event.key()
        if k in (Qt.Key_Plus, Qt.Key_Equal): self.scale(1.15, 1.15); return
        if k == Qt.Key_Minus: self.scale(1/1.15, 1/1.15); return
        if k == Qt.Key_F and self.scene(): self.fitInView(self.scene().itemsBoundingRect(), Qt.KeepAspectRatio); return
        if k == Qt.Key_Escape: self._clear_temp_focus(); self._clear_sticky_focus(); return
        super().keyPressEvent(event)

    def highlight(self, term: str, show_labels: bool) -> int:
        term = term.strip().lower()
        hits = 0
        for n, item in list(self._nodes.items()):
            hit = bool(term and term in n.lower())
            if isinstance(item, (ChronikNode, WorkNode)) and self._belongs_here(item):
                item.set_highlight(hit)
                item.toggle_label(show_labels or hit)
            hits += int(hit)
        return hits

    def export_png(self, path: str) -> None:
        if not self.scene():
            raise RuntimeError("Keine Szene geladen.")
        rect = self.scene().itemsBoundingRect().adjusted(-20, -20, 20, 20)
        img = QtGui.QImage(int(rect.width()), int(rect.height()), QtGui.QImage.Format_ARGB32)
        img.fill(QtGui.QColor(self._theme.bg))
        painter = QtGui.QPainter(img)
        painter.setRenderHint(QtGui.QPainter.Antialiasing, True)
        self.scene().render(painter, QtCore.QRectF(img.rect()), rect)
        painter.end()
        if not img.save(path):
            raise IOError(f"PNG konnte nicht gespeichert werden: {path}")
        debug(f"PNG exportiert: {path}")


# ------------------------ Demo (optional) ------------------------

def _try_candidates() -> Optional[str]:
    env = os.environ.get("CHRONIKEN_MENTIONS_CSV", "").strip()
    cands = [
        env if env else "",
        os.path.join(os.getcwd(), "chroniken_mentions.csv"),
        os.path.expanduser("~/PycharmProjects/Find_Bibliography_NEw/data/azk_library/chroniken_mentions.csv"),
    ]
    for p in cands:
        if p and os.path.isfile(p):
            return p
    return None


def main() -> None:
    debug("Starte Viewer …")
    csv = _try_candidates()
    try:
        QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling, True)
        QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_UseHighDpiPixmaps, True)
    except Exception:
        pass
    app = QtWidgets.QApplication([])
    win = QtWidgets.QMainWindow()
    win.setWindowTitle("chroniken_library↔Werke — Viewer (network.py)")
    view = GraphCanvas()
    win.setCentralWidget(view)
    win.resize(1300, 850)
    if csv:
        df = load_mentions_csv(csv)
        doc_col, work_col = resolve_columns(df)
        G = build_bipartite_graph(df, doc_col, work_col)
        pos = compute_layout(G, "forceatlas2")
        scene = make_scene(G, pos, min_edge_weight=1, show_labels=True, theme=Theme.dark())
        view.set_graph_scene(scene)
    else:
        debug("Hinweis: Keine CSV gefunden. Über GUI laden.")
    win.show()
    app.exec_()


if __name__ == "__main__":
    main()