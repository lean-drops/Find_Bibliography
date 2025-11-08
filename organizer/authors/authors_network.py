#!/usr/bin/env python3
"""
authors_network.py
Reines Netzwerk-Modul + Canvas für Autoren-/Chroniken-Netze.

API (für dein GUI):
  load_mentions_csv(path) -> pd.DataFrame
  resolve_columns(df) -> (doc_col, work_col)
  build_bipartite_graph(df, doc_col, work_col) -> nx.Graph
  build_graph_edges(df, src_col, tgt_col, weight_col|None) -> nx.DiGraph
  compute_layout(G, mode='bipartite'|'spring'|'kamada_kawai'|'forceatlas2', bip_gap=4.0) -> Dict[node,(x,y)]
  make_scene(G, positions, min_edge_weight, show_labels, theme, direction, arrow_scale, high_contrast) -> QGraphicsScene
  GraphCanvas(QtWidgets.QGraphicsView) mit:
      set_graph_scene(scene)
      highlight(term, show_labels)
      set_focus_direction('out'|'in')  (Tastenkürzel: O = out, I = in)

Neues Verhalten (auf Klick/Sticky):
  • Es werden NUR die Kanten in einer Richtung gezeigt: ausgehend ODER eingehend (umschalten mit O/I).
  • Knoten werden mitgefärbt: Fokus, Nachbarn (Richtungsspezifisch), alle anderen gedimmt.
  • Labels sind größer, kontrastreicher, sofort sichtbar für Fokus + Nachbarn.

Abhängigkeiten: PyQt5, pandas, numpy, networkx
"""
from __future__ import annotations

import math
import os
import sys
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, List

import numpy as np
import pandas as pd
import networkx as nx
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import Qt


# ---------------------------- Debug ----------------------------

def debug(msg: str) -> None:
    print(f"[DEBUG] {msg}", flush=True)


# ---------------------------- CSV / Graphbau ----------------------------

def load_mentions_csv(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"CSV nicht gefunden: {path}")
    for sep in (';', ',', None):
        try:
            df = pd.read_csv(path, sep=sep, engine='python')
            if df.empty:
                raise ValueError("CSV ist leer.")
            return df
        except Exception:
            continue
    raise ValueError(f"Konnte CSV nicht lesen: {path}")


def resolve_columns(df: pd.DataFrame) -> tuple[str, str]:
    doc_candidates = ["pdf_file", "pdf", "document", "source", "file", "filename", "doc"]
    work_candidates = ["label", "canonical", "work", "title", "chronik", "edition", "name", "match", "normalized"]
    lower = {c.lower(): c for c in df.columns}
    doc_col = next((lower[c] for c in doc_candidates if c in lower), None)
    work_col = next((lower[c] for c in work_candidates if c in lower), None)
    if not doc_col:
        raise KeyError(f"Dokumentspalte nicht erkannt. Erwartet eine aus {doc_candidates}.")
    if not work_col:
        raise KeyError(f"Werk/Chronik-Spalte nicht erkannt. Erwartet eine aus {work_candidates}.")
    return doc_col, work_col


def _id_pdf(path: str) -> str:
    return f"P|{os.path.basename(path)}"


def _id_chronik(name: str) -> str:
    return f"C|{name}"


def build_bipartite_graph(df: pd.DataFrame, doc_col: str, work_col: str) -> nx.Graph:
    df = df[[doc_col, work_col]].dropna().copy()
    df[doc_col] = df[doc_col].astype(str).str.strip()
    df[work_col] = df[work_col].astype(str).str.strip()
    pairs = df.groupby([doc_col, work_col]).size().reset_index(name="w")

    chronik_docs = pairs.groupby(work_col)[doc_col].nunique().to_dict()
    pdf_chroniks = pairs.groupby(doc_col)[work_col].nunique().to_dict()

    G = nx.Graph(name="Mentions")
    for ch, n_docs in chronik_docs.items():
        G.add_node(_id_chronik(ch), label=ch, role="chronik", bipartite=0, mentions=int(n_docs))
    for pdf, n_ch in pdf_chroniks.items():
        G.add_node(_id_pdf(pdf), label=os.path.basename(pdf) or pdf, role="work", bipartite=1, items=int(n_ch))
    for _, r in pairs.iterrows():
        G.add_edge(_id_pdf(r[doc_col]), _id_chronik(r[work_col]), weight=float(r["w"]))
    G.graph["kind"] = "mentions"
    debug(f"Graph Mentions: nodes={G.number_of_nodes()} edges={G.number_of_edges()}")
    return G


def build_graph_edges(df: pd.DataFrame, src_col: str, tgt_col: str, weight_col: Optional[str]) -> nx.DiGraph:
    cols = [src_col, tgt_col] + ([weight_col] if weight_col and weight_col in df.columns else [])
    wk = df[cols].dropna().copy()
    wk[src_col] = wk[src_col].astype(str).str.strip()
    wk[tgt_col] = wk[tgt_col].astype(str).str.strip()
    if weight_col and weight_col in wk.columns:
        wk[weight_col] = pd.to_numeric(wk[weight_col], errors="coerce").fillna(0.0)
        agg = wk.groupby([src_col, tgt_col], as_index=False)[weight_col].sum().rename(columns={weight_col: "w"})
    else:
        agg = wk.groupby([src_col, tgt_col], as_index=False).size().rename(columns={"size": "w"})
    G = nx.DiGraph(name="Authors")
    for a in pd.unique(pd.concat([agg[src_col], agg[tgt_col]])):
        G.add_node(a, label=a, role="author")
    for _, r in agg.iterrows():
        G.add_edge(r[src_col], r[tgt_col], weight=float(r["w"]))
    G.graph["kind"] = "edges"
    debug(f"Graph Edges: nodes={G.number_of_nodes()} edges={G.number_of_edges()}")
    return G


# ---------------------------- Layouts ----------------------------

def _node_mass(G: nx.Graph, n: str) -> float:
    d = G.nodes[n]
    if d.get("role") == "chronik":
        return 1.0 + math.sqrt(max(1, int(d.get("mentions", 1))))
    if d.get("role") == "work":
        return 1.0 + math.sqrt(max(1, int(d.get("items", 1))))
    return 1.0 + math.sqrt(max(1, G.degree(n)))


def _bipartite_ordered_layout(G: nx.Graph, x_gap: float = 4.0) -> Dict[str, Tuple[float, float]]:
    left = [n for n, d in G.nodes(data=True) if d.get("role") == "work"]
    right = [n for n, d in G.nodes(data=True) if d.get("role") == "chronik"]

    def _order(side: List[str], other: List[str]) -> List[str]:
        idx = {n: i for i, n in enumerate(other)}
        scores = {}
        for n in side:
            neigh = list(G.neighbors(n))
            if not neigh:
                scores[n] = (0.0, 1e-9); continue
            s = w = 0.0
            for m in neigh:
                mm = _node_mass(G, m)
                s += idx.get(m, 0) * mm; w += mm
            scores[n] = (s, w)
        return sorted(side, key=lambda k: scores[k][0] / scores[k][1])

    for _ in range(2):
        left = _order(left, right)
        right = _order(right, left)

    pos: Dict[str, Tuple[float, float]] = {}
    def _coords(lst: List[str], xval: float) -> None:
        n = max(1, len(lst))
        for i, node in enumerate(lst):
            pos[node] = (xval, (i - (n - 1) / 2.0) * 1.0)

    _coords(left, -x_gap)
    _coords(right, x_gap)
    return pos


def _forceatlas2(G: nx.Graph,
                 init_pos: Optional[Dict[str, Tuple[float, float]]] = None,
                 iterations: int = 300, gravity: float = 0.05,
                 scaling: float = 1.1, dt: float = 0.08) -> Dict[str, Tuple[float, float]]:
    import random
    nodes = list(G.nodes())
    if not nodes:
        return {}
    pos = {n: list(init_pos[n]) if init_pos and n in init_pos else [random.uniform(-1, 1), random.uniform(-1, 1)] for n in nodes}
    mass = {n: _node_mass(G, n) for n in nodes}

    for _ in range(iterations):
        fx = {n: 0.0 for n in nodes}; fy = {n: 0.0 for n in nodes}
        for i in range(len(nodes)):
            n1 = nodes[i]; x1, y1 = pos[n1]
            for j in range(i + 1, len(nodes)):
                n2 = nodes[j]; x2, y2 = pos[n2]
                dx, dy = x1 - x2, y1 - y2
                dist = math.hypot(dx, dy) + 1e-9
                f = (scaling * mass[n1] * mass[n2]) / dist
                rx, ry = f * (dx / dist), f * (dy / dist)
                fx[n1] += rx; fy[n1] += ry; fx[n2] -= rx; fy[n2] -= ry
        for u, v, ed in G.edges(data=True):
            w = float(ed.get("weight", 1.0))
            x1, y1 = pos[u]; x2, y2 = pos[v]
            dx, dy = x2 - x1, y2 - y1
            dist = math.hypot(dx, dy) + 1e-9
            f = w * dist
            ax, ay = f * (dx / dist), f * (dy / dist)
            fx[u] += ax; fy[u] += ay; fx[v] -= ax; fy[v] -= ay
        for n in nodes:
            x, y = pos[n]; fx[n] += -gravity * x; fy[n] += -gravity * y
        max_disp = 0.0
        for n in nodes:
            inv = 1.0 / mass[n]
            dx, dy = dt * fx[n] * inv, dt * fy[n] * inv
            pos[n][0] += dx; pos[n][1] += dy
            max_disp = max(max_disp, abs(dx) + abs(dy))
        if max_disp < 1e-4: break
    return {n: (float(x), float(y)) for n, (x, y) in pos.items()}


def _relax_positions(G: nx.Graph, pos: Dict[str, Tuple[float, float]],
                     iterations: int = 140, base_dist: float = 0.35, step: float = 0.05) -> Dict[str, Tuple[float, float]]:
    nodes = list(pos.keys());
    if len(nodes) <= 1: return pos
    radius = {n: 0.05 + 0.02 * math.sqrt(_node_mass(G, n)) for n in nodes}
    p = {n: [float(x), float(y)] for n,(x,y) in pos.items()}
    for _ in range(iterations):
        moved = 0
        for i,n1 in enumerate(nodes):
            x1,y1 = p[n1]; fx=fy=0.0
            for j,n2 in enumerate(nodes):
                if i==j: continue
                x2,y2=p[n2]; dx,dy=x1-x2,y1-y2; dist=math.hypot(dx,dy)+1e-6
                want = base_dist + radius[n1] + radius[n2]
                if dist < want:
                    f=(want-dist)/want; fx+=(dx/dist)*f; fy+=(dy/dist)*f
            if fx or fy:
                p[n1]=[x1+fx*step, y1+fy*step]; moved+=1
        if moved==0: break
    return {n:(float(x),float(y)) for n,(x,y) in p.items()}


def compute_layout(G: nx.Graph, mode: str = "bipartite", bip_gap: float = 4.0) -> Dict[str, Tuple[float, float]]:
    if G.number_of_nodes()==0: return {}
    if mode=="bipartite" and G.graph.get("kind")=="mentions":
        pos0=_bipartite_ordered_layout(G, x_gap=bip_gap)
        return _relax_positions(G, pos0, iterations=160, base_dist=0.35, step=0.06)
    if mode=="kamada_kawai":
        pos0=nx.kamada_kawai_layout(G, weight="weight")
        return _relax_positions(G, {str(n):(float(x),float(y)) for n,(x,y) in pos0.items()},
                                iterations=120, base_dist=0.3, step=0.05)
    if mode=="forceatlas2":
        init=_bipartite_ordered_layout(G, x_gap=1.2) if G.graph.get("kind")=="mentions" else None
        pos0=_forceatlas2(G, init_pos=init, iterations=350, gravity=0.06, scaling=1.2, dt=0.08)
        return _relax_positions(G, pos0, iterations=140, base_dist=0.32, step=0.05)
    pos0=nx.spring_layout(G, weight="weight", iterations=350, seed=42)
    return _relax_positions(G, {str(n):(float(x),float(y)) for n,(x,y) in pos0.items()},
                            iterations=140, base_dist=0.32, step=0.05)


# ---------------------------- Theme ----------------------------

@dataclass
class Theme:
    bg: str = "#0b0f14"
    label: str = "#e6edf6"
    label_bg: str = "#0b0f14"
    chronik_fill: str = "#1fb6ff"; chronik_stroke: str = "#073a53"
    work_fill: str = "#fbbf24";  work_stroke: str = "#3a2c0b"
    author_fill: str = "#a78bfa"; author_stroke: str = "#2b1e52"
    edge_neutral: str = "#8b93a5"
    dir_out: str = "#22d3ee"    # ausgehend
    dir_in:  str = "#f43f5e"    # eingehend
    dir_neutral_dim: str = "#374151"


def _qcolor(hex_code: str, alpha: Optional[int] = None) -> QtGui.QColor:
    c = QtGui.QColor(hex_code)
    if alpha is not None:
        c.setAlpha(max(0, min(255, int(alpha))))
    return c


# ---------------------------- Graphics Items ----------------------------

try:
    import sip  # type: ignore
except Exception:
    sip = None


def _is_dead(obj: object) -> bool:
    if obj is None: return True
    if sip is not None:
        try: return sip.isdeleted(obj)  # type: ignore[attr-defined]
        except Exception: return True
    try: return getattr(obj, "scene", None) is None and False
    except Exception: return True


def _node_radius_px(item: QtWidgets.QGraphicsItem) -> float:
    r = item.boundingRect(); return 0.5 * max(r.width(), r.height())


def _bezier_point(p0: QtCore.QPointF, p1: QtCore.QPointF, p2: QtCore.QPointF, p3: QtCore.QPointF, t: float) -> QtCore.QPointF:
    u=1.0-t
    return QtCore.QPointF((u**3)*p0.x()+3*u*u*t*p1.x()+3*u*t*t*p2.x()+(t**3)*p3.x(),
                          (u**3)*p0.y()+3*u*u*t*p1.y()+3*u*t*t*p2.y()+(t**3)*p3.y())


def _bezier_tangent(p0: QtCore.QPointF, p1: QtCore.QPointF, p2: QtCore.QPointF, p3: QtCore.QPointF, t: float) -> QtCore.QPointF:
    u=1.0-t
    return QtCore.QPointF(3*u*u*(p1.x()-p0.x())+6*u*t*(p2.x()-p1.x())+3*t*t*(p3.x()-p2.x()),
                          3*u*u*(p1.y()-p0.y())+6*u*t*(p2.y()-p1.y())+3*t*t*(p3.y()-p2.y()))


class EdgeItem(QtWidgets.QGraphicsPathItem):
    """Kante mit stark sichtbarer Richtung: Halo, Schaft, Spitze, Chevrons."""
    def __init__(self, a: QtWidgets.QGraphicsItem, b: QtWidgets.QGraphicsItem,
                 src_item: QtWidgets.QGraphicsItem, dst_item: QtWidgets.QGraphicsItem,
                 weight: float, theme: Theme, arrow_to_b: bool,
                 arrow_scale: float = 1.6, high_contrast: bool = True):
        super().__init__()
        self.a=a; self.b=b; self.src_item=src_item; self.dst_item=dst_item
        self.weight=max(1.0, float(weight)); self._theme=theme
        self._arrow_to_b=bool(arrow_to_b); self._arrow_scale=float(arrow_scale)
        self._high_contrast=bool(high_contrast)
        self.setZValue(-101); self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)

        # Halo
        self.halo = QtWidgets.QGraphicsPathItem(self); self.halo.setZValue(-103)
        self.halo.setPen(QtGui.QPen(_qcolor(theme.edge_neutral, 200 if high_contrast else 140),
                                    10.0, Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin))
        self.halo.setBrush(QtGui.QBrush(Qt.NoBrush))

        # Schaft
        pen = QtGui.QPen(_qcolor(theme.edge_neutral, 240 if high_contrast else 220),
                         max(2.0, 1.2+math.sqrt(self.weight)), Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin)
        pen.setCosmetic(True); self.setPen(pen)

        # Spitzen
        self.arrow = QtWidgets.QGraphicsPolygonItem(self); self.arrow.setZValue(-60)
        self.arrow.setPen(QtGui.QPen(_qcolor("#0b0f14", 230), 2))
        self.arrow.setBrush(QtGui.QBrush(_qcolor(theme.edge_neutral, 255)))
        self.chev1 = QtWidgets.QGraphicsPolygonItem(self); self.chev1.setZValue(-61)
        self.chev1.setPen(QtGui.QPen(_qcolor("#0b0f14", 220), 1))
        self.chev1.setBrush(QtGui.QBrush(_qcolor(theme.edge_neutral, 255)))
        self.chev2 = QtWidgets.QGraphicsPolygonItem(self); self.chev2.setZValue(-61)
        self.chev2.setPen(QtGui.QPen(_qcolor("#0b0f14", 220), 1))
        self.chev2.setBrush(QtGui.QBrush(_qcolor(theme.edge_neutral, 255)))

        self.mode="neutral"
        self.update_position()

    def _apply_color(self, hx: str) -> None:
        pen=self.pen(); pen.setColor(_qcolor(hx, 255 if self._high_contrast else 230))
        pen.setWidthF(max(2.0, 1.2+math.sqrt(self.weight))); self.setPen(pen)
        hpen=self.halo.pen(); hpen.setColor(_qcolor(hx, 180 if self._high_contrast else 130))
        hpen.setWidth(12 if self._high_contrast else 10); self.halo.setPen(hpen)
        self.arrow.setBrush(QtGui.QBrush(_qcolor(hx, 255)))
        self.chev1.setBrush(QtGui.QBrush(_qcolor(hx, 255)))
        self.chev2.setBrush(QtGui.QBrush(_qcolor(hx, 255)))

    def set_mode(self, mode: str) -> None:
        self.mode=mode
        if mode=="in": self._apply_color(self._theme.dir_in)
        elif mode=="out": self._apply_color(self._theme.dir_out)
        else: self._apply_color(self._theme.edge_neutral)

    def update_position(self) -> None:
        if _is_dead(self.a) or _is_dead(self.b): return
        p1=self.a.pos(); p2=self.b.pos(); dx=p2.x()-p1.x(); k=0.18*abs(dx)
        c1=QtCore.QPointF(p1.x()+0.25*dx, p1.y()-k); c2=QtCore.QPointF(p2.x()-0.25*dx, p2.y()+k)
        path=QtGui.QPainterPath(p1); path.cubicTo(c1,c2,p2); self.setPath(path); self.halo.setPath(path)
        if self._arrow_to_b:
            tip,unit=self._offset_tip_tangent(p1,c1,c2,p2,self.b)
        else:
            tip,unit=self._offset_tip_tangent(p2,c2,c1,p1,self.a)
        self._place_arrow_polys(tip, unit, self._arrow_scale)
        self._place_chevrons(p1,c1,c2,p2,self._arrow_scale)

    def _offset_tip_tangent(self, p0,p1,p2,p3,target)->tuple[QtCore.QPointF,QtCore.QPointF]:
        t=0.98; tip_raw=_bezier_point(p0,p1,p2,p3,t); tan=_bezier_tangent(p0,p1,p2,p3,t)
        L=max(1e-9, math.hypot(tan.x(),tan.y())); ux,uy=tan.x()/L, tan.y()/L
        tip=QtCore.QPointF(tip_raw.x()-ux*(_node_radius_px(target)+6.0),
                           tip_raw.y()-uy*(_node_radius_px(target)+6.0))
        return tip, QtCore.QPointF(ux,uy)

    def _place_arrow_polys(self, tip, unit, scale)->None:
        L=max(20.0, 12.0+4.0*math.sqrt(self.weight))*scale; W=0.66*L
        bx=tip.x()-unit.x()*L; by=tip.y()-unit.y()*L; px,py=-unit.y(), unit.x()
        left=QtCore.QPointF(bx+px*(W/2.0), by+py*(W/2.0))
        right=QtCore.QPointF(bx-px*(W/2.0), by-py*(W/2.0))
        self.arrow.setPolygon(QtGui.QPolygonF([tip,left,right]))

    def _place_chevrons(self, p0,p1,p2,p3, scale)->None:
        for t,obj in ((0.35,self.chev1),(0.65,self.chev2)):
            pt=_bezier_point(p0,p1,p2,p3,t); tan=_bezier_tangent(p0,p1,p2,p3,t)
            L=max(1e-9, math.hypot(tan.x(),tan.y())); ux,uy=tan.x()/L, tan.y()/L
            Lc=max(12.0,8.0+2.5*math.sqrt(self.weight))*(scale*0.55); Wc=0.58*Lc
            bx=pt.x()-ux*Lc; by=pt.y()-uy*Lc; px,py=-uy, ux
            left=QtCore.QPointF(bx+px*(Wc/2.0), by+py*(Wc/2.0))
            right=QtCore.QPointF(bx-px*(Wc/2.0), by-py*(Wc/2.0))
            obj.setPolygon(QtGui.QPolygonF([pt,left,right]))


class NodeItem(QtWidgets.QGraphicsEllipseItem):
    def __init__(self, name: str, pos: Tuple[float,float], size: float,
                 fill: str, stroke: str, show_label: bool, theme: Theme):
        super().__init__(-size, -size, 2*size, 2*size)
        self.name=name
        self._fill_base = QtGui.QColor(fill)
        self._stroke_base = QtGui.QColor(stroke)
        self._theme = theme

        self.setAcceptHoverEvents(True)
        self.setPos(pos[0]*300.0, pos[1]*300.0)
        self.setBrush(QtGui.QBrush(self._fill_base))
        pen=QtGui.QPen(self._stroke_base); pen.setWidth(1); pen.setCosmetic(True); self.setPen(pen)
        self.setCacheMode(QtWidgets.QGraphicsItem.DeviceCoordinateCache)
        self.setFlag(QtWidgets.QGraphicsItem.ItemIsMovable, True)
        self.setFlag(QtWidgets.QGraphicsItem.ItemSendsGeometryChanges, True)
        self.setFlag(QtWidgets.QGraphicsItem.ItemIsSelectable, True)
        self.edges: List[EdgeItem]=[]

        # Label: größer, besser lesbar
        self._label_bg = QtWidgets.QGraphicsRectItem(self)
        self._label_bg.setBrush(QtGui.QBrush(_qcolor(theme.label_bg, 210)))
        self._label_bg.setPen(QtGui.QPen(Qt.NoPen))
        self._label_bg.setFlag(QtWidgets.QGraphicsItem.ItemIgnoresTransformations, True)
        self._label_bg.setVisible(show_label)

        self.label = QtWidgets.QGraphicsSimpleTextItem(name if show_label else "", self)
        font = QtGui.QFont()
        font.setPointSize(12)
        font.setBold(True)
        self.label.setFont(font)
        self.label.setBrush(QtGui.QBrush(QtGui.QColor(theme.label)))
        self.label.setFlag(QtWidgets.QGraphicsItem.ItemIgnoresTransformations, True)
        self.label.setPos(size+10, -12)
        br=self.label.boundingRect(); pad=4.0
        self._label_bg.setRect(QtCore.QRectF(br.x()-pad, br.y()-pad, br.width()+2*pad, br.height()+2*pad))

    def set_style(self, role: str) -> None:
        """
        role: 'focus'|'neighbor_out'|'neighbor_in'|'muted'|'base'
        """
        if role == "focus":
            fill = QtGui.QColor(self._fill_base).lighter(130)
            pen = QtGui.QPen(_qcolor("#ffffff", 180)); pen.setWidth(2); pen.setCosmetic(True)
        elif role == "neighbor_out":
            fill = _qcolor(self._theme.dir_out, 220)
            pen = QtGui.QPen(_qcolor(self._theme.dir_out, 255)); pen.setWidth(2); pen.setCosmetic(True)
        elif role == "neighbor_in":
            fill = _qcolor(self._theme.dir_in, 220)
            pen = QtGui.QPen(_qcolor(self._theme.dir_in, 255)); pen.setWidth(2); pen.setCosmetic(True)
        elif role == "muted":
            fill = _qcolor(self._theme.dir_neutral_dim, 120)
            pen = QtGui.QPen(_qcolor(self._theme.dir_neutral_dim, 180)); pen.setWidth(1); pen.setCosmetic(True)
        else:
            fill = self._fill_base
            pen = QtGui.QPen(self._stroke_base); pen.setWidth(1); pen.setCosmetic(True)

        self.setBrush(QtGui.QBrush(fill))
        self.setPen(pen)

    def toggle_label(self, show: bool)->None:
        self.label.setText(self.name if show else "")
        self._label_bg.setVisible(show)
        if show:
            br=self.label.boundingRect(); pad=4.0
            self._label_bg.setRect(QtCore.QRectF(br.x()-pad, br.y()-pad, br.width()+2*pad, br.height()+2*pad))

    def itemChange(self, change, value):
        if change==QtWidgets.QGraphicsItem.ItemPositionHasChanged:
            for e in list(self.edges):
                if not _is_dead(e): e.update_position()
        return super().itemChange(change, value)


# ---------------------------- Scene / Canvas ----------------------------

def make_scene(
    G: nx.Graph,
    positions: Dict[str, Tuple[float, float]],
    min_edge_weight: int,
    show_labels: bool,
    theme: Optional[Theme] = None,
    direction: str = "citing",
    arrow_scale: float = 1.6,
    high_contrast: bool = True
) -> QtWidgets.QGraphicsScene:
    theme = theme or Theme()
    scene = QtWidgets.QGraphicsScene()
    scene._theme = theme
    scene._show_labels = bool(show_labels)
    scene._direction = "citing" if str(direction).lower().startswith("cit") else "cited"
    scene._kind = G.graph.get("kind", "edges")
    scene.setBackgroundBrush(QtGui.QBrush(QtGui.QColor(theme.bg)))

    node_items: Dict[str, NodeItem] = {}

    for n, d in G.nodes(data=True):
        label = d.get("label", str(n))
        pos = positions.get(n, (0.0, 0.0))
        role = d.get("role", "author")
        if role == "chronik":
            size = 7.0 + 2.8 * math.sqrt(max(1, int(d.get("mentions", 1))))
            it = NodeItem(label, pos, size, theme.chronik_fill, theme.chronik_stroke, show_labels, theme)
        elif role == "work":
            size = 11.0 + 3.2 * math.sqrt(max(1, int(d.get("items", 1))))
            it = NodeItem(label, pos, size, theme.work_fill, theme.work_stroke, show_labels, theme)
        else:
            size = 9.0 + 2.0 * math.sqrt(1 + G.degree(n))
            it = NodeItem(label, pos, size, theme.author_fill, theme.author_stroke, show_labels, theme)
        node_items[n] = it; scene.addItem(it)

    kept = 0
    for u, v, ed in G.edges(data=True):
        w = float(ed.get("weight", 1.0))
        if w < float(min_edge_weight):
            continue
        a = node_items[u]; b = node_items[v]
        if scene._kind == "mentions":
            # Logik: work → chronik
            src_item = a if G.nodes[u].get("role") == "work" else b
            dst_item = b if src_item is a else a
            arrow_to_b = (scene._direction == "citing")
            if src_item is b: arrow_to_b = not arrow_to_b
        else:
            # Autor→Autor: u→v als Logik
            src_item, dst_item = a, b
            arrow_to_b = (scene._direction == "citing")
        e = EdgeItem(a, b, src_item, dst_item, w, theme, arrow_to_b,
                     arrow_scale=arrow_scale, high_contrast=high_contrast)
        e.set_mode("neutral")
        a.edges.append(e); b.edges.append(e)
        scene.addItem(e); kept += 1

    debug(f"Scene erstellt: nodes={len(node_items)} edges_visible={kept} dir={scene._direction} kind={scene._kind}")
    return scene


class GraphCanvas(QtWidgets.QGraphicsView):
    """Zoom, Pan, Klick-Fokus. Zeigt EITHER ausgehende ODER eingehende Kanten; färbt verbundene Knoten; Labels gut lesbar."""
    def __init__(self):
        super().__init__()
        self.setRenderHint(QtGui.QPainter.Antialiasing, True)
        self.setDragMode(QtWidgets.QGraphicsView.ScrollHandDrag)
        self.setViewportUpdateMode(QtWidgets.QGraphicsView.SmartViewportUpdate)
        self.setTransformationAnchor(QtWidgets.QGraphicsView.AnchorUnderMouse)
        self.setStyleSheet("border: 0px;")
        self._nodes: Dict[str, NodeItem] = {}
        self._theme = Theme()
        self._sticky: Optional[NodeItem] = None
        self._nav: List[NodeItem] = []
        self._nav_idx = -1
        self._focus_dir: str = "out"  # 'out' oder 'in'

    # ---------- Public API ----------
    def set_graph_scene(self, scene: QtWidgets.QGraphicsScene) -> None:
        prev = self.scene()
        if prev is not None:
            try:
                for it in prev.items():
                    if isinstance(it, EdgeItem): it.set_mode("neutral")
            except Exception:
                pass
        super().setScene(scene)
        self._nodes.clear()
        for it in scene.items():
            if isinstance(it, NodeItem):
                self._nodes[it.name] = it
        self._theme = getattr(scene, "_theme", Theme())
        self.setStyleSheet(f"QGraphicsView {{ background:{self._theme.bg}; }}")
        try:
            self.fitInView(scene.itemsBoundingRect(), Qt.KeepAspectRatio)
        except Exception:
            pass
        self._sticky = None; self._nav.clear(); self._nav_idx = -1
        debug(f"Neu gezeichnet: nodes={len(self._nodes)}")

    def highlight(self, term: str, show_labels: bool) -> int:
        term = term.strip().lower(); hits = 0
        for name, node in self._nodes.items():
            hit = term and term in name.lower()
            node.toggle_label(show_labels or bool(hit))
            hits += int(bool(hit))
        return hits

    def set_focus_direction(self, mode: str) -> None:
        mode = (mode or "").lower().strip()
        self._focus_dir = "in" if mode.startswith("in") else "out"
        # Re-apply current focus if any
        if self._sticky:
            self._apply_focus(self._sticky, sticky=True)

    # ---------- Intern ----------
    def _belongs_here(self, it: QtWidgets.QGraphicsItem) -> bool:
        try: return not _is_dead(it) and it.scene() is self.scene()
        except Exception: return False

    def _collect_edges(self) -> List[EdgeItem]:
        scn = self.scene()
        return [it for it in scn.items() if isinstance(it, EdgeItem)] if scn else []

    def _cluster(self, node: NodeItem) -> tuple[List[EdgeItem], List[EdgeItem]]:
        incoming: List[EdgeItem] = []; outgoing: List[EdgeItem] = []
        for e in getattr(node, "edges", []):
            if _is_dead(e): continue
            if e.dst_item is node: incoming.append(e)
            if e.src_item is node: outgoing.append(e)
        return incoming, outgoing

    def _reset_nodes(self) -> None:
        for node in self._nodes.values():
            node.set_style("base")
            node.toggle_label(False)

    def _apply_focus(self, node: NodeItem, sticky: bool) -> None:
        scn = self.scene()
        if not scn or not self._belongs_here(node): return
        if sticky and self._sticky and self._sticky is not node: self._clear_sticky()

        incoming, outgoing = self._cluster(node)
        show_edges = outgoing if self._focus_dir == "out" else incoming
        hide_edges = incoming+outgoing
        # 1) Kanten-Sichtbarkeit global
        for e in self._collect_edges():
            is_on = (e in show_edges)
            e.setVisible(is_on)
            if is_on:
                e.set_mode("out" if self._focus_dir == "out" else "in")

        # 2) Knoten-Stil
        self._reset_nodes()
        node.set_style("focus"); node.toggle_label(True)

        # Nachbarn bestimmen (nur sichtbare Richtung)
        neighbors: List[NodeItem] = []
        for e in show_edges:
            nbr = e.dst_item if self._focus_dir == "out" else e.src_item
            if isinstance(nbr, NodeItem) and nbr not in neighbors:
                neighbors.append(nbr)

        for nbr in neighbors:
            nbr.set_style("neighbor_out" if self._focus_dir == "out" else "neighbor_in")
            nbr.toggle_label(True)

        # alle anderen dimmen
        dimmed = set(self._nodes.values()) - {node} - set(neighbors)
        for n in dimmed:
            n.set_style("muted")

        if sticky:
            self._sticky = node
            self._nav = sorted(neighbors, key=lambda x: x.name.lower())
            self._nav_idx = -1

        self.viewport().update()

    def _clear_sticky(self) -> None:
        self._sticky = None; self._nav.clear(); self._nav_idx = -1
        # Alles wieder sichtbar machen und auf neutral setzen
        for e in self._collect_edges():
            e.setVisible(True)
            e.set_mode("neutral")
        self._reset_nodes()
        self.viewport().update()

    # ---------- Events ----------
    def mousePressEvent(self, e: QtGui.QMouseEvent) -> None:
        scn = self.scene()
        if scn and e.button() == Qt.LeftButton:
            pos = self.mapToScene(e.pos()); clicked = scn.items(pos)
            node = next((it for it in clicked if isinstance(it, NodeItem)), None)
            if node: self._apply_focus(node, sticky=True)
            else: self._clear_sticky()
        super().mousePressEvent(e)

    def keyPressEvent(self, e: QtGui.QKeyEvent) -> None:
        k = e.key()
        if k in (Qt.Key_Plus, Qt.Key_Equal): self.scale(1.15, 1.15); return
        if k == Qt.Key_Minus: self.scale(1/1.15, 1/1.15); return
        if k == Qt.Key_F and self.scene():
            try: self.fitInView(self.scene().itemsBoundingRect(), Qt.KeepAspectRatio)
            except Exception: pass
            return
        if k == Qt.Key_Escape: self._clear_sticky(); return
        if k == Qt.Key_O: self.set_focus_direction("out"); return
        if k == Qt.Key_I: self.set_focus_direction("in"); return
        if k in (Qt.Key_Up, Qt.Key_Down) and self._sticky and self._nav:
            self._nav_idx = (self._nav_idx + (1 if k == Qt.Key_Down else -1)) % len(self._nav)
            self._apply_focus(self._nav[self._nav_idx], sticky=True); return
        super().keyPressEvent(e)