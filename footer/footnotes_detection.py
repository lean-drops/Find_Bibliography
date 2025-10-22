#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
footnotes_detection.py
----------------------
Extrahiert NUR Fußnoten (keine Dedupe). Verbesserte Gruppierung mit "Smart-Merge",
um fälschliche Splits durch harte Zeilenumbrüche / Spaltenwechsel zu heilen.

Heuristik-Schritte:
1) Kandidatenzeilen: Bottom-Zone ODER deutlich kleinere Schrift als Body.
2) Erst-Gruppierung: Marker-Start ODER (großer vertikaler Gap).
3) SMART-MERGE (neu): Merge benachbarter Gruppen OHNE Marker, wenn Fortsetzungs-
   heuristiken erfüllt sind (adaptiver Gap, unbalancierte Klammern/Quotes, offenes
   Satzende, kleingeschriebener Start, Spalten-Wrap).
4) Zusammenführen von Zeilen- und Gruppen-Text (inkl. Silbentrennung).

API:
- page_footnotes(doc: fitz.Document, pno: int) -> Dict mit 'groups' (Liste von Fußnoten-Dicts)

Konfig:
- Unten anpassbare Schwellen (mit Debug-Output).
"""

from __future__ import annotations
import math
import re
import shutil
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import fitz  # PyMuPDF
import numpy as np

# OCR optional (für Seiten ohne Textlayer)
try:
    import pytesseract
    _OCR_OK = shutil.which("tesseract") is not None
except Exception:
    _OCR_OK = False

# ---------- Konfiguration ----------
BOTTOM_MARGIN_RATIO = 0.22      # untere 22% als Fußnoten-Zone (Fallback wenn keine Linie)
SIZE_RATIO_THRESHOLD = 0.86     # Kandidaten <= 86% der Body-Median-Size
MIN_BODY_FONT = 6.0
MAX_BODY_FONT = 42.0

HLINE_DETECT_YRATIO = 0.12      # in den untersten 12% nach horizontaler Linie suchen
BASE_MERGE_LINE_GAP = 3.8       # Basisschwelle für Zeilen-Merge (Punkte)
MERGE_GAP_MULT = 1.9            # adaptiver Multiplikator auf medianen Zeilengap der Kandidaten

SMART_MERGE_ENABLE = True
SMART_MERGE_MAX_GAP_MULT = 2.6  # max erlaubter Gap-Faktor (sehr konservativ)
SMART_MERGE_INDENT_TOL = 14.0   # Spalten/Hanging-Indent-Toleranz in Punkten
SMART_MERGE_LOWERCASE_WORDS = (
    "und", "oder", "vgl.", "siehe", "s.", "cp.", "and", "or", "see"
)

DEBUG_VERBOSE = True

# Marker: 1), 1., [1], *, †, ‡
MARKER_RE = re.compile(r"^\s*(\(?\[?(?:\d{1,3}|[*†‡])(?:[\]\)\.:])?)\s+(.*)$", re.U)

# Für „offenes“ Satzende bei Fortsetzung
OPEN_TAIL_RE = re.compile(r"([,;:/–—\-]\s*)$")

# Quotes/Brackets-Paare für Balance-Check
PAIRS = [
    ("(", ")"), ("[", "]"), ("{", "}"),
    ("«", "»"), ("‹", "›"), ("„", "“"), ("‚", "‘"), ("“", "”"), ("‘", "’"),
]

def dprint(msg: str) -> None:
    if DEBUG_VERBOSE:
        print(msg, flush=True)

@dataclass
class Line:
    text: str
    y0: float
    y1: float
    x0: float
    x1: float
    size_avg: float

# ---------- Helfer ----------
def _normalize_spaces(t: str) -> str:
    t = re.sub(r"[ \t\u00A0]+", " ", t)
    t = re.sub(r"\s+\n", "\n", t)
    return t.strip()

def _median(vals: List[float]) -> float:
    if not vals:
        return 10.0
    s = sorted(vals); n = len(s); m = n//2
    return s[m] if n % 2 else 0.5*(s[m-1]+s[m])

def _extract_lines(page: fitz.Page) -> Tuple[List[Line], float]:
    data = page.get_text("dict")
    sizes: List[float] = []
    lines: List[Line] = []
    for block in data.get("blocks", []):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            parts: List[str] = []
            svals: List[float] = []
            x0=y0=math.inf; x1=y1=-math.inf
            for sp in line.get("spans", []):
                txt = sp.get("text") or ""
                if not txt.strip():
                    continue
                sz = float(sp.get("size") or 0)
                if MIN_BODY_FONT <= sz <= MAX_BODY_FONT:
                    svals.append(sz); sizes.append(sz)
                bx = sp.get("bbox") or [0,0,0,0]
                x0=min(x0,bx[0]); y0=min(y0,bx[1]); x1=max(x1,bx[2]); y1=max(y1,bx[3])
                parts.append(txt)
            if parts and svals:
                lines.append(Line(
                    text=_normalize_spaces("".join(parts)),
                    y0=y0,y1=y1,x0=x0,x1=x1,size_avg=sum(svals)/len(svals)
                ))
    body_med = _median(sizes)
    return lines, body_med

def _detect_hline_bottom(page: fitz.Page) -> Optional[float]:
    """Horizontale Trennlinie im unteren Seitenbereich erkennen."""
    try:
        for d in page.get_drawings():
            for p in d.get("items", []):
                if p[0] != "l":
                    continue
                x1,y1,x2,y2 = p[1]
                if abs(y1-y2) <= 0.8 and abs(x2-x1) >= (page.rect.width*0.35):
                    if y1 >= page.rect.height*(1.0 - HLINE_DETECT_YRATIO):
                        dprint(f"[DEBUG] Trennlinie bei y={y1:.1f}")
                        return y1
    except Exception:
        pass
    return None

def _join_hyph(a: str, b: str) -> str:
    a = a.rstrip(); b = b.lstrip()
    # Silbentrennung nur bei alphabetischen Fortsetzungen reparieren
    if a.endswith("-") and re.match(r"^[A-Za-zÄÖÜäöüß]", b):
        return a[:-1] + b
    return (a + " " + b).strip() if a and b else (a or b)

def _lines_to_text(lines: List[Line]) -> str:
    t = lines[0].text
    for ln in lines[1:]:
        t = _join_hyph(t, ln.text)
    return re.sub(r"\s{2,}"," ", t).strip()

def _bbox_of_lines(lines: List[Line]) -> Dict[str, float]:
    return {
        "x0": min(l.x0 for l in lines),
        "y0": min(l.y0 for l in lines),
        "x1": max(l.x1 for l in lines),
        "y1": max(l.y1 for l in lines),
    }

def _adaptive_gap_threshold(cands: List[Line]) -> float:
    """Bestimme adaptiven vertikalen Gap auf Basis der Kandidaten."""
    if len(cands) < 2:
        return BASE_MERGE_LINE_GAP
    gaps = []
    for i in range(1, len(cands)):
        gaps.append(max(0.0, cands[i].y0 - cands[i-1].y1))
    med_gap = _median(gaps) or BASE_MERGE_LINE_GAP
    thr = max(BASE_MERGE_LINE_GAP, med_gap * MERGE_GAP_MULT)
    dprint(f"[DEBUG] adaptiver Zeilengap: median={med_gap:.2f} thr={thr:.2f}")
    return thr

def _is_balanced(text: str) -> bool:
    """True, wenn Klammern/Anführungen geschlossen."""
    for L, R in PAIRS:
        if text.count(L) > text.count(R):
            return False
    return True

def _starts_like_continuation(text: str) -> bool:
    if not text:
        return False
    # beginnt mit kleinem Buchstaben oder schließender Klammer/Punktuation
    if re.match(r"^[\])}\.,;:/–—\-]", text):
        return True
    if re.match(r"^[a-zäöüß]", text):
        return True
    low = text.lower().strip()
    for w in SMART_MERGE_LOWERCASE_WORDS:
        if low.startswith(w + " "):
            return True
    return False

def _ends_looks_open(text: str) -> bool:
    """Vorheriger Text wirkt 'offen' (Komma, ;, :, /, –, —, -) oder offene Klammer/Quote."""
    if OPEN_TAIL_RE.search(text):
        return True
    return not _is_balanced(text)

def _is_column_wrap(prev_bbox: Dict[str, float], next_bbox: Dict[str, float], max_gap: float) -> bool:
    """Spaltenumbruch: x springt deutlich nach links, vertikaler Gap klein."""
    dy = max(0.0, next_bbox["y0"] - prev_bbox["y1"])
    x_left = next_bbox["x0"] + SMART_MERGE_INDENT_TOL < prev_bbox["x0"]
    return x_left and dy <= max_gap

# ---------- Hauptlogik ----------
def page_footnotes(doc: fitz.Document, pno: int) -> Dict[str, Any]:
    page = doc.load_page(pno)
    w, h = page.rect.width, page.rect.height
    lines, body_med = _extract_lines(page)

    if not lines:
        if _OCR_OK:
            dprint(f"[WARN] Seite {pno+1}: kein Textlayer – OCR fallback.")
            pix = page.get_pixmap(dpi=300)
            txt = pytesseract.image_to_string(pix.tobytes("ppm"), lang="deu+eng")
            bottom_text = txt.strip()
            return {"page_size": (w,h), "bottom_text": bottom_text, "groups": [], "lines": []}
        return {"page_size": (w,h), "bottom_text": "", "groups": [], "lines": []}

    # Fußnoten-Zone
    y_line = _detect_hline_bottom(page)
    foot_y0 = (y_line + 1.0) if y_line else (h * (1.0 - BOTTOM_MARGIN_RATIO))

    # Kandidaten auswählen
    cands: List[Line] = []
    for ln in lines:
        in_bottom = ln.y0 >= foot_y0
        is_small = ln.size_avg <= (body_med * SIZE_RATIO_THRESHOLD)
        if in_bottom or is_small:
            cands.append(ln)
    cands.sort(key=lambda L: (L.y0, L.x0))

    if not cands:
        dprint(f"[INFO] Seite {pno+1}: keine Kandidaten im Fußnotenbereich.")
        return {
            "page_size": (w,h),
            "body_median": body_med,
            "foot_y0": foot_y0,
            "groups": [],
            "bottom_text": ""
        }

    # Adaptiver Gap
    LINE_GAP_THR = _adaptive_gap_threshold(cands)

    # 1) Erst-Gruppierung: Marker-Start ODER (Gap > Threshold) => neue Gruppe
    raw_groups: List[List[Line]] = []
    cur: List[Line] = []
    prev: Optional[Line] = None

    for ln in cands:
        is_marker_start = bool(MARKER_RE.match(ln.text))
        if prev is None:
            cur = [ln]
        else:
            gap = ln.y0 - prev.y1
            if is_marker_start or gap > LINE_GAP_THR:
                raw_groups.append(cur); cur = [ln]
            else:
                cur.append(ln)
        prev = ln
    if cur:
        raw_groups.append(cur)

    dprint(f"[INFO] Seite {pno+1}: Erstgruppen = {len(raw_groups)}")

    # 2) SMART-MERGE: benachbarte Gruppen ohne Marker ggf. zusammenführen
    merged_groups: List[List[Line]] = []
    if not SMART_MERGE_ENABLE or len(raw_groups) == 1:
        merged_groups = raw_groups
    else:
        # Vorbereiten: Metadaten je Gruppe
        def grp_meta(grp: List[Line]) -> Dict[str, Any]:
            text = _lines_to_text(grp)
            m = MARKER_RE.match(text)
            marker = m.group(1).strip() if m else None
            content = (m.group(2).strip() if m else text)
            bbox = _bbox_of_lines(grp)
            return {
                "text": text, "marker": marker, "content": content, "bbox": bbox,
                "size_avg": float(np.mean([l.size_avg for l in grp])),
            }

        metas = [grp_meta(g) for g in raw_groups]
        merged_groups = [raw_groups[0]]
        for i in range(1, len(raw_groups)):
            prev_grp = merged_groups[-1]
            next_grp = raw_groups[i]
            prev_meta = grp_meta(prev_grp)
            next_meta = metas[i]
            prev_text = prev_meta["content"]
            next_text = next_meta["content"]
            prev_bbox = prev_meta["bbox"]
            next_bbox = next_meta["bbox"]

            next_has_marker = bool(next_meta["marker"])
            dy = max(0.0, next_bbox["y0"] - prev_bbox["y1"])
            max_cont_gap = min(LINE_GAP_THR * SMART_MERGE_MAX_GAP_MULT, LINE_GAP_THR + 10.0)

            merge_reason = None
            if not next_has_marker:
                # (a) Sehr kleiner Gap UND Start wirkt wie Fortsetzung
                if dy <= max_cont_gap and _starts_like_continuation(next_text):
                    merge_reason = f"Fortsetzung (Start: '{next_text[:12]}…') dy={dy:.2f}"
                # (b) Vorheriger Text "offen" ODER Klammern unausgeglichen
                elif dy <= max_cont_gap and _ends_looks_open(prev_text):
                    merge_reason = f"Vorheriger Text offen/unbalanciert dy={dy:.2f}"
                # (c) Spalten-Wrap (x springt links) mit kleinem Gap
                elif _is_column_wrap(prev_bbox, next_bbox, max_cont_gap):
                    merge_reason = "Spalten-Wrap (x-links + kleiner Gap)"
                # (d) Einzug sehr ähnlich (Hanging-Indent), kein Marker, kleiner Gap
                else:
                    xdelta = abs(next_bbox["x0"] - prev_bbox["x0"])
                    if dy <= max_cont_gap and xdelta <= SMART_MERGE_INDENT_TOL:
                        merge_reason = f"ähnlicher Einzug Δx={xdelta:.1f} dy={dy:.2f}"

            if merge_reason:
                dprint(f"[DEBUG] SMART-MERGE: Gruppe {i} mit {i-1} -> {merge_reason}")
                # tatsächliches Mergen
                merged_groups[-1] = prev_grp + next_grp
            else:
                merged_groups.append(next_grp)

    dprint(f"[INFO] Seite {pno+1}: Nach SMART-MERGE = {len(merged_groups)}")

    # 3) Ausgabe-Struktur
    out_groups: List[Dict[str, Any]] = []
    for gi, grp in enumerate(merged_groups, start=1):
        raw = _lines_to_text(grp)
        m = MARKER_RE.match(raw)
        marker, text = (m.group(1).strip() if m else None), (m.group(2).strip() if m else raw)
        if not text:
            continue
        bbox = _bbox_of_lines(grp)
        out_groups.append({
            "index_in_page": gi,
            "marker": marker,
            "text": text,
            "font_size_avg": float(np.mean([l.size_avg for l in grp])),
            "bbox": bbox,
            "score": 0.75 + 0.15*int(bool(marker)),
            "method": "heuristic"
        })

    bottom_text = "\n".join([l.text for l in cands])
    return {
        "page_size": (w,h),
        "body_median": body_med,
        "foot_y0": foot_y0,
        "groups": out_groups,
        "bottom_text": bottom_text
    }