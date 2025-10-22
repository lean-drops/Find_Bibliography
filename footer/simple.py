#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
extract_pdf_footnotes.py
========================
Extrahiert ausschließlich Fußnoten aus einem PDF und speichert sie als TXT.
- PDF- und Ziel-Datei werden per Tkinter-Dialog gewählt (kein Konsolen-Input).
- Heuristik pro Seite:
  * Fußnoten liegen typischerweise im unteren Seitenbereich (z.B. unter 78–82% der Seitenhöhe).
  * Fußnoten sind oft kleiner als der Fließtext (z.B. <= 85% der Median-Schriftgröße).
  * Starten häufig mit Markern: 1), 1., [1], * , † , ‡ etc.
- Linien im Fußnotenbereich werden gesammelt und zu Einträgen gruppiert.
- Mehrzeilige Fußnoten werden zusammengeführt; Worttrennungen am Zeilenende werden korrigiert.

Ausgabe:
- TXT, eine Fußnote pro Zeile (optional mit Seitenangabe).
- Ausführliche Debug-Prints in der Konsole.

Install:
    pip install pymupdf
"""

import re
import math
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Optional

import fitz  # PyMuPDF

# ---------- GUI ----------
try:
    import tkinter as tk
    from tkinter import filedialog, messagebox
    TK_OK = True
except Exception:
    TK_OK = False

# ---------- Konfiguration ----------
BOTTOM_MARGIN_RATIO = 0.22   # Unterer Seitenanteil, der als "Fußnoten-Zone" gilt (0.22 = untere 22%)
SIZE_RATIO_THRESHOLD = 0.85  # Spans/Linien <= 85% der Text-Median-Schriftgröße gelten als "klein"
MIN_BODY_FONT = 6.0          # Untere Schranke, um Artefakte zu vermeiden
MAX_BODY_FONT = 40.0         # Obere Schranke
MERGE_LINE_GAP = 3.5         # Vertikaler Abstand (pt) zum Mergen von Linien der selben Fußnote
INCLUDE_PAGE_IN_TXT = True   # Seitenangabe am Anfang jeder Fußnote mitspeichern
DEDUP_STRIP_NUMBERS = False  # Falls True, Marker/Leitnummer vor der Fußnote entfernen
DEBUG_VERBOSE = True         # Viele Debug-Prints aktivieren

# Marker am Zeilenanfang, die einen neuen Fußnoteneintrag einleiten
FOOTNOTE_START_RE = re.compile(
    r"""^\s*(?:\(?\[?)               # optional ( oder [
         (?:\d{1,3}|[*‡†])           # Nummer 1..999 oder *, †, ‡
         (?:[\]\)\.:])?              # optional ] ) . :
         \s+                         # mind. ein Leerraum
      """,
    re.UNICODE | re.VERBOSE
)

# Falls Zeile keine explizite Nummer hat, aber sehr wahrscheinlich eine FN-Zeile ist:
LIKELY_FOOTNOTE_LINE_RE = re.compile(r"^\s*(?:\d{1,3}|[*‡†])\s+", re.UNICODE)


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
    font_size_avg: float


def median(vals: List[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    if n % 2:
        return s[mid]
    return 0.5 * (s[mid - 1] + s[mid])


def normalize_spaces(t: str) -> str:
    t = re.sub(r"[ \t\u00A0]+", " ", t)
    t = re.sub(r"\s+\n", "\n", t)
    return t.strip()


def join_hyphenation(a: str, b: str) -> str:
    """
    Verbindet a und b, korrigiert Silbentrennung:
    - Wenn a mit '-' endet und b mit Klein-/Großbuchstaben beginnt -> verbinde ohne '- '
    - Sonst normal mit Leerzeichen trennen (falls nötig).
    """
    a = a.rstrip()
    b = b.lstrip()
    if a.endswith("-") and re.match(r"^[A-Za-zÄÖÜäöüß]", b):
        return a[:-1] + b
    return (a + " " + b).strip() if a and b else (a or b)


def is_footnote_start(line_text: str) -> bool:
    return bool(FOOTNOTE_START_RE.match(line_text)) or bool(LIKELY_FOOTNOTE_LINE_RE.match(line_text))


def pick_body_font_median(spans: List[float]) -> float:
    # Filter: nur plausible Fließtextgrößen
    spans = [s for s in spans if MIN_BODY_FONT <= s <= MAX_BODY_FONT]
    if not spans:
        return 10.0  # konservativer Fallback
    return median(spans)


def extract_lines_from_page(page: fitz.Page) -> Tuple[List[Line], float]:
    """
    Erzeuge zeilenweise Struktur aus dem dict-Text: Blocks -> Lines -> Spans.
    Bestimme auch den Median der Span-Schriftgrößen als Referenz.
    """
    data = page.get_text("dict")
    spans_sizes: List[float] = []
    lines: List[Line] = []

    for block in data.get("blocks", []):
        if block.get("type", 0) != 0:  # nur Textblöcke
            continue
        for line in block.get("lines", []):
            parts = []
            sizes = []
            x0 = math.inf
            x1 = -math.inf
            y0 = math.inf
            y1 = -math.inf
            for span in line.get("spans", []):
                txt = span.get("text") or ""
                if not txt.strip():
                    continue
                parts.append(txt)
                sz = float(span.get("size") or 0.0)
                if sz > 0:
                    sizes.append(sz)
                    spans_sizes.append(sz)
                bx = span.get("bbox") or [0, 0, 0, 0]
                x0 = min(x0, bx[0]); y0 = min(y0, bx[1])
                x1 = max(x1, bx[2]); y1 = max(y1, bx[3])
            if parts and sizes:
                text = normalize_spaces("".join(parts))
                lines.append(Line(text=text, y0=y0, y1=y1, x0=x0, x1=x1, font_size_avg=sum(sizes)/len(sizes)))

    body_median = pick_body_font_median(spans_sizes)
    return lines, body_median


def group_footnote_lines(lines: List[Line], page_h: float, body_median: float) -> List[List[Line]]:
    """
    Wähle Kandidaten im unteren Seitenbereich oder mit kleiner Fontsize.
    Gruppiere in Fußnoten anhand Start-Marker + vertikaler Lücken.
    """
    foot_y0 = page_h * (1.0 - BOTTOM_MARGIN_RATIO)
    dprint(f"[DEBUG] Fußnoten-Zone ab y >= {foot_y0:.1f} (Seitenhöhe {page_h:.1f})")
    dprint(f"[DEBUG] Body-Median-Schriftgröße ~ {body_median:.2f} pt")

    candidates: List[Line] = []
    for ln in lines:
        in_bottom = ln.y0 >= foot_y0
        is_small = ln.font_size_avg <= (body_median * SIZE_RATIO_THRESHOLD)
        if in_bottom or is_small:
            candidates.append(ln)

    # Sortiert nach y (oben->unten), dann x
    candidates.sort(key=lambda L: (L.y0, L.x0))

    groups: List[List[Line]] = []
    current: List[Line] = []

    def start_new_group(ln: Line):
        nonlocal current, groups
        if current:
            groups.append(current)
        current = [ln]

    prev: Optional[Line] = None
    for ln in candidates:
        new_start = is_footnote_start(ln.text)
        if prev is None:
            start_new_group(ln)
        else:
            vertical_gap = ln.y0 - prev.y1
            if new_start or vertical_gap > MERGE_LINE_GAP:
                # neuer Eintrag
                start_new_group(ln)
            else:
                # gleiche Fußnote fortsetzen
                current.append(ln)
        prev = ln

    if current:
        groups.append(current)

    dprint(f"[DEBUG] Kandidaten-Linien: {len(candidates)} -> Gruppen (Fußnoten): {len(groups)}")
    return groups


def lines_to_text(lines: List[Line]) -> str:
    """
    Merge Zeilen zu einem Fußnotentext, korrigiere Silbentrennungen und überschüssige Leerzeichen.
    """
    if not lines:
        return ""
    text = lines[0].text
    for ln in lines[1:]:
        text = join_hyphenation(text, ln.text)
    # Aufräumen
    text = re.sub(r"\s{2,}", " ", text)
    text = text.strip()
    return text


def strip_leading_marker(text: str) -> str:
    if not text:
        return text
    m = FOOTNOTE_START_RE.match(text)
    if m:
        return text[m.end():].lstrip()
    return text


def extract_footnotes_from_pdf(pdf_path: Path) -> List[Tuple[int, str]]:
    """
    Liefert Liste [(seite_1basiert, fußnotentext), ...]
    """
    out: List[Tuple[int, str]] = []
    doc = fitz.open(pdf_path)
    dprint(f"[INFO] PDF geöffnet: {pdf_path} – {doc.page_count} Seiten")

    for pno in range(doc.page_count):
        page = doc.load_page(pno)
        page_w, page_h = page.rect.width, page.rect.height
        dprint(f"[INFO] Seite {pno+1}/{doc.page_count}: Größe {page_w:.0f}×{page_h:.0f}")

        lines, body_median = extract_lines_from_page(page)
        if not lines:
            dprint("[WARN] Keine Textzeilen erkannt – Seite wird übersprungen.")
            continue

        groups = group_footnote_lines(lines, page_h, body_median)

        for grp in groups:
            txt = lines_to_text(grp)

            # Falls die Gruppe gar keinen Fußnotencharakter hat, skippen:
            # Kurze, unmarkierte Zeilen im Bottom-Bereich könnten Seitennummer/Legende sein.
            if not FOOTNOTE_START_RE.match(txt) and len(txt) < 15:
                continue

            if DEDUP_STRIP_NUMBERS:
                txt = strip_leading_marker(txt)

            if txt:
                out.append((pno + 1, txt))

    dprint(f"[INFO] Gesamt gefundene Fußnoten: {len(out)}")
    return out


def save_footnotes_to_txt(footnotes: List[Tuple[int, str]], txt_path: Path) -> None:
    with txt_path.open("w", encoding="utf-8") as f:
        for page_no, text in footnotes:
            line = text
            if INCLUDE_PAGE_IN_TXT:
                line = f"[p.{page_no}] {text}"
            f.write(line + "\n")
    dprint(f"[INFO] TXT gespeichert: {txt_path} ({txt_path.stat().st_size} Bytes)")


def pick_paths_via_gui() -> Tuple[Path, Path]:
    if not TK_OK:
        raise RuntimeError("Tkinter ist nicht verfügbar – bitte Python mit Tk-Unterstützung nutzen.")
    root = tk.Tk()
    root.withdraw()
    messagebox.showinfo(
        "PDF Fußnoten-Extraktor",
        ("Wähle ein PDF. Anschließend Ziel-TXT bestimmen.\n"
         "Das Skript extrahiert nur Fußnoten (Heuristik: unterer Seitenbereich, "
         "kleinere Schriftgröße, Startmarker).")
    )
    pdf_file = filedialog.askopenfilename(
        title="PDF wählen",
        filetypes=[("PDF-Dateien", "*.pdf"), ("Alle Dateien", "*.*")]
    )
    if not pdf_file:
        raise RuntimeError("Kein PDF gewählt.")
    out_txt = filedialog.asksaveasfilename(
        title="Zieldatei (TXT) wählen",
        defaultextension=".txt",
        filetypes=[("Textdatei", "*.txt")]
    )
    if not out_txt:
        raise RuntimeError("Keine Zieldatei gewählt.")
    return Path(pdf_file), Path(out_txt)


def main():
    dprint("[INFO] Starte PDF-Fußnoten-Extraktion (PyMuPDF)…")
    pdf_path, txt_path = pick_paths_via_gui()
    dprint(f"[INFO] Eingabe-PDF: {pdf_path}")
    dprint(f"[INFO] Ausgabe-TXT: {txt_path}")

    notes = extract_footnotes_from_pdf(pdf_path)
    if not notes:
        dprint("[WARN] Keine Fußnoten erkannt. Prüfe ggf. die Schwellenwerte.")
    save_footnotes_to_txt(notes, txt_path)
    dprint("[INFO] Fertig.")


if __name__ == "__main__":
    # keine Konsolen-Eingaben – sofort loslegen
    main()