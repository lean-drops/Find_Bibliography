#!/usr/bin/env python3
# ─────────────────────────────────────────────────────────────────────────────
# scan_fonts.py – Font- & Layout-Clustering für PDFs  (v4-fast, 2025-04-27)
#
#   • seiten­parallele Verarbeitung (ThreadPool)  → deutlich schneller
#   • sehr viele Debug-Statements  (--debug)
#   • robuste Heuristik: fällt nie auf leere Sequenzen
#   • Optionale Speicherung aller Spans (--keep-spans)
#
# Aufruf-Beispiele
#   python scan_fonts.py ./one.pdf --debug
#   python scan_fonts.py ./pdfs -o meta/ -j 8 --min-body-ratio 0.35
# ─────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import argparse, concurrent.futures as cf, json, logging, math, os, re, sys, time
from pathlib import Path
from typing import Dict, List, Tuple
import fitz                # PyMuPDF
from tqdm import tqdm

# ───────────── Logging ──────────────────────────────────────────────────────
LOG = logging.getLogger("fontrec")
dbg = LOG.debug
def init_log(debug: bool):
    logging.basicConfig(
        level="DEBUG" if debug else "INFO",
        format="%(asctime)s %(levelname)-7s| %(funcName)s:%(lineno)-3d – %(message)s",
        datefmt="%H:%M:%S")

# ───────────── Typen & Regex ────────────────────────────────────────────────
FontKey = Tuple[str, float, int]        # (fontName, size, styleFlag)
SPAN_RE = re.compile(r"^[\s\W0-9]+$")   # reiner Ziffern/Leerzeichen-Zeilen ausfiltern

def span_key(span) -> FontKey:
    """Erzeuge Font-Schlüssel aus einem PyMuPDF-Span"""
    flags = 1 if span["flags"] & 2 else 2 if span["flags"] & 1 else 0  # 1=bold, 2=italic
    return (span["font"], round(span["size"], 1), flags)

# ───────────── Seite → Cluster  (Einzelfunktion für ThreadPool) ─────────────
def analyse_page(args):
    """Return (pageNo, bodyFonts, headingFonts, previewSpans, fontStatsDict)"""
    pdf_path, pnum, min_body_ratio, keep_spans = args
    page = fitz.open(pdf_path, filetype="pdf").load_page(pnum)  # light reopen
    spans = [s for b in page.get_text("dict")["blocks"] if b["type"] == 0
             for l in b["lines"] for s in l["spans"]]

    page_fonts: Dict[FontKey, int] = {}
    preview: List[Tuple[str, *FontKey]] = []

    for s in spans:
        txt = s["text"].strip()
        if not txt or SPAN_RE.match(txt):
            continue
        key = span_key(s)
        n = len(txt)
        page_fonts[key] = page_fonts.get(key, 0) + n
        if len(preview) < 60:          # kleine Vorschau
            preview.append((txt[:100], *key))

    tot = sum(page_fonts.values()) or 1
    body = [k for k, c in page_fonts.items() if c / tot >= min_body_ratio]
    if not body:                       # Fallback: häufigster Font
        body = [max(page_fonts, key=page_fonts.get)]
    max_body_size = max(k[1] for k in body)
    heading = [k for k, c in page_fonts.items() if k[1] > 1.05 * max_body_size]

    dbg("p%03d  body=%d  heading=%d  fonts=%d", pnum + 1, len(body),
        len(heading), len(page_fonts))

    # Font-Stats fürs Gesamt-Dokument zurück
    return (pnum + 1, body, heading, preview if keep_spans else [],
            {k: page_fonts[k] for k in page_fonts})

# ───────────── PDF → JSON ───────────────────────────────────────────────────
def cluster_fonts(pdf: Path, threads: int, min_body_ratio: float,
                  keep_spans: bool) -> Dict:
    t0 = time.perf_counter()
    doc = fitz.open(pdf)
    dbg("opened %s  pages=%d", pdf.name, doc.page_count)

    # Seite-parallel
    with cf.ThreadPoolExecutor(max_workers=threads) as tp:
        res = list(
            tp.map(analyse_page,
                   [(str(pdf), p, min_body_ratio, keep_spans)
                    for p in range(doc.page_count)]))

    # Ausgabe sortieren
    res.sort(key=lambda t: t[0])
    pages_out = []
    total_stats: Dict[FontKey, int] = {}
    for pno, body, head, prev, stats in res:
        pages_out.append({"n": pno,
                          "body": [list(k) for k in body],
                          "heading": [list(k) for k in head],
                          **({"spans": prev} if keep_spans else {})})
        for k, c in stats.items():
            total_stats[k] = total_stats.get(k, 0) + c

    data = {
        "pages": pages_out,
        "font_ranking": sorted(([*k, c] for k, c in total_stats.items()),
                               key=lambda t: t[3], reverse=True),
        "pdf_pages": doc.page_count,
        "runtime_s": round(time.perf_counter() - t0, 3)
    }
    LOG.info("%s  ✔  pages=%d  %.2fs", pdf.name, doc.page_count, data["runtime_s"])
    return data

# ───────────── Batch-Runner ─────────────────────────────────────────────────
def run_batch(path: Path, out_dir: Path | None, threads: int,
              **kw) -> None:
    pdfs = sorted(path.glob("*.pdf"))
    if not pdfs:
        LOG.error("Keine PDFs in %s", path); return
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)

    def job(p: Path):
        try:
            data = cluster_fonts(p, threads, **kw)
            if out_dir:
                (out_dir / f"{p.stem}.fonts.json").write_text(
                    json.dumps(data, indent=2, ensure_ascii=False))
            return p.name, "ok"
        except Exception as e:
            return p.name, f"ERROR: {e}"

    with cf.ThreadPoolExecutor(max_workers=min(len(pdfs), threads)) as pool:
        for name, status in tqdm(pool.map(job, pdfs), total=len(pdfs), desc="PDFs"):
            LOG.info("%s – %s", name, status)

# ───────────── CLI ─────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser("Fast Font-Cluster-Extractor v4-fast")
    ap.add_argument("path", type=Path,
                    help="PDF-Datei oder Ordner mit PDFs")
    ap.add_argument("-o", "--out", type=Path,
                    help="Ziel-Ordner für *.fonts.json")
    ap.add_argument("-j", type=int, default=os.cpu_count() or 4,
                    help="Threads pro PDF (und in Batch)")
    ap.add_argument("--min-body-ratio", type=float, default=0.40,
                    help="Schwelle Haupt-Font-Anteil (relativ)")
    ap.add_argument("--keep-spans", action="store_true",
                    help="Alle Span-Previews speichern (größer!)")
    ap.add_argument("--stdout", action="store_true",
                    help="JSON zu STDOUT statt Datei (nur Einzel-PDF)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()
    init_log(args.debug)

    if args.path.is_file():
        data = cluster_fonts(args.path, args.j,
                             min_body_ratio=args.min_body_ratio,
                             keep_spans=args.keep_spans)
        if args.stdout:
            print(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            out = args.out or args.path.with_suffix(".fonts.json")
            out.write_text(json.dumps(data, indent=2, ensure_ascii=False))
            LOG.info("geschrieben: %s", out)
        return

    if args.path.is_dir():
        out_dir = args.out or args.path
        run_batch(args.path, out_dir, args.j,
                  min_body_ratio=args.min_body_ratio,
                  keep_spans=args.keep_spans)
        return

    LOG.error("%s ist weder Datei noch Verzeichnis", args.path)
    sys.exit(1)

if __name__ == "__main__":
    main()