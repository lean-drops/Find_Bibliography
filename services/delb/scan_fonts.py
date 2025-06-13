#!/usr/bin/env python3
"""scan_fonts.py — v2.0 *turbo* (2025‑04‑26)

Parallel‑Scanner für **ganze PDFs** oder **ganze Ordner**.

• **Thread‑Pool pro PDF**   → Seiten werden gleichzeitig gelesen.
• **Prozess‑Pool für Ordner** → mehrere PDFs parallel.
• Fortschrittsbalken mit *tqdm*.
• Jeder Job schreibt: `meta/<PDF‑Name>/font_cluster.json`.

Aufruf­beispiele
----------------
# Einzel‑PDF, alle CPU‑Threads nutzen
    python scan_fonts.py book.pdf

# Ordner scannen, 4 Prozesse & 6 Threads pro PDF
    python scan_fonts.py ./pdfs -j 4 -t 6
"""
from __future__ import annotations

import argparse, json, logging, math, os, sys, time
from concurrent import futures as cf
from pathlib import Path
from typing import Dict, Tuple, List

import fitz  # PyMuPDF
from tqdm import tqdm

LOG = logging.getLogger("fontrec")

# --------------------------------------------------------------------------- #
FontKey = Tuple[str, float, int]  # (FontName, Size, StyleFlags)


def span_key(span) -> FontKey:
    return span["font"], round(span["size"], 1), span["flags"]


def analyse_page(idx_page_tuple):
    """Worker‑Funktion für ThreadPool (PageIndex, PDF‑Pfad)."""
    i, pdf_path = idx_page_tuple
    doc = fitz.open(pdf_path)
    page = doc.load_page(i)
    stats: Dict[FontKey, int] = {}
    for block in page.get_text("dict")["blocks"]:
        if block["type"]:
            continue
        for line in block["lines"]:
            for span in line["spans"]:
                txt = span["text"]
                if not txt:
                    continue
                k = span_key(span)
                stats[k] = stats.get(k, 0) + len(txt)
    return i, stats


def analyse_pdf(pdf_path: Path, threads: int) -> dict:
    """Scannt ein PDF mit *threads* parallelen Seiten‑Worker."""
    t0 = time.perf_counter()
    doc = fitz.open(pdf_path)
    n_pages = doc.page_count
    global_stats: Dict[FontKey, int] = {}
    pages_out: List[dict] = [{}] * n_pages  # pre‑alloc

    with cf.ThreadPoolExecutor(max_workers=threads) as tp:
        for i, stats in tqdm(tp.map(analyse_page, [(i, str(pdf_path)) for i in range(n_pages)]),
                             total=n_pages, desc=pdf_path.name, leave=False):
            pages_out[i] = {
                "page": i + 1,
                "fonts": [list(k) + [c] for k, c in sorted(stats.items(),
                                                             key=lambda t: (-t[1], t[0]))],
            }
            for k, c in stats.items():
                global_stats[k] = global_stats.get(k, 0) + c

    return {
        "pdf": pdf_path.name,
        "pages_total": n_pages,
        "runtime_s": round(time.perf_counter() - t0, 3),
        "font_ranking": [list(k) + [c] for k, c in sorted(global_stats.items(),
                                                           key=lambda t: (-t[1], t[0]))],
        "pages": pages_out,
    }


# --------------------------------------------------------------------------- #
# I/O helper
# --------------------------------------------------------------------------- #

def write_json(data: dict, pdf_name: str):
    target_dir = Path("meta") / Path(pdf_name).stem
    target_dir.mkdir(parents=True, exist_ok=True)
    out_path = target_dir / "font_cluster.json"
    out_path.write_text(json.dumps(data, indent=2, ensure_ascii=False))

# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def main() -> None:
    ap = argparse.ArgumentParser("recognize_fonts – turbo‑parallel Font‑Scanner")
    ap.add_argument("path", type=Path, help="PDF oder Verzeichnis")
    ap.add_argument("-j", "--proc", type=int, default=max(os.cpu_count() // 2, 1),
                    help="Prozesse (nur Ordner‑Modus)")
    ap.add_argument("-t", "--threads", type=int, default=os.cpu_count() or 4,
                    help="Threads pro PDF")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level="DEBUG" if args.debug else "INFO",
                        format="%(levelname)s|%(message)s")

    p = args.path
    if p.is_file():
        if p.suffix.lower() != ".pdf":
            LOG.error("%s ist keine PDF", p)
            sys.exit(1)
        data = analyse_pdf(p, args.threads)
        write_json(data, p.name)
        LOG.info("%s → fertig (%.2fs)", p.name, data["runtime_s"])
        return

    if p.is_dir():
        pdfs = sorted([f for f in p.iterdir() if f.is_file() and f.suffix.lower() == ".pdf"])
        if not pdfs:
            LOG.warning("Keine PDFs in %s", p)
            return
        with cf.ProcessPoolExecutor(max_workers=args.proc) as pool:
            future_to_pdf = {pool.submit(analyse_pdf, pdf, args.threads): pdf for pdf in pdfs}
            for fut in tqdm(cf.as_completed(future_to_pdf), total=len(future_to_pdf),
                            desc="PDFs", leave=True):
                pdf = future_to_pdf[fut]
                try:
                    data = fut.result()
                    write_json(data, pdf.name)
                except Exception as e:
                    LOG.error("%s – Fehler: %s", pdf.name, e)
        return

    LOG.error("%s ist weder Datei noch Verzeichnis", p)
    sys.exit(1)


if __name__ == "__main__":
    main()
