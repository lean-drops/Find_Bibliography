#!/usr/bin/env python3
"""
extract_toc.py – ToC‑Finder mit zentralen Metadateien
====================================================

Speichert alles in   …/services/delb/meta/
  • <PDF>/pages_ratio.json         – pro Buch
  • pages_ratio_summary.json       – Überblick über alle Bücher
  • index.json                     – komplette ToC‑Zeilen aller Bücher

Einziger Aufrufparameter ist der Pfad zu einer einzelnen PDF oder zu einem
Ordner (z. B. `pdfs/`). Der Meta‑Ordner wird automatisch gefunden.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pdfplumber

try:
    import fitz  # PyMuPDF
except ImportError:  # pragma: no cover
    fitz = None  # type: ignore

try:
    from langdetect import detect
except ImportError:  # pragma: no cover
    detect = lambda _txt: 'unknown'  # type: ignore

try:
    import pytesseract
except ImportError:  # pragma: no cover
    pytesseract = None  # type: ignore

# ---------------------------------------------------------------------------
# Konfiguration --------------------------------------------------------------
BASE_KEYWORDS: dict[str, set[str]] = {
    "en": {"table of contents", "contents", "content"},
    "de": {"inhaltsverzeichnis"},
    "fr": {"table des matières"},
    "es": {"índice", "indice"},
    "it": {"indice", "sommario"},
}
DOT_LEADER_RE = re.compile(r"\.{2,}\s*(\d+|[IVXLCDM]+)\s*$")
NUM_RE = re.compile(r"\s(\d+|[IVXLCDM]+)\s*$")
SUMMARY_JSON = "pages_ratio_summary.json"
INDEX_JSON = "index.json"

# ---------------------------------------------------------------------------
# CLI -----------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:  # noqa: WPS110
    parser = argparse.ArgumentParser(description="Locate ToC & store meta JSONs")
    parser.add_argument("path", help="Pfad zu PDF oder Verzeichnis mit PDFs")
    parser.add_argument("--max-pages", type=int,
                        help="Festes Seiten‑Limit; überschreibt 1/3‑Regel")
    parser.add_argument("--ocr", action="store_true", help="OCR für gescannte PDFs")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose log")
    return parser.parse_args()

# ---------------------------------------------------------------------------
# Hilfsfunktionen -----------------------------------------------------------

def _detect_meta_root(any_path: Path) -> Path:
    """Finde `services/delb/meta` relativ zu *any_path*."""
    for parent in [any_path, *any_path.parents]:
        if parent.name == "delb":
            meta = parent / "meta"
            meta.mkdir(parents=True, exist_ok=True)
            return meta
    # Fallback, falls Struktur abweicht
    meta = any_path.parent / "meta"
    meta.mkdir(parents=True, exist_ok=True)
    return meta


def _outline_fallback(pdf: Path) -> Tuple[Optional[int], List[str]]:
    """Nutze PDF‑Bookmarks, falls vorhanden."""
    if not fitz:
        return None, []
    try:
        doc = fitz.open(pdf)  # type: ignore[arg-type]
        toc = doc.get_toc(simple=True)
    except Exception:  # noqa: WPS420
        return None, []

    if not toc:
        return None, []

    page_no = toc[0][2] + 1  # 0‑basiert ➜ 1‑basiert
    lines = [f"{'  ' * (lvl - 1)}{title} ...... {pg + 1}" for lvl, title, pg in toc]
    return page_no, lines


def _detect_lang(text: str) -> str:
    try:
        return detect(text)
    except Exception:  # noqa: WPS420
        return "unknown"


def _ocr_page(page) -> str:
    if not pytesseract:
        return ""
    return pytesseract.image_to_string(page.to_image(resolution=300).original)


def _looks_like_toc(text: str, lang: str) -> bool:
    if not text:
        return False
    lower = text.lower()
    keys = BASE_KEYWORDS.get(lang, set()) | set.union(*BASE_KEYWORDS.values())
    if any(k in lower for k in keys):
        return True
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    dotted = sum(DOT_LEADER_RE.search(ln) is not None for ln in lines)
    numbered = sum(NUM_RE.search(ln) is not None for ln in lines)
    return dotted >= 3 or (numbered / len(lines) > 0.6 and numbered >= 4)

# ---------------------------------------------------------------------------
# Kern‑Routine --------------------------------------------------------------

def _scan_pdf(pdf: Path, max_pages: Optional[int], use_ocr: bool) -> Tuple[Optional[int], List[str]]:
    """Suche nach ToC, liefere (Seite, Zeilen)."""
    # 1) Outline‑Methode
    page_no, toc_lines = _outline_fallback(pdf)
    if page_no:
        print(f"[{pdf.name}] ToC via Outline ➜ Seite {page_no}")
        for ln in toc_lines:
            print("   •", ln)
        return page_no, toc_lines

    # 2) Heuristische Suche
    with pdfplumber.open(pdf) as pdf_doc:
        total_pages = len(pdf_doc.pages)
        limit = max_pages or (total_pages + 2) // 3
        lang = "unknown"
        sampled = False
        for idx in range(limit):
            pg = pdf_doc.pages[idx]
            txt = pg.extract_text() or ""
            if not txt and use_ocr:
                txt = _ocr_page(pg)
            if not sampled and txt:
                lang = _detect_lang(txt[:500])
                sampled = True
            if _looks_like_toc(txt, lang):
                page_no = idx + 1
                toc_lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
                print(f"[{pdf.name}] ToC heuristisch ➜ Seite {page_no}")
                print("――――――――――――――――――――――――――――――――――――――")
                for ln in toc_lines:
                    print(ln)
                print()
                return page_no, toc_lines

    logging.warning("%s: kein ToC in erstem Drittel", pdf.name)
    return None, []

# ---------------------------------------------------------------------------
# File‑Writer ---------------------------------------------------------------

def _write_per_pdf_meta(meta_root: Path, pdf: Path, toc_pg: Optional[int], total: int) -> None:
    folder = meta_root / pdf.stem
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / "pages_ratio.json").open("w", encoding="utf-8") as fp:
        json.dump({"toc_page": toc_pg, "total_pages": total}, fp, indent=2)

# ---------------------------------------------------------------------------
# Main ----------------------------------------------------------------------

def main() -> None:  # noqa: WPS231
    args = _parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.ERROR,
                        format="[%(levelname)s] %(message)s")

    root_input = Path(args.path).expanduser().resolve()
    if not root_input.exists():
        sys.exit(f"Pfad nicht gefunden: {root_input}")

    meta_root = _detect_meta_root(root_input)

    pdf_files = [root_input] if root_input.is_file() else sorted(root_input.rglob("*.pdf"))
    if not pdf_files:
        sys.exit("Keine PDF‑Dateien gefunden.")

    summary: Dict[str, Dict[str, Optional[int]]] = {}
    index_dict: Dict[str, List[str]] = {}

    for pdf in pdf_files:
        try:
            with pdfplumber.open(pdf) as doc:
                total_pages = len(doc.pages)
        except Exception as exc:  # noqa: WPS420
            logging.error("Fehler bei %s – %s", pdf.name, exc)
            continue

        toc_pg, toc_lines = _scan_pdf(pdf, args.max_pages, args.ocr)
        summary[pdf.name] = {"toc_page": toc_pg, "total_pages": total_pages}
        if toc_lines:
            index_dict[pdf.name] = toc_lines
        _write_per_pdf_meta(meta_root, pdf, toc_pg, total_pages)

    # Zentrale JSONs schreiben
    with (meta_root / SUMMARY_JSON).open("w", encoding="utf-8") as fp:
        json.dump(summary, fp, indent=2, ensure_ascii=False)
    with (meta_root / INDEX_JSON).open("w", encoding="utf-8") as fp:
        json.dump(index_dict, fp, indent=2, ensure_ascii=False)

    print(f"\n➜ Metadateien gespeichert: {meta_root / SUMMARY_JSON}, {meta_root / INDEX_JSON}")


if __name__ == "__main__":
    main()
