"""
PDF-Scan und Kontextbildung. Optional: Parallelisierung.
"""
from __future__ import annotations

import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from .constants import DEFAULT_SKIP_BIBLIOGRAPHY, SNIPPET_LEN
from .env import fitz
from .models import Hit, PatternEntry
from .patterns import detect_bibliography_pages
from .text import extract_text

def make_context(text: str, span: Tuple[int, int], length: int = SNIPPET_LEN) -> str:
    s, e = span
    mid = (s + e) // 2
    start = max(0, mid - length // 2)
    end = min(len(text), start + length)
    return " ".join(text[start:end].split())

def scan_pdf(pdf_path: Path, patterns: List[PatternEntry], skip_bib: bool = DEFAULT_SKIP_BIBLIOGRAPHY) -> List[Hit]:
    hits: List[Hit] = []
    try:
        doc = fitz.open(str(pdf_path))
    except Exception as e:
        print(f"[WARN] Konnte PDF nicht öffnen: {pdf_path} ({e})")
        return hits
    try:
        mtime = pdf_path.stat().st_mtime if pdf_path.exists() else 0.0
        bib_pages = detect_bibliography_pages(doc) if skip_bib else set()
        for pidx in range(doc.page_count):
            if skip_bib and pidx in bib_pages:
                continue
            page = doc.load_page(pidx)
            txt = extract_text(page, str(pdf_path), pidx, mtime)
            if not txt:
                continue
            for pe in patterns:
                for m in pe.regex.finditer(txt):
                    s, e = m.span()
                    ctx = make_context(txt, (s, e))
                    hits.append(Hit(
                        pdf_path=str(pdf_path),
                        page=pidx + 1,
                        group=pe.group,
                        label=pe.label,
                        pattern=pe.regex.pattern,
                        context=ctx
                    ))
    finally:
        doc.close()
    return hits

# ---------- Parallel-Scan ----------

def _serialize_patterns(patterns: Sequence[PatternEntry]) -> List[Tuple[str, str, str]]:
    return [(p.label, p.group, p.regex.pattern) for p in patterns]

def _compile_local(serialized: Sequence[Tuple[str, str, str]]) -> List[PatternEntry]:
    loc: List[PatternEntry] = []
    for label, group, pat in serialized:
        try:
            loc.append(PatternEntry(label=label, group=group, regex=re.compile(pat, re.IGNORECASE)))
        except re.error:
            continue
    return loc

def _worker_scan(pdf_path_str: str, patterns_serialized: Sequence[Tuple[str, str, str]], skip_bib: bool) -> List[Hit]:
    path = Path(pdf_path_str)
    pats = _compile_local(patterns_serialized)
    return scan_pdf(path, pats, skip_bib=skip_bib)

def scan_pdfs(pdfs: Sequence[Path], patterns: List[PatternEntry], max_workers: int = 0, skip_bib: bool = DEFAULT_SKIP_BIBLIOGRAPHY) -> List[Hit]:
    if len(pdfs) < 4 or max_workers == 1:
        # Sequenziell
        out: List[Hit] = []
        for i, p in enumerate(pdfs, 1):
            print(f"[INFO] ({i}/{len(pdfs)}) Verarbeite: {p}")
            hs = scan_pdf(p, patterns, skip_bib=skip_bib)
            print(f"       Treffer: {len(hs)}")
            out.extend(hs)
        return out

    # Parallel
    import multiprocessing
    if max_workers <= 0:
        cpu = max(1, (os.cpu_count() or 2) - 1)
        max_workers = min(cpu, 4)
    print(f"[INFO] Parallel-Scan mit {max_workers} Prozessen …")
    out: List[Hit] = []
    ser = _serialize_patterns(patterns)
    with ProcessPoolExecutor(max_workers=max_workers, mp_context=multiprocessing.get_context("spawn")) as ex:
        futures = {ex.submit(_worker_scan, str(p), ser, skip_bib): p for p in pdfs}
        done = 0
        total = len(futures)
        for fut in as_completed(futures):
            p = futures[fut]
            try:
                hs = fut.result()
                out.extend(hs)
                done += 1
                print(f"[INFO] ({done}/{total}) Fertig: {p} | Treffer: {len(hs)}")
            except Exception as e:
                print(f"[ERROR] Worker-Fehler bei {p}: {e}. Fallback sequentiell.")
                try:
                    hs = scan_pdf(p, patterns, skip_bib=skip_bib)
                    out.extend(hs)
                except Exception as e2:
                    print(f"[ERROR] Fallback fehlgeschlagen bei {p}: {e2}")
    return out

