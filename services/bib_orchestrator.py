#!/usr/bin/env python3
# services/delb/bib_orchestrator.py   ·  rev. 2025-05-02
# ─────────────────────────────────────────────────────────────────────────────
# 0) Inhaltsverzeichnis (Jackpot)        – erkennt auch „bis EOF“-Fall
# 1) Keyword-Block (≥ kw_min_block)      – super fix
# 2) Heading-Fonts                       – Kapitel-Heading nach ToC-Barriere
# 3) Vollheuristik  _detect_text()       – letzte Rettung
# ─────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import List, Optional, Tuple

import fitz  # PyMuPDF – nur ein kurzer Open um page_count zu holen

# Sub-Detektoren ------------------------------------------------------------
from services.delb.extract_toc     import _scan_pdf as _toc_scan, detect as _detect_text
from services.delb.keyword_hits    import analyse_pdf   as _kw_pages
from services.delb.scan_fonts      import analyse_pdf   as _font_scan, write_json as _font_write
from services.delb.detect_chapters import analyse_file  as _chap_analyse

LOG = logging.getLogger(__name__)

# ───────── Bibliographie-Heading (Font-Scanner) ────────────────────────────
_RE_BIB_HDR = re.compile(
    r"\b(bibliograph\w*|references?|reference\s+list|works\s+cited|"
    r"literaturverzeichnis|quellen(?:verzeichnis| und literatur)?)\b",
    re.I,
)

# ───────── ToC-Helper (Keyword & Seitenzahl) ───────────────────────────────
_TOC_KEYS = {
    "bibliograph", "reference", "literatur", "quellen",
    "works cited", "literature cited"
}
_TOC_NUM = re.compile(r"(?:\.{2,}|\s)(\d{1,4})\s*$")          #  …… 391

def _page_from_toc(lines: List[str]) -> int | None:
    """Extrahiert die Seitenzahl des Bibliographie-Eintrags in der ToC."""
    for ln in lines:
        if any(k in ln.lower() for k in _TOC_KEYS):
            if (m := _TOC_NUM.search(ln)):
                try:
                    return int(m.group(1))
                except ValueError:
                    pass
    return None

# ───────── Haupt-API ───────────────────────────────────────────────────────
def detect_bibliography(
    pdf: str | Path,
    *,
    kw_min_block : int  = 2,
    head         : float = .05,
    tail         : float = .25,
    kw_workers   : int | None = None,
    font_threads : int | None = None,
    use_gpt      : bool = False,
    boost        : float = 1.0,
) -> Optional[Tuple[int, int]]:
    """Liefert `(first_page, last_page)` oder `None`."""
    p           = Path(pdf)
    kw_workers  = kw_workers   or max(2, (os.cpu_count() or 4) // 2)
    font_threads= font_threads or (os.cpu_count()  or 4)

    # 0 ── Inhaltsverzeichnis ──────────────────────────────────────────────
    toc_page, toc_lines = _toc_scan(p, max_pages=None, use_ocr=False)
    if toc_lines:
        bib_pg = _page_from_toc(toc_lines)
        if bib_pg:
            # Prüfen, ob Bibliographie der LETZTE ToC-Eintrag ist
            try:
                last_num = max(int(m.group(1))
                               for ln in toc_lines
                               if (m := _TOC_NUM.search(ln)))
            except ValueError:
                last_num = bib_pg

            if bib_pg == last_num:
                with fitz.open(p) as doc:
                    eof_page = doc.page_count          # 1-basiert
                LOG.info("Bibliographie via ToC → Seiten %d-%d (bis EOF)",
                         bib_pg, eof_page)
                return (bib_pg, eof_page)

            LOG.info("Bibliographie via ToC → Seite %d", bib_pg)
            return (bib_pg, bib_pg)

    # 1 ── Keyword-Block ───────────────────────────────────────────────────
    kw_hits = _kw_pages(p, workers=kw_workers, debug=False)
    if kw_hits:
        kw_hits.sort()
        blocks: List[List[int]] = []
        cur = [kw_hits[0]]
        for pg in kw_hits[1:]:
            if pg == cur[-1] + 1:
                cur.append(pg)
            else:
                blocks.append(cur); cur = [pg]
        blocks.append(cur)
        best = max(blocks, key=len)

        if len(best) >= kw_min_block:
            LOG.info("Bibliographie via Keyword-Block → %s", best)
            return (best[0], best[-1])

    # 2 ── Heading-Fonts ───────────────────────────────────────────────────
    fonts = _font_scan(p, threads=font_threads)
    meta_dir = Path("meta") / p.stem
    meta_dir.mkdir(parents=True, exist_ok=True)
    _font_write(fonts, p.name)

    _, chapters, _ = _chap_analyse(str(meta_dir / "font_cluster.json"), trace=False)
    if chapters:
        cand = [h for h in chapters.get("chapters", [])
                if _RE_BIB_HDR.search(h["text"])
                and h["page"] > (toc_page or 0)]
        if cand:
            first = min(h["page"] for h in cand)
            LOG.info("Bibliographie via Heading-Fonts → Seite %d", first)

            block, _ = _detect_text(
                p, head=0.0, tail=0.0,
                use_gpt=use_gpt, gpt_only=False, boost=boost)
            if block and block[0] <= first <= block[1]:
                return block
            return (first, first)

    # 3 ── Vollheuristik ───────────────────────────────────────────────────
    bounds, _ = _detect_text(
        p, head=head, tail=tail,
        use_gpt=use_gpt, gpt_only=False, boost=boost)
    if bounds:
        LOG.info("Bibliographie via detect() → %s", bounds)
    return bounds