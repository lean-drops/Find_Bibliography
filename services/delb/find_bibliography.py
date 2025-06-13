#!/usr/bin/env python3
# ─────────────────────────────────────────────────────────────────────────────
# services/delb/find_bibliography.py
# Robust detector for bibliography / reference blocks
# rev. 2025-05-01 · v5
# ----------------------------------------------------------------------------
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import math
import os
import re
import ssl
import statistics
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from time import perf_counter
from typing import List, Optional, Sequence, Tuple

import fitz                                 # PyMuPDF
from dotenv import load_dotenv
from tqdm import tqdm

# ───────────────────────── Logging ──────────────────────────────────────────
LOG = logging.getLogger("bibdet")
dbg = LOG.debug


def _init_log(debug: bool = False) -> None:
    logging.basicConfig(
        level="DEBUG" if debug else "INFO",
        format="%(asctime)s %(levelname)-7s| %(funcName)s:%(lineno)-3d – %(message)s",
        datefmt="%H:%M:%S",
    )


# ───────────────────────── OpenAI (optional) ────────────────────────────────
load_dotenv()
try:
    import openai
except ModuleNotFoundError:                  # pragma: no cover
    openai = None                            # type: ignore

def _init_openai() -> None:
    if openai is None:
        return
    if not (key := os.getenv("OPENAI_API_KEY", "")):
        return
    openai.api_key = key

    # macOS / some alpine builds → TLS-work-around
    if os.getenv("OPENAI_INSECURE_TLS") == "1":
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        from openai import _base_client
        _base_client._make_session()
        _base_client._session._ssl = ctx      # type: ignore[attr-defined]

_init_openai()

# ───────────────────────── Terminologie (CSV) ──────────────────────────────
_TERM_CSV = Path(__file__).with_name("bibliography_terms.csv")
if _TERM_CSV.exists():
    with _TERM_CSV.open(newline="", encoding="utf-8") as fp:
        reader = csv.reader(fp)
        BIB_TERMS = {cell.strip().lower()
                     for row in reader for cell in row if cell.strip()}
else:                                        # Development-Fallback
    BIB_TERMS = {
        "references", "bibliography", "literatur",
        "literaturverzeichnis", "works cited",
        "literature cited", "quellen"
    }

_RE_TERMS = re.compile("|".join(map(re.escape, BIB_TERMS)), re.I)

# ───────────────────────── Regex-Pools (DOI, Jahr …) ────────────────────────
_RE_DOI        = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+", re.I)
_RE_YEAR_PAREN = re.compile(r"\(?\b(1[5-9]\d{2}|20\d{2})[a-z]?\b\)?")
_RE_YEAR_BARE  = re.compile(r"\b(1[5-9]\d{2}|20\d{2})[a-z]?\b")
_RE_NUM        = re.compile(r"^\s*\[?\d{1,3}\]?[:.) ]")
_RE_AUTH       = re.compile(r"^[A-ZÄÖÜ][\w’'\-ÄÖÜäöüß]+,\s+[A-Z](?:[A-Z]|\w+)?\.?")

# ───────────────────────── Scoring-Parameter ───────────────────────────────
HDR_W, CITE_W  = 0.60, 0.40
HDR_CAPS_MIN   = 0.45
CITE_RATIO_THR = 0.30
MIN_BLOCK_LEN  = 2
MIN_SCORE_ABS  = 0.45         # Untergrenze für leere Seiten

# GPT-Feintuning
_GPT_MODEL  = "gpt-4o-mini"
_GPT_SYS    = "Only reply BIB if page snippet is bibliography; otherwise NO."
_GPT_MAXTOK = 1

# ───────────────────────── Heuristik pro Seite ─────────────────────────────
def _is_cite_line(line: str) -> bool:
    return bool(
        _RE_DOI.search(line)
        or _RE_NUM.match(line)
        or (_RE_AUTH.match(line) and _RE_YEAR_BARE.search(line))
        or _RE_YEAR_BARE.search(line)
    )

def _score_page(txt: str) -> float:
    if not txt.strip():
        return 0.0

    # Header-Analyse
    hdr   = " ".join(txt.splitlines()[:8])
    tokens = re.findall(r"\w+", hdr)
    caps  = sum(tok.isupper() or tok.istitle() for tok in tokens) / (len(tokens) or 1)
    hdr_bonus = 1.0 if (_RE_TERMS.search(hdr) and caps >= HDR_CAPS_MIN) else 0.6

    # Zitier-Dichte
    lines = [l.strip() for l in txt.splitlines() if l.strip()]
    cite_ratio = sum(_is_cite_line(l) for l in lines) / (len(lines) or 1)

    score = HDR_W * hdr_bonus + CITE_W * min(cite_ratio / CITE_RATIO_THR, 1.0)
    return max(score, MIN_SCORE_ABS) if cite_ratio >= 0.05 else score

async def _gpt_flag(txt: str, sem: asyncio.Semaphore) -> bool:
    """True→ GPT hält Seite sicher für Bibliographie."""
    if openai is None or not openai.api_key:
        return False
    async with sem:
        r = await openai.ChatCompletion.acreate(
            model=_GPT_MODEL,
            messages=[
                {"role": "system", "content": _GPT_SYS},
                {"role": "user",   "content": txt[:1000]},
            ],
            temperature=0.0,
            max_tokens=_GPT_MAXTOK,
        )
    return r.choices[0].message.content.strip().upper().startswith("B")

def _choose_better(cur: Tuple[int, int] | None,
                   cand: Tuple[int, int],
                   scores: Sequence[float]) -> Tuple[int, int]:
    """Pick interval with higher mean-score (tie-break = longer)."""
    if cur is None:
        return cand
    cur_avg  = statistics.mean(scores[cur[0]:cur[1]+1])
    cand_avg = statistics.mean(scores[cand[0]:cand[1]+1])
    if cand_avg > cur_avg or (cand_avg == cur_avg and cand[1]-cand[0] > cur[1]-cur[0]):
        return cand
    return cur

# ───────────────────────── Text-Puller (Head / Tail) ────────────────────────
def _pull_pages(p: Path, head: float, tail: float) -> List[Tuple[int, str]]:
    with fitz.open(p) as doc:
        n        = doc.page_count
        head_n   = math.ceil(n * head)
        tail_n   = math.ceil(n * tail)
        out: list[Tuple[int, str]] = []

        # Kopf
        for i in range(head_n):
            out.append((i, doc.load_page(i).get_text("text", sort=True)))

        # Schwanz
        for i in range(n-1, max(-1, n-tail_n-1), -1):
            out.append((i, doc.load_page(i).get_text("text", sort=True)))

        dbg("pulled %d/%d pages (head=%d, tail=%d)", len(out), n, head_n, tail_n)
        return out

# ───────────────────────── Kern-Detector (reiner Text) ──────────────────────
def _detect_block(
    pdf      : Path,
    head     : float = 0.05,
    tail     : float = 0.25,
    use_gpt  : bool  = False,
    gpt_only : bool  = False,
    boost    : float = 1.0,
) -> Tuple[Optional[Tuple[int, int]], Optional[str]]:
    """
    Liefert ((first,last), page_text_of_first)  oder  (None,None)
    """

    def _evaluate(pages: List[Tuple[int, str]]) -> Tuple[int, int] | None:
        # Seitenscoring parallel
        scores = [0.0] * len(pages)
        with ThreadPoolExecutor(max_workers=min(8, os.cpu_count() or 4)) as tp:
            for i, sc in enumerate(tp.map(lambda p: _score_page(p[1]), pages)):
                scores[i] = sc
        med = statistics.median(scores)
        dbg("scores=%s  median=%.3f", scores, med)

        # GPT-Verfeinerung
        if use_gpt and openai and openai.api_key:
            sem = asyncio.Semaphore(5)

            async def refine() -> None:
                tasks = []
                for i, (_, txt) in enumerate(pages):
                    if gpt_only or scores[i] < 0.75:          # nur unsichere Seiten
                        tasks.append(asyncio.create_task(_gpt_flag(txt, sem)))
                    else:
                        tasks.append(asyncio.create_task(asyncio.sleep(0)))
                for i, ok in enumerate(await asyncio.gather(*tasks)):
                    if ok:
                        scores[i] = 1.0

            try:
                asyncio.run(refine())
            except Exception as exc:                         # pragma: no cover
                LOG.warning("GPT-skip: %s", exc)

        # bestes zusammenhängendes Intervall
        best: Tuple[int, int] | None = None
        cur  : int  | None = None
        for idx, sc in enumerate(scores):
            if sc >= med * boost:
                cur = idx if cur is None else cur
            elif cur is not None:
                best = _choose_better(best, (cur, idx-1), scores); cur = None
        if cur is not None:
            best = _choose_better(best, (cur, len(scores)-1), scores)

        if best and (best[1]-best[0]+1) >= MIN_BLOCK_LEN:
            abs_pages = [pages[i][0]+1 for i in range(best[0], best[1]+1)]
            return min(abs_pages), max(abs_pages)
        return None

    t0 = perf_counter()
    dbg("=== %s ===", pdf.name)

    # Schnell-Pfad: Head/Tail
    res = _evaluate(_pull_pages(pdf, head, tail))
    if res:
        first_txt = fitz.open(pdf).load_page(res[0]-1).get_text("text", sort=True)
        LOG.debug("fast-path OK (%.2fs)", perf_counter()-t0)
        return res, first_txt

    # Full-Scan
    dbg("fast miss → fullscan")
    pages = [(i, pg.get_text("text", sort=True)) for i, pg in enumerate(fitz.open(pdf))]
    res   = _evaluate(pages)
    if res:
        LOG.debug("fullscan OK (%.2fs)", perf_counter()-t0)
        return res, pages[res[0]-1][1]

    LOG.debug("no match (%.2fs)", perf_counter()-t0)
    return (None, None)

# ───────────────────────── Orchestrator (ToC ▸ Keywords ▸ Fonts ▸ Heuristik)
# Späte-Imports (erst hier, sonst Zirkelschleife beim Unit-Test)
from services.delb.keyword_hits    import analyse_pdf   as _kw_pages
from services.delb.scan_fonts      import analyse_pdf   as _font_scan, write_json as _font_write
from services.delb.detect_chapters import analyse_file  as _chap_analyse
from services.delb.extract_toc     import _scan_pdf     as _toc_scan

_TOC_KEYS = {"bibliograph", "literatur", "reference", "works cited", "literature cited"}
_TOC_NUM  = re.compile(r"(?:\.{2,}|\s)(\d{1,4})\s*$")      # Inhaltsverz. → ………… 391

def _bib_from_toc(lines: list[str]) -> int | None:
    for ln in lines:
        low = ln.lower()
        if any(k in low for k in _TOC_KEYS):
            if (m := _TOC_NUM.search(ln)):
                try:
                    return int(m.group(1))
                except ValueError:
                    continue
    return None

_RE_BIB_HDR = re.compile(
    r"\b(bibliograph\w*|references?|reference\s+list|works\s+cited|"
    r"literaturverzeichnis|quellen(?:verzeichnis| und literatur)?)\b", re.I
)

def detect_bibliography(                       # Public API
    pdf          : str | Path,
    *,
    kw_workers   : int | None = None,
    kw_min_block : int        = 2,
    font_threads : int | None = None,
    head         : float      = 0.05,
    tail         : float      = 0.25,
    use_gpt      : bool       = False,
    boost        : float      = 1.0,
) -> Optional[Tuple[int, int]]:
    """
    Rückgabe (first_page, last_page)  oder  None.
    Pipeline:
        0. ToC-Jackpot
        1. Keyword-Block
        2. Heading-Fonts
        3. Vollheuristik (Text only)
    """
    pdf_path     = Path(pdf)
    kw_workers   = kw_workers   or max(2, (os.cpu_count() or 4) // 2)
    font_threads = font_threads or (os.cpu_count() or 4)

    # 0) Inhaltsverzeichnis --------------------------------------------------
    toc_pg, toc_lines = _toc_scan(pdf_path, max_pages=None, use_ocr=False)
    if (bib := _bib_from_toc(toc_lines)):
        LOG.info("Bibliographie via ToC → Seite %d", bib)
        return (bib, bib)                     # konservativ: 1-Seiten-Block

    # 1) Keyword-Hits --------------------------------------------------------
    kw_pages: List[int] = _kw_pages(pdf_path, workers=kw_workers, debug=False)
    kw_pages.sort()
    if kw_pages:
        runs, cur = [], [kw_pages[0]]
        for p in kw_pages[1:]:
            if p == cur[-1] + 1:
                cur.append(p)
            else:
                runs.append(cur); cur = [p]
        runs.append(cur)
        best_run = max(runs, key=len)
        if len(best_run) >= kw_min_block:
            LOG.info("Bibliographie via Keyword-Block %s", best_run)
            return best_run[0], best_run[-1]

    # 2) Heading-Fonts -------------------------------------------------------
    toc_barrier = toc_pg or 0
    fonts       = _font_scan(pdf_path, threads=font_threads)

    meta_dir  = Path("meta") / pdf_path.stem
    meta_dir.mkdir(parents=True, exist_ok=True)
    json_path = meta_dir / "font_cluster.json"
    _font_write(fonts, pdf_path.name)        # Cache aktualisieren

    _, chaps, _ = _chap_analyse(str(json_path), trace=False)
    if chaps:
        bib_hdrs = [h for h in chaps.get("chapters", [])
                    if _RE_BIB_HDR.search(h["text"]) and h["page"] > toc_barrier]
        if bib_hdrs:
            first = min(h["page"] for h in bib_hdrs)
            LOG.info("Bibliographie via Heading-Fonts → Seite %d", first)
            block, _ = _detect_block(
                pdf_path, head=0.0, tail=0.0,
                boost=boost, use_gpt=use_gpt, gpt_only=False)
            if block and block[0] <= first <= block[1]:
                return block
            return (first, first)

    # 3) Vollheuristik -------------------------------------------------------
    bounds, _ = _detect_block(
        pdf_path, head=head, tail=tail,
        boost=boost, use_gpt=use_gpt, gpt_only=False)
    if bounds:
        LOG.info("Bibliographie via detect() → %s", bounds)
    return bounds

# ───────────────────────── Batch-Runner (Ordner) ────────────────────────────
def _batch(
    paths   : List[Path],
    workers : int,
    **kwargs,
) -> dict[str, Optional[Tuple[int, int]]]:
    out: dict[str, Optional[Tuple[int, int]]] = {}
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(detect_bibliography, p, **kwargs): p for p in paths}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="PDFs"):
            out[futs[fut].name] = fut.result()
    return out

# ───────────────────────── CLI / main() ─────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser("Detect bibliography pages fast & accurately")
    ap.add_argument("path", type=Path, help="PDF-Datei oder Verzeichnis")
    ap.add_argument("--head", type=float, default=0.05)
    ap.add_argument("--tail", type=float, default=0.25)
    ap.add_argument("--gpt", action="store_true")
    ap.add_argument("--boost", type=float, default=1.0)
    ap.add_argument("-j", type=int, default=os.cpu_count() or 4, help="Prozesse")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    _init_log(args.debug)

    if args.path.is_file():
        res = detect_bibliography(
            args.path, head=args.head, tail=args.tail,
            use_gpt=args.gpt, boost=args.boost)
        print(json.dumps({args.path.name: res}, indent=2, ensure_ascii=False))
        return

    if args.path.is_dir():
        pdfs = sorted(args.path.glob("*.pdf"))
        if not pdfs:
            sys.exit("Keine PDF-Dateien gefunden.")
        res = _batch(
            pdfs, workers=args.j,
            head=args.head, tail=args.tail,
            use_gpt=args.gpt, boost=args.boost)
        print(json.dumps(res, indent=2, ensure_ascii=False))
        return

    sys.exit("Pfad ist weder Datei noch Verzeichnis.")

# ----------------------------------------------------------------------------
if __name__ == "__main__":          # pragma: no cover
    main()