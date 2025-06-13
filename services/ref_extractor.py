#!/usr/bin/env python3
# ─────────────────────────────────────────────────────────────────────────────
# services/ref_extractor.py
# Rev. 2025-05-03  ·  Thread-safe  ·  GPT-aware  ·  Null-safe
# ─────────────────────────────────────────────────────────────────────────────
"""
extract_references(pdf_path, pages,
                   *, use_gpt=False, line_parser=None)  →  List[Dict]

• extrahiert Text aus den angegebenen PDF-Seiten (PyMuPDF)
• erkennt Zitationsstil   (Regex → optional GPT-4-o Verfeinerung)
• parst jede Referenzzeile
      – Custom-Parser (line_parser) hat Vorrang
      – sonst eigene Regex + Fallback
• gibt komplette Records  {authors, title, publisher?, year}  zurück
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from hashlib            import sha1
from pathlib            import Path
from typing             import Callable, Dict, List, Optional

import fitz                                 # PyMuPDF
# ───────────────────────── GPT optional ─────────────────────────────────────
try:
    import openai                            # wird nur genutzt, wenn installiert
except ModuleNotFoundError:                  # pragma: no cover
    openai = None                            # type: ignore

# ───────────────────────── Logging ──────────────────────────────────────────
LOG = logging.getLogger("ref_extractor")

# ───────────────────────── Regex-Grundlagen ─────────────────────────────────
YEAR_RE   = re.compile(r"(1[5-9]\d{2}|20\d{2})")
_SHY      = "\u00AD"                         # Soft-Hyphen

_PATTERNS: dict[str, re.Pattern[str]] = {
    # APA-Style
    "apa": re.compile(
        r"^(?P<authors>.+?)\s+\((?P<year>\d{4})\)\.\s+"
        r"(?P<title>.+?)\.\s+(?P<publisher>.+?)\.", re.U),
    # Harvard
    "harvard": re.compile(
        r"^(?P<authors>.+?)\s+(?P<year>\d{4})\s+[–\-:]\s+"
        r"(?P<title>.+?)\.\s+(?P<publisher>.+?)\.", re.U),
    # Klassische Nummerierung (IEEE u. ä.)
    "num": re.compile(
        r"^\s*\[?\d+\]?\s+(?P<authors>.+?),\s+"
        r"['“\"]?(?P<title>.+?)['”\"]?,\s+(?P<publisher>.+?),\s+(?P<year>\d{4})", re.U),
    # very common author-title
    "author-title": re.compile(
        r"^(?P<authors>.+?),\s+(?P<title>.+?),\s*(?P<publisher>[^0-9]+?)\s*(?P<year>\d{4})", re.U),
}

# ───────────────────────── Hilfsfunktionen ─────────────────────────────────
def _norm(txt: Optional[str]) -> str:
    """Unicode-NFKD → Klein → Whitespace squash  (Null-safe)."""
    if not txt:
        return ""
    txt = txt.replace(_SHY, "")
    txt = unicodedata.normalize("NFKD", txt)
    return re.sub(r"\s+", " ", txt).strip()

def _merge_lines(page_text: str) -> List[str]:
    """
    verbindet *weiche* Umbrüche; z. B. wenn eine Zeile nicht auf
    . ; : , endet und die Folgezeile mit Kleinbuchstaben startet.
    """
    out, buf = [], ""
    for ln in page_text.splitlines():
        ln = ln.rstrip()
        if not ln:
            if buf:
                out.append(buf); buf = ""
            continue
        if buf and not re.search(r"[.;:,]$", buf) and ln and ln[0].islower():
            buf += " " + ln
        else:
            if buf:
                out.append(buf)
            buf = ln
    if buf:
        out.append(buf)
    return out

# ───────────────────────── Event-Loop Helper (Thread-safe) ─────────────────
def _ensure_loop() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:                      # kein Loop im aktuellen Thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

# ───────────────────────── GPT-gestützte Stilerkennung ─────────────────────
_SYS_MSG   = ("You classify bibliography lines. Reply with ONLY one word: "
              "apa | harvard | num | author-title | unknown")
_GPT_MODEL = os.getenv("GPT_MODEL", "gpt-4o-mini")
_TIMEOUT   = float(os.getenv("GPT_TIMEOUT", "7"))
_CACHE_TTL = 12 * 60 * 60                   # 12 h
_STYLE_CACHE: dict[str, tuple[str, float]] = {}

def _sample_hash(txt: str) -> str:
    return sha1(txt.encode()).hexdigest()[:12]

async def _gpt_detect(sample: str) -> str:
    if openai is None or not openai.api_key:
        return "unknown"

    key = _sample_hash(sample)
    now = time.time()
    if (hit := _STYLE_CACHE.get(key)) and now - hit[1] < _CACHE_TTL:
        return hit[0]

    try:
        rsp = await asyncio.wait_for(
            openai.ChatCompletion.acreate(
                model=_GPT_MODEL,
                messages=[{"role": "system", "content": _SYS_MSG},
                          {"role": "user",   "content": sample[:600]}],
                max_tokens=1,
                temperature=0.0,
            ),
            timeout=_TIMEOUT,
        )
        sty = rsp.choices[0].message.content.strip().lower()
        if sty not in _PATTERNS:
            sty = "unknown"
    except (asyncio.TimeoutError, Exception) as exc:
        LOG.warning("GPT style detect failed: %s", exc)
        sty = "unknown"

    _STYLE_CACHE[key] = (sty, now)
    return sty

# ───────────────────────── Fallback-Heuristik  ─────────────────────────────
_FALLBACK_RE = re.compile(
    rf"^(?P<authors>.+?)\s+[–\-]?\s*(?P<year>{YEAR_RE.pattern})[a-z]?[.,:]?\s+"
    rf"(?P<title>.+?)(?:\.\s+(?P<publisher>[^0-9]+?))?$", re.U)

def _fallback_parse(line: str) -> Optional[Dict]:
    if (m := _FALLBACK_RE.match(line)):
        try:
            return {
                "authors"  : _norm(m["authors"]),
                "year"     : int(m["year"]),
                "title"    : _norm(m["title"]),
                "publisher": _norm(m["publisher"]),
            }
        except ValueError:
            pass
    return None

# ───────────────────────── Haupt-API  ───────────────────────────────────────
def extract_references(
    pdf_path: Path,
    pages: range,
    *,
    use_gpt: bool = False,
    line_parser: Callable[[str], Optional[Dict]] | None = None,
) -> List[Dict]:
    """liefert Liste sauberer Referenz-Dictionaries."""
    # 1) Seiten-Text
    page_txt: List[str] = []
    with fitz.open(pdf_path) as doc:
        for i in pages:
            page_txt.append(doc.load_page(i).get_text("text", sort=True))

    # 2) Stil-Erkennung (Sample = erste 12 Non-Empty Zeilen)
    sample_lines = [ln for t in page_txt for ln in t.splitlines() if ln.strip()][:12]
    sample       = "\n".join(sample_lines)
    style        = next((n for n, p in _PATTERNS.items()
                         if any(p.match(l) for l in sample_lines)),
                        "unknown")
    # GPT-Fine-Tune
    if use_gpt and style == "unknown":
        loop  = _ensure_loop()
        style = loop.run_until_complete(_gpt_detect(sample))

    LOG.info("detected citation style: %s", style)

    # 3) Parsing pro Seite  (Thread-Pool)
    def _parse_page(txt: str) -> List[Dict]:
        out: List[Dict] = []
        for raw in _merge_lines(txt):
            rec: Optional[Dict] = None

            # a) Custom-Parser des Callers
            if line_parser:
                rec = line_parser(raw)

            # b) Regex gemäß Stil
            if rec is None and style in _PATTERNS:
                if (m := _PATTERNS[style].match(raw)):
                    rec = {
                        "authors"  : _norm(m["authors"]),
                        "year"     : int(m["year"]),
                        "title"    : _norm(m["title"]),
                        "publisher": _norm(m.groupdict().get("publisher")),
                    }

            # c) universal-Fallback
            if rec is None:
                rec = _fallback_parse(raw)

            # d) nur vollständige Einträge akzeptieren
            if rec and rec["authors"] and rec["title"]:
                out.append(rec)
                if os.getenv("REF_DEBUG") == "1":
                    LOG.debug("✓ %-38s  —  %-45s (%s)",
                              rec['authors'][:38], rec['title'][:45], rec['year'])
            elif os.getenv("REF_DEBUG") == "1":
                LOG.debug("✗ skipped  «%s»", raw[:80])
        return out

    refs: List[Dict] = []
    with ThreadPoolExecutor(max_workers=4) as tp:
        for chunk in tp.map(_parse_page, page_txt):
            refs.extend(chunk)

    return refs