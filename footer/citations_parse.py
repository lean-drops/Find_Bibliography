#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
citations_parse.py
------------------
Heuristische Zerlegung eines Fußnotentextes in einzelne Zitate und Feld-Parsing.
- Splits entlang ';' und ' — ' außerhalb von Klammern
- Erkennung: DOI, URL, Jahr, Seiten, Volume/Issue, 'In:'-Container
- Grobe Typisierung: article/book/chapter/web/unknown
- Autorenliste sehr konservativ (erhält 'Nachname, Vorname' Blöcke)

API:
- extract_citations_heuristic(note_text: str) -> List[dict]
"""

from __future__ import annotations
import re
from typing import Dict, List, Optional

DOI_RE = re.compile(r"\b10\.\d{4,9}/\S+\b", re.I)
URL_RE = re.compile(r"\bhttps?://\S+\b", re.I)
YEAR_RE = re.compile(r"\b(1[5-9]\d{2}|20\d{2}|21\d{2})\b")
PAGES_RE = re.compile(r"\b(?:S\.|S:|pp\.|p\.)\s*([0-9]+(?:[-–][0-9]+)?)", re.I)
VOL_ISSUE_RE = re.compile(r"\b(?:(?:Bd\.|Vol\.|Jg\.)\s*([0-9]+))?(?:\s*\((\d{4})\))?(?:\s*(?:Nr\.|No\.|Heft)\s*([0-9]+))?\b", re.I)
IN_CONTAINER_RE = re.compile(r"\bIn:\s+(.+)$", re.I)
QUOTE_TITLE_RE = re.compile(r"[\"“”„«»‚‘’‹›](.+?)[\"“”„«»‚‘’‹›]")

def dprint(msg: str) -> None:
    print(msg, flush=True)

def _split_outside_parens(text: str) -> List[str]:
    """
    Split an ';' or ' — ' only when not inside (), [], {}.
    """
    parts: List[str] = []
    buf = []
    depth = 0
    i = 0
    while i < len(text):
        ch = text[i]
        if ch in "([{":
            depth += 1
            buf.append(ch)
        elif ch in ")]}":
            depth = max(0, depth-1)
            buf.append(ch)
        elif depth == 0 and ch == ';':
            parts.append("".join(buf).strip())
            buf = []
        elif depth == 0 and text[i:i+3] == " — ":
            parts.append("".join(buf).strip())
            buf = []
            i += 2
        else:
            buf.append(ch)
        i += 1
    if buf:
        parts.append("".join(buf).strip())
    # Filter leere
    return [p for p in (p.strip().strip(",") for p in parts) if p]

def _extract_authors_prefix(chunk: str) -> List[str]:
    """
    Nimm am Anfang bis zum ersten Anzeichen von Titel (Anführungszeichen oder kursiv surrogat),
    splitte vorsichtig an ';' / ' und ' / ' and ' – behalte 'Nachname, Vorname'.
    """
    # cut at first quoted title
    qm = QUOTE_TITLE_RE.search(chunk)
    head = chunk[:qm.start()].strip() if qm else chunk.split(" In:",1)[0].strip()
    # remove trailing year-like tails
    head = re.sub(r"\(\s*\d{4}\s*\)$", "", head).strip()
    # split
    parts = re.split(r"\s+und\s+|\s+and\s+|;\s*", head)
    authors = []
    for p in parts:
        p = p.strip(" ,;")
        # grobe filter: mindestens ein Komma (Nachname, Vorname) ODER 2+ Wörter
        if not p or (',' not in p and len(p.split()) < 2):
            continue
        authors.append(p)
    # dedupe simple
    seen=set(); out=[]
    for a in authors:
        if a not in seen:
            out.append(a); seen.add(a)
    return out

def _extract_title(chunk: str) -> Optional[str]:
    m = QUOTE_TITLE_RE.search(chunk)
    return m.group(1).strip() if m else None

def _extract_container(chunk: str) -> Optional[str]:
    m = IN_CONTAINER_RE.search(chunk)
    if m:
        tail = m.group(1).strip()
        # bis vor Seitenangaben/DOI/URL abschneiden
        tail = re.split(r"(?:S\.|S:|pp\.|p\.)\s*[0-9]", tail)[0]
        tail = DOI_RE.split(tail)[0]
        tail = URL_RE.split(tail)[0]
        return tail.strip(" ,.;")
    return None

def _ctype(url: Optional[str], title_in_quotes: Optional[str], in_container: Optional[str]) -> str:
    if url:
        return "web"
    if in_container and title_in_quotes:
        return "chapter"
    if title_in_quotes and not in_container:
        return "article"
    if in_container and not title_in_quotes:
        return "chapter"
    return "book" if not title_in_quotes else "unknown"

def parse_citation_chunk(chunk: str) -> Dict[str, Optional[str] | List[str]]:
    raw = chunk.strip().strip(".")
    # Basics
    doi_m = DOI_RE.search(raw); doi = doi_m.group(0) if doi_m else None
    url_m = URL_RE.search(raw); url = url_m.group(0) if url_m else None
    year_m = YEAR_RE.search(raw); year = year_m.group(1) if year_m else None
    pages_m = PAGES_RE.search(raw); pages = pages_m.group(1) if pages_m else None
    vol_m = VOL_ISSUE_RE.search(raw)
    volume = vol_m.group(1) if vol_m and vol_m.group(1) else None
    # prefer explicit (year) if present
    if vol_m and vol_m.group(2) and not year:
        year = vol_m.group(2)
    issue = vol_m.group(3) if vol_m and vol_m.group(3) else None

    title_q = _extract_title(raw)
    container = _extract_container(raw)
    authors = _extract_authors_prefix(raw)
    ctype = _ctype(url, title_q, container)

    # very light publisher/place extraction
    publisher=None; place=None
    if ctype == "book" and "In:" not in raw:
        # simple pattern "... Ort: Verlag, Jahr"
        m = re.search(r"([A-ZÄÖÜ][A-Za-zÄÖÜäöüß\- ]+):\s*([A-ZÄÖÜ][^,]+)(?:,\s*\d{4})?", raw)
        if m:
            place = m.group(1).strip()
            publisher = m.group(2).strip()

    return {
        "raw_text": raw,
        "type": ctype,
        "authors": authors,
        "year": year,
        "title": title_q,
        "container_title": container,
        "volume": volume,
        "issue": issue,
        "pages": pages,
        "publisher": publisher,
        "place": place,
        "doi": doi,
        "url": url,
    }

def extract_citations_heuristic(note_text: str) -> List[Dict]:
    if not note_text:
        return []
    # harte splits
    chunks = _split_outside_parens(note_text)
    # wenn nur ein chunk und sehr lang, versuch sekundäre splits an '  –  ' oder ' — '
    if len(chunks) == 1 and len(chunks[0]) > 200:
        alt = re.split(r"\s[–—]\s", chunks[0])
        if len(alt) > 1:
            chunks = [a.strip().strip(",") for a in alt if a.strip()]
    out: List[Dict] = []
    for ch in chunks:
        out.append(parse_citation_chunk(ch))
    return out