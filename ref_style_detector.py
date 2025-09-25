#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ref_style_detector.py — Heuristische Stil- & Typ-Erkennung für Literaturangaben.

Erkennt:
- style_family: 'author-year', 'numeric', 'note-chicago', 'mla-like', 'other'
- entry_type:   'journal-article', 'book', 'chapter', 'proceedings', 'thesis', 'report', 'other'

Vorgehen:
- Scoring per Regex-Signalen (mehrsprachig: de/en/fr/it).
- Keine externen Abhängigkeiten. Laute Debug-Prints (keine Logfiles).
"""

from __future__ import annotations
import re
from typing import Dict, Tuple

# ——— Signale (Regex) ————————————————————————————————————————————————
RE_YEAR      = re.compile(r"\b(1[6-9]\d{2}|20\d{2}|21\d{2})\b")
RE_DOI       = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.I)
RE_ISBN      = re.compile(r"\b(?:97[89][-\s]?)?\d{1,5}[-\s]?\d{1,7}[-\s]?\d{1,7}[-\s]?[\dX]\b")
RE_PAGES     = re.compile(r"\b(pp\.?|S\.?|pages?)\s*\d{1,5}\s*(?:[-–—]\s*\d{1,5})?\b", re.I)
RE_RANGE     = re.compile(r"\b\d{1,5}\s*[-–—]\s*\d{1,5}\b")
RE_VOL_ISS   = re.compile(r"\b\d{1,4}\s*(?:\(\s*\d+\s*\))\b")          # 12(3)
RE_VOL_ONLY  = re.compile(r"\b(?:vol\.?|Bd\.?)\s*\d{1,4}\b", re.I)
RE_ISSUE     = re.compile(r"\b(?:no\.?|Heft|nr\.)\s*\d+\b", re.I)
RE_IN        = re.compile(r"\b(?:in|in:|dans|en:)\b", re.I)
RE_ED        = re.compile(r"\b(?:ed\.?|eds\.?|éd\.?|hg\.|hrsg\.|herausg\.)\b", re.I)
RE_PLACE_PUB = re.compile(r"\b[A-ZÄÖÜ][A-Za-zÄÖÜäöüß.\- ]{2,30}\s*:\s*[A-Z][^\d,;]{2,}\b")  # Ort: Verlag
RE_URL       = re.compile(r"https?://\S+", re.I)
RE_BRACKETED = re.compile(r"^\s*\[\d+\]\s*")  # [12] Numeric

def detect_style_and_type(raw: str) -> Dict[str, str]:
    s = " ".join(raw.split())
    print(f"[style] Eingabe: {s[:120]}{'…' if len(s)>120 else ''}")

    signals = {
        "year": bool(RE_YEAR.search(s)),
        "doi": bool(RE_DOI.search(s)),
        "isbn": bool(RE_ISBN.search(s)),
        "pages_kw": bool(RE_PAGES.search(s)),
        "range": bool(RE_RANGE.search(s)),
        "vol_iss": bool(RE_VOL_ISS.search(s)),
        "vol_kw": bool(RE_VOL_ONLY.search(s)),
        "issue_kw": bool(RE_ISSUE.search(s)),
        "in_kw": bool(RE_IN.search(s)),
        "ed_kw": bool(RE_ED.search(s)),
        "place_pub": bool(RE_PLACE_PUB.search(s)),
        "url": bool(RE_URL.search(s)),
        "bracket_num": bool(RE_BRACKETED.search(s)),
    }
    print(f"[style] Signale: {signals}")

    # — Stil —————————————————————————
    style_family = "other"
    score_author_year = 0
    score_numeric     = 0
    score_note        = 0
    score_mla         = 0

    # Author-Year: Autor(en) + Jahr in Klammern oder nah nach Autor
    if re.search(r"\([12]\d{3}\)", s) or re.search(r"[A-Z][A-Za-z\-]+[, ]+\d{4}\b", s):
        score_author_year += 2
    if signals["year"]:
        score_author_year += 1

    # Numeric: [12], [1], 12. vor Autoren
    if signals["bracket_num"] or re.search(r"^\s*\d+\.\s", s):
        score_numeric += 2

    # Note/Chicago-like: viele Kommas, 'ed./Hg.', Ort:Verlag, Jahr am Ende
    if signals["ed_kw"] and signals["place_pub"] and signals["year"]:
        score_note += 2

    # MLA-like (City: Publisher, Year) + Anführungszeichen beim Titel, wenig Jahr-Klammern
    if signals["place_pub"] and "”" in s or "\"" in s:
        score_mla += 2

    scores = {
        "author-year": score_author_year,
        "numeric": score_numeric,
        "note-chicago": score_note,
        "mla-like": score_mla
    }
    style_family = max(scores, key=scores.get)
    if scores[style_family] == 0:
        style_family = "other"
    print(f"[style] Scores: {scores} → style_family={style_family}")

    # — Typ ———————————————————————————
    entry_type = "other"
    if signals["in_kw"] and signals["ed_kw"]:
        entry_type = "chapter"  # Kapitel in Sammelband
    elif signals["vol_iss"] or (signals["vol_kw"] and signals["range"]):
        entry_type = "journal-article"
    elif signals["place_pub"] and not signals["in_kw"]:
        entry_type = "book"
    elif re.search(r"\b(thesis|diss\.?|dissertation)\b", s, re.I):
        entry_type = "thesis"
    elif re.search(r"\bproceedings|conf\.|konferenz|tagung\b", s, re.I):
        entry_type = "proceedings"

    print(f"[style] entry_type={entry_type}")
    return {"style_family": style_family, "entry_type": entry_type}
