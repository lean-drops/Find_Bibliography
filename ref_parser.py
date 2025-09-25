#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ref_parser.py — Robuster Parser für Literaturangaben (aus ROHZEILE!).

Gibt strukturierte Felder zurück:
- authors[], editors[]  (Liste {family, given})
- title, container_title (Journal- oder Buchtitel)
- publisher, publisher_place
- year, volume, issue, pages
- doi, isbn, url
- entry_type, style_family
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import re
import unicodedata

from ref_style_detector import detect_style_and_type

# ——— Utility-Normalisierung ————————————————————————————————————————
def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)

def fix_ws(s: str) -> str:
    s = re.sub(r"[ \t\u00A0\u2000-\u200B]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()

def norm_dashes(s: str) -> str:
    s = s.replace("--", "—").replace("–", "–").replace("—", "—")
    s = re.sub(r"(?<=\d)\s*-\s*(?=\d)", "–", s)
    return s

def norm_quotes(s: str) -> str:
    s = s.replace('"', "“").replace("„", "“")
    s = s.replace("‟", "“").replace("”", "”").replace("‚", "‘")
    return s

def clean(s: str) -> str:
    return fix_ws(norm_quotes(norm_dashes(nfc(s))))

# ——— Regex ————————————————————————————————————————————————————————
RE_YEAR   = re.compile(r"\b(1[6-9]\d{2}|20\d{2}|21\d{2})\b")
RE_DOI    = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.I)
RE_ISBN   = re.compile(r"\b97[89][-\s]?\d{1,5}[-\s]?\d{1,7}[-\s]?\d{1,7}[-\s]?[\dX]\b|\b\d{1,5}[-\s]?\d{1,7}[-\s]?\d{1,7}[-\s]?[\dX]\b")
RE_URL    = re.compile(r"https?://\S+", re.I)
RE_IN     = re.compile(r"\b(in|in:|dans|en:)\b", re.I)
RE_ED     = re.compile(r"\b(ed\.?|eds\.?|éd\.?|hg\.|hrsg\.|herausg\.)\b", re.I)
RE_VOL_ISS= re.compile(r"\b(\d{1,4})\s*\(\s*(\d+)\s*\)\b")  # 12(3)
RE_PAGES  = re.compile(r"\b(pp\.?|S\.?)\s*(\d{1,5}\s*(?:[-–—]\s*\d{1,5})?)\b", re.I)
RE_RANGE  = re.compile(r"\b(\d{1,5}\s*(?:[-–—]\s*\d{1,5})?)\b")
RE_PLACE_PUB_COLON = re.compile(r"([A-ZÄÖÜ][A-Za-zÄÖÜäöüß.\- ]{2,40})\s*:\s*([^,;]{2,100})")
RE_PUB_PLACE_COMMA = re.compile(r"([^,:;]{3,100})\s*,\s*([A-ZÄÖÜ][A-Za-zÄÖÜäöüß.\- ]{2,40})$")

NAME_PARTICLES = {"von","van","der","den","de","del","da","di","du","la","le","zu","zum","zur","y"}

def split_people(text: str) -> List[Dict[str,str]]:
    print(f"[parse] split_people input: {text}")
    t = clean(text)
    # vereinheitliche Trenner
    t = re.sub(r"\s*(;|&| und | and | y )\s*", ";", t, flags=re.I)
    t = re.sub(r"\s*,\s*und\s+|\s*,\s*and\s+", ";", t, flags=re.I)
    chunks = [c.strip(" ;,") for c in t.split(";") if c.strip(" ;,")]
    people = []
    for c in chunks:
        if "," in c:
            last, first = [p.strip() for p in c.split(",",1)]
            people.append({"family": last, "given": first})
        else:
            parts = c.split()
            if len(parts)==1:
                people.append({"family": parts[0], "given": ""})
            else:
                family = parts[-1]
                given  = " ".join(parts[:-1])
                # Partikel?
                if parts[-2].lower() in NAME_PARTICLES:
                    family = parts[-2] + " " + parts[-1]
                    given  = " ".join(parts[:-2])
                people.append({"family": family, "given": given})
    print(f"[parse] split_people → {people}")
    return people

@dataclass
class ParsedRef:
    style_family: str
    entry_type: str
    authors: List[Dict[str,str]]
    editors: List[Dict[str,str]]
    title: str
    container_title: Optional[str]
    publisher: Optional[str]
    publisher_place: Optional[str]
    year: Optional[int]
    volume: Optional[str]
    issue: Optional[str]
    pages: Optional[str]
    doi: Optional[str]
    isbn: Optional[str]
    url: Optional[str]

def extract_year(s: str) -> Optional[int]:
    years = RE_YEAR.findall(s)
    if not years:
        return None
    # nimm die letzte gefundene Jahreszahl (Chicago/MLA meist hinten)
    y = int(years[-1])
    print(f"[parse] year={y}")
    return y

def strip_and_capture(pattern: re.Pattern, s: str) -> Tuple[Optional[str], str]:
    m = pattern.search(s)
    if not m:
        return None, s
    value = m.group(0)
    s2 = s[:m.start()].strip() + " " + s[m.end():].strip()
    s2 = fix_ws(s2)
    return value, s2

def parse_place_publisher(s: str) -> Tuple[Optional[str], Optional[str]]:
    # Ort: Verlag
    m = RE_PLACE_PUB_COLON.search(s)
    if m:
        place, pub = m.group(1).strip(), m.group(2).strip()
        print(f"[parse] place/publisher (colon): {place} / {pub}")
        return place, pub
    # Verlag, Ort (selten am Ende)
    m = RE_PUB_PLACE_COMMA.search(s)
    if m:
        pub, place = m.group(1).strip(), m.group(2).strip()
        print(f"[parse] place/publisher (comma): {place} / {pub}")
        return place, pub
    return None, None

def extract_title_quoted(s: str) -> Tuple[Optional[str], str]:
    # Titel in Anführungen
    m = re.search(r"“([^”]{3,200})”", s)
    if m:
        title = m.group(1).strip()
        s2 = (s[:m.start()] + " " + s[m.end():]).strip()
        print(f"[parse] quoted title: {title}")
        return title, fix_ws(s2)
    return None, s

def parse_reference(raw: str) -> ParsedRef:
    s0 = clean(raw)
    print(f"[parse] RAW: {s0}")

    # Stil/Typ erkennen
    meta = detect_style_and_type(s0)
    style_family = meta["style_family"]
    entry_type   = meta["entry_type"]

    # URLs/DOI/ISBN zuerst abtrennen
    doi, s1 = strip_and_capture(RE_DOI, s0)
    isbn, s1 = strip_and_capture(RE_ISBN, s1)
    url, s1 = strip_and_capture(RE_URL, s1)

    # Jahr
    year = extract_year(s1)

    # Autoren: Bereich vom Anfang bis vor dem ersten Anführungs-Titel oder vor "in:" oder vor Ort:Verlag
    authors: List[Dict[str,str]] = []
    editors: List[Dict[str,str]] = []

    # Versuche zuerst: Namen vor dem ersten Anführungs-Titel
    tmp_title, s2 = extract_title_quoted(s1)
    left = s1
    if tmp_title:
        left = s1[:s1.index("“")].strip()

    # Wenn 'in:' vorkommt, splitten
    container_title = None
    pages = None
    volume = None
    issue  = None

    # Grob: Autoren stehen am Anfang bis zum ersten '—' oder Punkt vor Jahr/„Titel“
    lead = left.split("—")[0].split(" . ")[0]
    lead = lead.strip(" ,;.")
    print(f"[parse] lead(authors/editors?)='{lead}'")

    # Editor-Hinweise einsammeln
    role_editors = bool(RE_ED.search(lead))
    lead_clean = RE_ED.sub("", lead).strip(" ,;.")
    people = split_people(lead_clean) if lead_clean else []

    if role_editors:
        editors = people
    else:
        authors = people

    # Titel
    title = ""
    if tmp_title:
        title = tmp_title
    else:
        # Fallback: Segment nach erstem Strich
        segs = s1.split("—")
        if len(segs) >= 2:
            title = segs[1].strip(" ,;.")
        else:
            # Fallback: nach Autorenteil bis erstes Komma/Punkt
            rest = s1[len(lead):]
            m = re.search(r"[,.]\s*", rest)
            title = rest[:m.start()].strip() if m else rest.strip()
    print(f"[parse] title='{title}'")

    # Container (in:)
    m_in = RE_IN.search(s1)
    if m_in:
        after_in = s1[m_in.end():].strip()
        # bis vor Ort:Verlag/Seiten/Jahr
        # entferne Seitenangabe
        m_pages = RE_PAGES.search(after_in) or RE_RANGE.search(after_in)
        if m_pages:
            container_title = after_in[:m_pages.start()].strip(" ,;.")
            pages = m_pages.group(1) if hasattr(m_pages, "group") else m_pages.group(0)
        else:
            # bis vor Ort:Verlag
            place_tmp, pub_tmp = parse_place_publisher(after_in)
            if place_tmp or pub_tmp:
                end_idx = after_in.find(":")  # grob
                container_title = after_in[:end_idx].strip(" ,;.")
            else:
                container_title = after_in.strip(" ,;.")
        print(f"[parse] container_title='{container_title}'")

    # Journal: Vol(Issue):Pages
    m_vi = RE_VOL_ISS.search(s1)
    if m_vi:
        volume = m_vi.group(1)
        issue  = m_vi.group(2)
        # pages eventuell nach Doppelpunkt oder Komma
        m_pg = re.search(r"[:，]\s*(\d{1,5}\s*(?:[-–—]\s*\d{1,5})?)", s1)
        if m_pg:
            pages = m_pg.group(1)
        print(f"[parse] vol/issue/pages: {volume}/{issue}/{pages}")

    if not pages:
        m_pg2 = RE_PAGES.search(s1)
        if m_pg2:
            pages = m_pg2.group(2)
            print(f"[parse] pages={pages}")

    # Ort/Verlag
    place, publisher = parse_place_publisher(s1)

    return ParsedRef(
        style_family=style_family,
        entry_type=entry_type,
        authors=authors,
        editors=editors,
        title=title,
        container_title=container_title,
        publisher=publisher,
        publisher_place=place,
        year=year,
        volume=volume,
        issue=issue,
        pages=pages,
        doi=doi,
        isbn=isbn,
        url=url,
    )
