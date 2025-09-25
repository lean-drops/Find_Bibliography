#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ref_normalizer.py — Normalisierung mit Re-Parsing der ROHZEILE.
Ersetzt die frühere Version.

- Nimmt Records aus deinem Extractor (idealerweise mit rec["raw"] oder rec["line"])
- Re-parst per ref_parser.parse_reference
- Glättet Felder (Casing, Zeichensetzung) und gibt strukturierte Objekte zurück
- Optional: Deduplizierung (rapidfuzz, falls vorhanden)

Lautstarke Debug-Prints pro Eintrag.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple
import re
import unicodedata

from ref_parser import parse_reference, ParsedRef

STOPWORDS_TITLE = {
    "and","of","the","in","und","der","die","das","im","den","vom","zum","zur",
    "et","de","du","des","la","le","ou","or","to","for","from","bei","am","an"
}

def _is_all_caps(s: str) -> bool:
    letters = [c for c in s if c.isalpha()]
    if not letters: return False
    return sum(1 for c in letters if c.isupper())/len(letters) >= 0.85

def _smart_titlecase(s: str) -> str:
    ws = s.lower().split()
    out=[]
    for i,w in enumerate(ws):
        if re.fullmatch(r"[MDCLXVI]+", w.upper()):
            out.append(w.upper())
        elif i==0 or w not in STOPWORDS_TITLE:
            out.append(w.capitalize())
        else:
            out.append(w)
    return " ".join(out)

@dataclass
class NormalizedRef:
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
    raw: Dict

def _clean_title(t: str) -> str:
    t = " ".join(t.split())
    if _is_all_caps(t):
        print("[norm] Titel ALL CAPS → smart titlecase")
        t = _smart_titlecase(t)
    # OCR-Fetzen: 'epos' statt 'Epos' am Titelfang? lassen wir so, lieber konservativ
    t = re.sub(r"\s*;\s*", ": ", t)  # Semikolon als Subtitel → Doppelpunkt
    t = re.sub(r"\s*\(\s*\)\s*$", "", t)  # leere Klammern
    return t.strip(" ,;.")

def normalize_record(rec: Dict) -> NormalizedRef:
    # 1) Rohzeile bestimmen
    raw_line = rec.get("raw") or rec.get("line")
    if not raw_line:
        # Fallback: aus Feldern zusammenbauen (schlechter, aber besser als nichts)
        raw_line = " — ".join(filter(None, [
            str(rec.get("authors","")), f"“{rec.get('title','')}”", str(rec.get("publisher","")), str(rec.get("year",""))
        ]))
    print(f"[norm] RAW-LINE: {raw_line}")

    # 2) Re-Parsing
    parsed: ParsedRef = parse_reference(raw_line)

    # 3) Titel & Container glätten
    title = _clean_title(parsed.title or (rec.get("title") or ""))

    container = parsed.container_title
    if container:
        container = " ".join(container.split())
        if _is_all_caps(container):
            container = _smart_titlecase(container)

    # 4) Publisher/Place minimal glätten
    pub = parsed.publisher.strip(" ,;.") if parsed.publisher else None
    place = parsed.publisher_place.strip(" ,;.") if parsed.publisher_place else None

    # 5) Year stabilisieren (bevorzugt parsed.year)
    year = parsed.year
    if year is None and isinstance(rec.get("year"), int):
        year = rec["year"]

    nr = NormalizedRef(
        style_family=parsed.style_family,
        entry_type=parsed.entry_type,
        authors=parsed.authors,
        editors=parsed.editors,
        title=title,
        container_title=container,
        publisher=pub,
        publisher_place=place,
        year=year,
        volume=parsed.volume,
        issue=parsed.issue,
        pages=parsed.pages,
        doi=parsed.doi,
        isbn=parsed.isbn,
        url=parsed.url,
        raw=rec
    )

    print(f"[norm] OK: type={nr.entry_type} style={nr.style_family} title='{nr.title[:60]}{'…' if len(nr.title)>60 else ''}', year={nr.year}")
    return nr

def normalize_records(records: List[Dict]) -> List[NormalizedRef]:
    print(f"[norm] Starte Normalisierung (mit Re-Parsing) für {len(records)} Einträge …")
    out=[]
    for i,r in enumerate(records,1):
        print(f"[norm] {i:4d}/{len(records)}")
        out.append(normalize_record(r))
    print("[norm] Normalisierung abgeschlossen.")
    return out

# ——— Deduplizierung (optional) ——————————————————————————————
def dedupe_records(records: List[NormalizedRef], threshold: int = 92) -> List[NormalizedRef]:
    print(f"[norm] Dedupe threshold={threshold}")
    try:
        from rapidfuzz import fuzz
    except Exception:
        print("[norm] rapidfuzz nicht verfügbar – einfache Schlüssel.")
        seen=set(); out=[]
        for r in records:
            key = ((r.authors[0]['family'].lower() if r.authors else (r.editors[0]['family'].lower() if r.editors else "")),
                   r.title.lower(), r.year or 0)
            if key in seen: continue
            seen.add(key); out.append(r)
        print(f"[norm] dedupe: {len(out)}/{len(records)}")
        return out

    used=[False]*len(records); out=[]
    def keyf(i):
        r=records[i]
        fam=(r.authors[0]['family'] if r.authors else (r.editors[0]['family'] if r.editors else "")).lower()
        return fam, r.title.lower(), r.year or 0

    for i in range(len(records)):
        if used[i]: continue
        used[i]=True
        fam_i, t_i, y_i = keyf(i)
        for j in range(i+1,len(records)):
            if used[j]: continue
            fam_j, t_j, y_j = keyf(j)
            if y_i and y_j and abs(y_i-y_j)>1: continue
            if fam_i and fam_j and fam_i!=fam_j: continue
            if fuzz.token_set_ratio(t_i, t_j) >= threshold:
                used[j]=True
        out.append(records[i])
    print(f"[norm] dedupe: {len(out)}/{len(records)}")
    return out
