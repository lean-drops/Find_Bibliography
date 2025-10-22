#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
citations_resolver.py
---------------------
Löst Kurzverweise wie "Ebd./ebd./ebenda/ibid.", "wie Anm. X", "ders./dies./idem/eadem"
auf und reichert die Zitat-Felder in der SQLite-DB (azk_footer.sqlite) an.

- Läuft dokumentweise in Lesereihenfolge (Seite → Fußnote → Zitat).
- "Ebd." erbt ALLE bibliographischen Felder vom vorherigen Zitat (gleiche Fußnote,
  sonst vorige Fußnote), überschreibt aber Seitenangaben, wenn im Rohtext vorhanden.
- "wie Anm. X" erbt von der referenzierten Fußnote (nimmt das letzte Zitat dort),
  ergänzt NUR fehlende Felder (Autor*innen, Titel, Container, Jahr, …).
- "ders./dies./idem/eadem" erbt NUR Autor*innen vom vorherigen Zitat.

Schreibt:
- aktualisierte Spalten in `citations`: authors_json, title, container_title, year,
  volume, issue, pages, publisher, place, doi, url, type, method (append "+resolve")
- detaillierte Herkunft in `citations.extra_json` unter key "resolved"

Kein Konsolen-Input. Startbar als Standalone: verarbeitet ALLE Dokumente der DB.
"""

from __future__ import annotations
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

DB_FILENAME = "../data/azk_footer.sqlite"

# ----------- Debug -----------
def dprint(msg: str) -> None:
    print(msg, flush=True)

# ----------- Regex -----------
RE_EBD   = re.compile(r"^\s*(?:vgl\.\s*)?(?:ebd\.?|ebenda|ibid\.?)\b", re.I | re.U)
RE_DERS  = re.compile(r"^\s*(?:vgl\.\s*)?(?:ders\.|dies\.|idem|eadem)\b", re.I | re.U)
RE_WIEANM= re.compile(r"\bwie\s+anm\.?\s*(\d{1,3})\b", re.I | re.U)
RE_PAGES = re.compile(r"\b(?:S\.|S:|Sp\.|Sp:)\s*([0-9]+(?:[-–][0-9]+)?[a-z]?)", re.I | re.U)
RE_NR    = re.compile(r"\bNr\.\s*([0-9]+(?:[-–][0-9]+)?)", re.I | re.U)
RE_PRINTED_FNNUM = re.compile(r"^\s*[\(\[]?\s*(\d{1,3})\s*[\)\]\.:]?\s*$")

# ----------- Helpers -----------
def _connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def _json_load(s: Optional[str]) -> Any:
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None

def _json_dump(o: Any) -> str:
    return json.dumps(o, ensure_ascii=False)

def _authors_from_json(s: Optional[str]) -> List[str]:
    val = _json_load(s)
    if isinstance(val, list):
        return [str(x) for x in val]
    return []

def _pages_from_text(txt: str) -> Optional[str]:
    m = RE_PAGES.search(txt or "")
    if m:
        return m.group(1).strip()
    m2 = RE_NR.search(txt or "")
    if m2:
        return f"Nr. {m2.group(1).strip()}"
    return None

def _printed_fn_number(marker: Optional[str]) -> Optional[int]:
    if not marker:
        return None
    m = RE_PRINTED_FNNUM.match(marker.strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

@dataclass
class Cit:
    id: int
    index_in_foot: int
    raw: str
    type: str
    authors: List[str]
    year: Optional[str]
    title: Optional[str]
    container: Optional[str]
    volume: Optional[str]
    issue: Optional[str]
    pages: Optional[str]
    publisher: Optional[str]
    place: Optional[str]
    doi: Optional[str]
    url: Optional[str]
    method: str
    extra_raw: Dict[str, Any]

@dataclass
class Foot:
    id: int
    page_no: int
    index_in_page: int
    marker: Optional[str]
    printed_no: Optional[int]
    text: str
    citations: List[Cit]

# ----------- Core resolution -----------
def _inherit_all(dst: Cit, src: Cit, overwrite_pages: Optional[str]) -> Dict[str, Any]:
    # returns dict of fields to set + explain
    fields = {
        "type": src.type or dst.type or "unknown",
        "authors_json": _json_dump(src.authors if src.authors else dst.authors),
        "year": src.year or dst.year or "",
        "title": src.title or dst.title or "",
        "container_title": src.container or dst.container or "",
        "volume": src.volume or dst.volume or "",
        "issue": src.issue or dst.issue or "",
        "pages": overwrite_pages or dst.pages or src.pages or "",
        "publisher": src.publisher or dst.publisher or "",
        "place": src.place or dst.place or "",
        "doi": src.doi or dst.doi or "",
        "url": src.url or dst.url or "",
    }
    reason = {"rule": "ebd/ibid", "pages_override": bool(overwrite_pages)}
    return fields | {"_reason": reason}

def _inherit_missing(dst: Cit, src: Cit) -> Dict[str, Any]:
    def pick(a, b):  # prefer dst if already filled
        return a if (a and (isinstance(a, str) and a.strip() or isinstance(a, list) and a)) else b or ""
    fields = {
        "type": pick(dst.type, src.type) or "unknown",
        "authors_json": _json_dump(dst.authors if dst.authors else src.authors),
        "year": pick(dst.year, src.year),
        "title": pick(dst.title, src.title),
        "container_title": pick(dst.container, src.container),
        "volume": pick(dst.volume, src.volume),
        "issue": pick(dst.issue, src.issue),
        "pages": pick(dst.pages, src.pages),
        "publisher": pick(dst.publisher, src.publisher),
        "place": pick(dst.place, src.place),
        "doi": pick(dst.doi, src.doi),
        "url": pick(dst.url, src.url),
    }
    reason = {"rule": "wie_anm", "note": "filled only missing fields"}
    return fields | {"_reason": reason}

def _inherit_authors_only(dst: Cit, src: Cit) -> Dict[str, Any]:
    fields = {"authors_json": _json_dump(src.authors if src.authors else dst.authors)}
    reason = {"rule": "ders/dies/idem/eadem"}
    return fields | {"_reason": reason}

def _update_citation(con: sqlite3.Connection, cit: Cit, updates: Dict[str, Any], resolved_from: Optional[int]) -> None:
    # merge method + extra_json
    method_new = (cit.method or "heuristic") + "+resolve"
    cur = con.cursor()
    # merge extra_json
    extra = cit.extra_raw or {}
    extra.setdefault("resolved", [])
    reason = updates.pop("_reason", {})
    extra["resolved"].append({
        "from_citation_id": resolved_from,
        **reason
    })
    cur.execute("""
        UPDATE citations SET
          type=?,
          authors_json=?,
          year=?,
          title=?,
          container_title=?,
          volume=?,
          issue=?,
          pages=?,
          publisher=?,
          place=?,
          doi=?,
          url=?,
          method=?,
          extra_json=?
        WHERE id=?
    """, (
        updates.get("type","unknown"),
        updates.get("authors_json","[]"),
        updates.get("year",""),
        updates.get("title",""),
        updates.get("container_title",""),
        updates.get("volume",""),
        updates.get("issue",""),
        updates.get("pages",""),
        updates.get("publisher",""),
        updates.get("place",""),
        updates.get("doi",""),
        updates.get("url",""),
        method_new,
        _json_dump(extra),
        cit.id
    ))
    con.commit()

# pick prev citation (same footnote, else previous footnote)
def _prev_citation(flow: List[Tuple[Foot, Cit]], idx: int) -> Optional[Cit]:
    if idx <= 0:
        return None
    # previous in flow
    for j in range(idx-1, -1, -1):
        prev = flow[j][1]
        if prev:
            return prev
    return None

def _last_citation_of_foot(foot: Foot) -> Optional[Cit]:
    return foot.citations[-1] if foot.citations else None

def _lookup_foot_by_number(history: List[Foot], number: int, before_idx: int) -> Optional[Foot]:
    """
    Sucht die Fußnote mit 'gedruckter' Nummer. Falls Nummer mehrfach vorkommt (Reset je Seite),
    nimm die NÄCHSTE rückwärts vor before_idx.
    """
    for k in range(before_idx-1, -1, -1):
        f = history[k]
        if f.printed_no == number:
            return f
    # als Fallback: erste passende überhaupt
    for f in history:
        if f.printed_no == number:
            return f
    return None

# ----------- DB fetchers -----------
def _load_document(con: sqlite3.Connection, doc_id: int) -> Tuple[List[Foot], List[Tuple[Foot, Cit]]]:
    cur = con.cursor()
    # Seiten + Fußnoten in stabiler Reihenfolge
    cur.execute("""
      SELECT f.id, p.page_no, f.index_in_page, f.marker, f.text
      FROM footnotes f
      JOIN pages p ON p.id=f.page_id
      WHERE f.document_id=?
      ORDER BY p.page_no, f.index_in_page, f.id
    """, (doc_id,))
    foots: List[Foot] = []
    for fid, pno, idx, marker, text in cur.fetchall():
        printed = _printed_fn_number(marker) if marker else None
        foots.append(Foot(
            id=fid,
            page_no=int(pno),
            index_in_page=int(idx or 0),
            marker=(marker or "").strip() or None,
            printed_no=printed,
            text=text or "",
            citations=[]
        ))
    # Citations pro Fußnote
    for f in foots:
        cur.execute("""
          SELECT id, COALESCE(index_in_footnote,0), raw_text, COALESCE(type,''), COALESCE(authors_json,'[]'),
                 COALESCE(year,''), COALESCE(title,''), COALESCE(container_title,''), COALESCE(volume,''),
                 COALESCE(issue,''), COALESCE(pages,''), COALESCE(publisher,''), COALESCE(place,''),
                 COALESCE(doi,''), COALESCE(url,''), COALESCE(method,''), COALESCE(extra_json,'{}')
          FROM citations
          WHERE footnote_id=? ORDER BY index_in_footnote, id
        """, (f.id,))
        rows = cur.fetchall()
        for r in rows:
            f.citations.append(Cit(
                id=r[0],
                index_in_foot=int(r[1]),
                raw=r[2] or "",
                type=r[3] or "unknown",
                authors=_authors_from_json(r[4]),
                year=(r[5] or "") or None,
                title=(r[6] or "") or None,
                container=(r[7] or "") or None,
                volume=(r[8] or "") or None,
                issue=(r[9] or "") or None,
                pages=(r[10] or "") or None,
                publisher=(r[11] or "") or None,
                place=(r[12] or "") or None,
                doi=(r[13] or "") or None,
                url=(r[14] or "") or None,
                method=r[15] or "heuristic",
                extra_raw=_json_load(r[16]) or {}
            ))
    # Flatten flow
    flow: List[Tuple[Foot, Cit]] = []
    for f in foots:
        for c in f.citations:
            flow.append((f, c))
    return foots, flow

# ----------- Resolution runner -----------
def resolve_document(db_path: Path, document_id: int) -> None:
    dprint(f"[INFO][RESOLVE] Dokument id={document_id} auflösen …")
    con = _connect(db_path)

    foots, flow = _load_document(con, document_id)
    if not flow:
        dprint("[WARN][RESOLVE] Keine Zitate gefunden – übersprungen.")
        return

    # Für "wie Anm. X": wir brauchen Verlauf der Fußnoten (für nearest-prior match)
    foot_history: List[Foot] = []
    foot_seen_ids: set[int] = set()

    for i, (foot, cit) in enumerate(flow):
        if foot.id not in foot_seen_ids:
            foot_history.append(foot)
            foot_seen_ids.add(foot.id)

        txt = cit.raw.strip()
        lower = txt.lower()

        did_any = False
        resolved_from_id: Optional[int] = None

        # 1) Ebd./ebd./ebenda/ibid
        if RE_EBD.match(txt):
            src = _prev_citation(flow, i)
            if src:
                pages_override = _pages_from_text(txt)
                updates = _inherit_all(cit, src, pages_override)
                _update_citation(con, cit, updates, src.id)
                did_any = True
                resolved_from_id = src.id
                dprint(f"[INFO][RESOLVE] Ebd./Ibid: cit#{cit.id} ⇐ cit#{src.id} (pages_override={bool(pages_override)})")
            else:
                dprint(f"[WARN][RESOLVE] Ebd. ohne Vorgänger (cit#{cit.id}) – übersprungen.")

        # 2) wie Anm. X
        if not did_any:
            m = RE_WIEANM.search(txt)
            if m:
                num = int(m.group(1))
                # bis zum aktuellen Fußnoten-Index rückwärts suchen
                # finde Position von aktueller Fußnote in history
                cur_pos = len(foot_history)
                ref_foot = _lookup_foot_by_number(foot_history, num, cur_pos)
                if ref_foot:
                    src = _last_citation_of_foot(ref_foot)
                    if src:
                        updates = _inherit_missing(cit, src)
                        _update_citation(con, cit, updates, src.id)
                        did_any = True
                        resolved_from_id = src.id
                        dprint(f"[INFO][RESOLVE] wie Anm. {num}: cit#{cit.id} ⇐ last cit of foot#{ref_foot.id} (cit#{src.id})")
                else:
                    dprint(f"[WARN][RESOLVE] 'wie Anm. {num}' – referenzierte Fußnote nicht gefunden (cit#{cit.id}).")

        # 3) ders./dies./idem/eadem (nur Autor*innen)
        if not did_any and RE_DERS.match(txt):
            src = _prev_citation(flow, i)
            if src and (src.authors):
                updates = _inherit_authors_only(cit, src)
                _update_citation(con, cit, updates, src.id)
                did_any = True
                resolved_from_id = src.id
                dprint(f"[INFO][RESOLVE] ders./dies.: cit#{cit.id} – Autoren ⇐ cit#{src.id}")
            elif src:
                dprint(f"[WARN][RESOLVE] ders./dies. aber vorige Zitation ohne Autoren (cit#{cit.id}).")
            else:
                dprint(f"[WARN][RESOLVE] ders./dies. ohne vorherige Zitation (cit#{cit.id}).")

        # 4) Optional: "Ebd." kann zusätzlich "wie Anm." enthalten – eine zweite Runde
        if not did_any and RE_EBD.match(txt) and RE_WIEANM.search(txt):
            # greife wie Anm. nochmals
            m = RE_WIEANM.search(txt)
            if m:
                num = int(m.group(1))
                cur_pos = len(foot_history)
                ref_foot = _lookup_foot_by_number(foot_history, num, cur_pos)
                if ref_foot:
                    src = _last_citation_of_foot(ref_foot)
                    if src:
                        pages_override = _pages_from_text(txt)
                        updates = _inherit_all(cit, src, pages_override)
                        _update_citation(con, cit, updates, src.id)
                        dprint(f"[INFO][RESOLVE] Ebd.+wie Anm.: cit#{cit.id} ⇐ foot#{ref_foot.id} cit#{src.id} (pages_override={bool(pages_override)})")
                        did_any = True

        if not did_any:
            dprint(f"[DEBUG][RESOLVE] cit#{cit.id} – keine Regel gegriffen.")

    dprint(f"[INFO][RESOLVE] Dokument id={document_id} fertig.")

def resolve_all_documents(db_path: Path) -> None:
    con = _connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT id, path FROM documents ORDER BY id")
    docs = cur.fetchall()
    if not docs:
        dprint("[WARN][RESOLVE] Keine Dokumente in DB.")
        return
    dprint(f"[INFO][RESOLVE] Dokumente gesamt: {len(docs)}")
    for doc_id, path in docs:
        dprint(f"[INFO][RESOLVE] == {doc_id}: {path}")
        resolve_document(db_path, int(doc_id))

# ----------- main -----------
def main():
    db_path = Path.cwd() / DB_FILENAME
    dprint(f"[INFO][RESOLVE] DB: {db_path}")
    if not db_path.exists():
        raise SystemExit(f"DB nicht gefunden: {db_path}")
    resolve_all_documents(db_path)
    dprint("[INFO][RESOLVE] Alles erledigt.")

if __name__ == "__main__":
    # Keine Konsolen-Interaktion – sofort los
    main()