#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
core_bridge.py
--------------
READ-ONLY Adapter zu deiner Verwaltungs-DB ("db_core").
- Unterstützt SQLite (empfohlen) über DB_CORE_SQLITE oder DB_CORE_DSN (sqlite DSN).
- Schreibt NIE in db_core. Nur SELECT.
- Introspektiert Tabellen/Spalten und sucht Matches für ein PDF:
    * Pfad (path/full_path/file_path/...)
    * Datei-Hash (sha1/sha256/md5/checksum)
    * Dateiname/-stamm (filename/name/stem)
- Gibt bestes Match als CoreHit zurück + Metadaten lesen.

Debug-Prints sehr ausführlich. Keine Konsolen-Interaktion.
"""

from __future__ import annotations
import os, sqlite3, hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

def dprint(msg: str) -> None:
    print(msg, flush=True)

@dataclass
class CoreHit:
    table: str
    id_col: str
    id_val: Any
    title: Optional[str]
    authors: List[str]
    doi: Optional[str]
    path: Optional[str]
    sha1: Optional[str]
    extra: Dict[str, Any]

# --- Verbindung (SQLite RO) ---------------------------------------------------
def _sqlite_ro_uri(dbfile: Path) -> str:
    # file:... mode=ro, immutable for safety
    return f"file:{dbfile.as_posix()}?mode=ro"

def _connect_sqlite_ro(dbfile: Path) -> sqlite3.Connection:
    if not dbfile.exists():
        raise FileNotFoundError(f"db_core SQLite nicht gefunden: {dbfile}")
    uri = _sqlite_ro_uri(dbfile)
    dprint(f"[CORE] Verbinde READ-ONLY zu {dbfile}")
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def get_core_connection() -> Tuple[sqlite3.Connection, str]:
    dsn = os.environ.get("DB_CORE_DSN", "").strip()
    pth = os.environ.get("DB_CORE_SQLITE", "").strip()
    if pth:
        return _connect_sqlite_ro(Path(pth)), "sqlite"
    if dsn.startswith("sqlite:"):
        # sqlite:////abs/pfad.sqlite
        path_part = dsn.split("sqlite:", 1)[1].lstrip("/")
        dbfile = Path("/" + path_part) if not path_part.startswith("/") else Path(path_part)
        return _connect_sqlite_ro(dbfile), "sqlite"
    # letzte Chance: relative Vermutung
    guess = Path.cwd() / "db_core.sqlite"
    return _connect_sqlite_ro(guess), "sqlite"

# --- Introspektion ------------------------------------------------------------
LIKELY_PATH = {"path","full_path","file_path","abs_path","rel_path","source_path"}
LIKELY_NAME = {"filename","name","stem","basename"}
LIKELY_SHA  = {"sha1","sha256","md5","checksum"}
LIKELY_TIT  = {"title","book_title","doc_title","work_title","main_title"}
LIKELY_AUT  = {"authors","author","creator","creators","contributors","persons","person"}
LIKELY_DOI  = {"doi","urn","handle","identifier"}
LIKELY_ID   = ("id","pk","uid","guid","uuid","rowid")

def _list_tables(con: sqlite3.Connection) -> List[str]:
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    return [r[0] for r in cur.fetchall()]

def _columns(con: sqlite3.Connection, table: str) -> List[sqlite3.Row]:
    return list(con.execute(f"PRAGMA table_info({table})").fetchall())

def _find_col(cols: List[sqlite3.Row], candidates: set[str]) -> Optional[str]:
    names = {c["name"]: c for c in cols}
    lower_map = {n.lower(): n for n in names.keys()}
    for want in candidates:
        if want in names:
            return want
        if want in lower_map:
            return lower_map[want]
    # we also try "file__path" style
    for n in names:
        if n.lower().replace("__","_") in candidates:
            return n
    return None

def _id_col(cols: List[sqlite3.Row]) -> str:
    names = [c["name"] for c in cols]
    for k in LIKELY_ID:
        if k in names:
            return k
    # fallback: first column
    return names[0]

# --- Suche --------------------------------------------------------------------
def _normpath(p: Path) -> str:
    return str(p.resolve())

def _sha1_of_path(p: Path) -> Optional[str]:
    try:
        h = hashlib.sha1()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1<<20), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def find_core_item_for_document(pdf_path: Path,
                                known_sha1: Optional[str] = None,
                                title_guess: Optional[str] = None) -> Optional[CoreHit]:
    con, dialect = get_core_connection()
    if dialect != "sqlite":
        dprint("[CORE][WARN] Nur SQLite wird aktuell unterstützt.")
        return None

    tables = _list_tables(con)
    dprint(f"[CORE] Tabellen in db_core: {len(tables)}")

    abs_path = _normpath(pdf_path)
    fname    = pdf_path.name
    stem     = pdf_path.stem
    sha1     = known_sha1 or _sha1_of_path(pdf_path)

    best_hit: Optional[CoreHit] = None

    for t in tables:
        cols = _columns(con, t)
        if not cols:
            continue
        name_path = _find_col(cols, LIKELY_PATH)
        name_name = _find_col(cols, LIKELY_NAME)
        name_sha  = _find_col(cols, LIKELY_SHA)
        name_tit  = _find_col(cols, LIKELY_TIT)
        name_aut  = _find_col(cols, LIKELY_AUT)
        name_doi  = _find_col(cols, LIKELY_DOI)
        idcol     = _id_col(cols)

        # (1) Pfad exakt
        if name_path:
            cur = con.execute(f"SELECT * FROM {t} WHERE LOWER({name_path})=LOWER(?) LIMIT 1", (abs_path,))
            row = cur.fetchone()
            if row:
                best_hit = _row_to_hit(row, t, idcol, name_tit, name_aut, name_doi, name_path, name_sha)
                dprint(f"[CORE] Match via path in {t}.{name_path}")
                break

            # evtl. nur Dateiname gespeichert
            cur = con.execute(f"SELECT * FROM {t} WHERE LOWER({name_path}) LIKE LOWER(?) LIMIT 1", (f"%{fname}%",))
            row = cur.fetchone()
            if row:
                best_hit = _row_to_hit(row, t, idcol, name_tit, name_aut, name_doi, name_path, name_sha)
                dprint(f"[CORE] Match via LIKE(path) in {t}.{name_path}")
                break

        # (2) Hash
        if sha1 and name_sha:
            cur = con.execute(f"SELECT * FROM {t} WHERE {name_sha}=? LIMIT 1", (sha1,))
            row = cur.fetchone()
            if row:
                best_hit = _row_to_hit(row, t, idcol, name_tit, name_aut, name_doi, name_path, name_sha)
                dprint(f"[CORE] Match via sha1 in {t}.{name_sha}")
                break

        # (3) Name/Stamm
        if name_name:
            cur = con.execute(f"SELECT * FROM {t} WHERE LOWER({name_name})=LOWER(?) LIMIT 1", (fname,))
            row = cur.fetchone()
            if row:
                best_hit = _row_to_hit(row, t, idcol, name_tit, name_aut, name_doi, name_path, name_sha)
                dprint(f"[CORE] Match via filename in {t}.{name_name}")
                break
            cur = con.execute(f"SELECT * FROM {t} WHERE LOWER({name_name})=LOWER(?) LIMIT 1", (stem,))
            row = cur.fetchone()
            if row:
                best_hit = _row_to_hit(row, t, idcol, name_tit, name_aut, name_doi, name_path, name_sha)
                dprint(f"[CORE] Match via stem in {t}.{name_name}")
                break

    if not best_hit:
        dprint("[CORE] Kein Treffer in db_core.")
    return best_hit

def _row_to_hit(row: sqlite3.Row, table: str, idcol: str,
                tit: Optional[str], aut: Optional[str], doi: Optional[str],
                pth: Optional[str], sh: Optional[str]) -> CoreHit:
    title = (row[tit] if tit and tit in row.keys() else None) or None
    doi_v = (row[doi] if doi and doi in row.keys() else None) or None
    path_v= (row[pth] if pth and pth in row.keys() else None) or None
    sha_v = (row[sh]  if sh  and sh  in row.keys() else None) or None
    # Autoren heuristisch splitten (lassen Original als extra drin)
    authors_raw = (row[aut] if aut and aut in row.keys() else None)
    authors: List[str] = []
    if authors_raw:
        txt = str(authors_raw)
        parts = [p.strip() for p in txt.replace("|",";").split(";")]
        authors = [p for p in parts if p]
    extra = {k: row[k] for k in row.keys()}
    return CoreHit(
        table=table, id_col=idcol, id_val=row[idcol],
        title=title, authors=authors, doi=doi_v, path=path_v, sha1=sha_v, extra=extra
    )

def get_core_metadata(table: str, id_col: str, id_val: Any) -> Dict[str, Any]:
    con, _ = get_core_connection()
    sql = f"SELECT * FROM {table} WHERE {id_col}=? LIMIT 1"
    row = con.execute(sql, (id_val,)).fetchone()
    if not row:
        return {}
    return {k: row[k] for k in row.keys()}