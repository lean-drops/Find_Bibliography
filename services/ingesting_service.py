#!/usr/bin/env python3
# ─────────────────────────────────────────────────────────────────────────────
#  services/ingest_service.py
#  ---------------------------------------------------------------------------
#  1)  Bibliographie-Seiten finden          (find_bibliography.detect_bibliography)
#  2)  PDF + Metadaten in die DB upserten   (works • documents • bibliography_pages)
#      • Neues Feld  works.analysed = 1     ⇢ eigenes PDF vorhanden
#  3)  (opt.) Referenzen extrahieren        (services.bib_handler.process_document)
# ─────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import hashlib
import logging
import os
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from pathlib            import Path
from typing             import Optional, Tuple

import mysql.connector as mysql
from dotenv             import load_dotenv

from services.delb.find_bibliography import detect_bibliography
from services.bib_handler            import process_document

# ───────────── Logging / ENV ────────────────────────────────────────────────
load_dotenv()

LOG = logging.getLogger("ingest")
LOG.setLevel(os.getenv("INGEST_LOG", "INFO").upper())

_POOL: mysql.pooling.MySQLConnectionPool | None = None
_BG_POOL = ThreadPoolExecutor(max_workers=4)          # async Ref-Parsing

# ───────────── Utility-Funktionen ───────────────────────────────────────────
YEAR_RE = re.compile(r"(19|20)\d{2}")

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ",
                  unicodedata.normalize("NFKD", s).lower()).strip()

def _hash_work(title: str, authors: str,
               publisher: str, year: Optional[int]) -> str:
    raw = f"{_norm(title)}|{_norm(authors)}|{_norm(publisher)}|{year or ''}"
    return hashlib.sha1(raw.encode()).hexdigest()

def _meta_from_fname(fname: str) -> Tuple[str, str, str, Optional[int]]:
    """
    Heuristisches (title, authors, publisher, year) aus einem
    LibGen-typischen Dateinamen:

        "Author – Title _Year_ Publisher.pdf"
        "Title (Publisher Year).pdf"
        …
    """
    stem = Path(fname).stem
    year  = None
    publ  = ""

    # Year
    if (m := YEAR_RE.search(stem)):
        year = int(m.group())

    # very naive split heuristics
    parts = re.split(r" - | _| \(|\)", stem, maxsplit=3)
    parts = [p.strip(" _-") for p in parts if p.strip(" _-")]

    author = title = ""
    if len(parts) == 1:
        title = parts[0]
    elif len(parts) == 2:
        author, title = parts
    elif len(parts) >= 3:
        author, title, publ = parts[:3]

    return title, author, publ, year

# ───────────── Lazy-Pool-Factory ────────────────────────────────────────────
def _get_pool() -> mysql.pooling.MySQLConnectionPool:
    global _POOL
    if _POOL is None:
        _POOL = mysql.pooling.MySQLConnectionPool(
            pool_name   = "bibbud_pool",
            pool_size   = int(os.getenv("DB_POOL_SIZE", "10")),
            host        = os.getenv("DB_HOST"),
            user        = os.getenv("DB_USER"),
            password    = os.getenv("DB_PASSWORD"),
            database    = os.getenv("DB_NAME", "Bibbud"),
            charset     = "utf8mb4",
            autocommit  = False,
        )
    return _POOL

# ───────────── Haupt-API ────────────────────────────────────────────────────
def ingest_single(
    pdf_path   : Path,
    *,
    tail_ratio : float = 0.25,
    parse_refs : bool  = True,
    async_refs : bool  = False,
) -> Tuple[str, Tuple[int, int] | None]:
    """
    Lädt **ein** PDF in die Datenbank und gibt
        (Dateiname, (first_page,last_page) | None)   zurück
    """

    # 1 · Bibliographie-Seiten finden ---------------------------------------
    bounds = detect_bibliography(pdf_path, tail=tail_ratio)

    # 2 · Metadaten + Hash ---------------------------------------------------
    title, authors, publisher, year = _meta_from_fname(pdf_path.name)
    w_hash = _hash_work(title, authors, publisher, year)

    # 3 · Transaktion --------------------------------------------------------
    with _get_pool().get_connection() as cnx, cnx.cursor() as cur:
        try:
            # works (analysed = 1 bei eigenem PDF)
            cur.execute("SELECT id FROM works WHERE hash=%s", (w_hash,))
            row = cur.fetchone()
            if row:
                wid = row[0]
                cur.execute("UPDATE works SET analysed=1 WHERE id=%s", (wid,))
            else:
                cur.execute("""
                    INSERT INTO works (hash,title,authors,publisher,year,analysed)
                    VALUES (%s,%s,%s,%s,%s,1)
                """, (w_hash, title, authors, publisher, year))
                wid = cur.lastrowid

            # documents
            cur.execute("""
                INSERT INTO documents (work_id,filename,filepath,filesize)
                VALUES (%s,%s,%s,%s)
            """, (wid, pdf_path.name, str(pdf_path), pdf_path.stat().st_size))
            did = cur.lastrowid

            # bibliography_pages
            if bounds:
                cur.execute("""
                    INSERT INTO bibliography_pages (document_id,start_page,end_page)
                    VALUES (%s,%s,%s)
                """, (did, *bounds))
            cnx.commit()

        except Exception as exc:
            cnx.rollback()
            LOG.error("Ingest failed for %s – %s", pdf_path, exc)
            raise

    # 4 · Referenz-Extraktion -----------------------------------------------
    if bounds and parse_refs and os.getenv("PARSE_REFS", "1") != "0":
        if async_refs:
            _BG_POOL.submit(process_document, did)
        else:
            process_document(did)

    return pdf_path.name, bounds