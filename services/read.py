#!/usr/bin/env python3
"""
services/read.py – O(1) Look-ups in der Bibbud-DB
=================================================

Beispiele
---------
    python services/read.py "Die Druckmacher" "Kaufmann, Thomas" 2022
    python services/read.py "Sea-PredatoryProtectorsConflict"
    READ_DEBUG=1 python services/read.py "Titel …"

Wird --debug (oder READ_DEBUG=1) aktiviert, protokolliert das Skript
jedes DB-Statement, Verbindungsereignisse und den resultierenden Datensatz.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import unicodedata
from typing import Optional

import mysql.connector as mysql
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging / CLI --------------------------------------------------------------
load_dotenv()

def _setup_logging(debug: bool) -> None:
    lvl = logging.DEBUG if debug or os.getenv("READ_DEBUG") == "1" else logging.INFO
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s  %(levelname)-7s| %(message)s",
        datefmt="%H:%M:%S",
    )

LOG = logging.getLogger("read")

def _dbg(msg: str, *args) -> None:
    """Kurzalias für LOG.debug (spart Tipparbeit)."""
    LOG.debug(msg, *args)

# ---------------------------------------------------------------------------
# DB-Pool (wird einmal lazily erzeugt) ---------------------------------------
_POOL: mysql.pooling.MySQLConnectionPool | None = None

def _get_pool() -> mysql.pooling.MySQLConnectionPool:
    global _POOL
    if _POOL is None:
        _dbg("initialising MySQLConnectionPool …")
        _POOL = mysql.pooling.MySQLConnectionPool(
            pool_name  = "bibbud_read",
            pool_size  = int(os.getenv("READ_POOL_SIZE", "5")),
            host       = os.getenv("DB_HOST"),
            user       = os.getenv("DB_USER"),
            password   = os.getenv("DB_PASSWORD"),
            database   = os.getenv("DB_NAME", "Bibbud"),
            charset    = "utf8mb4",
            autocommit = True,
        )
    return _POOL

# ---------------------------------------------------------------------------
# Helper-Funktionen ----------------------------------------------------------
def _norm(s: str) -> str:
    """Lower-case + Unicode-NFD + Whitespace squash."""
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKD", s).lower()).strip()

def work_hash(title: str, authors: str, year: Optional[int]) -> str:
    raw = f"{_norm(title)}|{_norm(authors)}|{year or ''}"
    return hashlib.sha1(raw.encode()).hexdigest()

# ---------------------------------------------------------------------------
# Public API -----------------------------------------------------------------
def get_stats(title: str,
              authors: str = "",
              year: Optional[int] = None) -> Optional[dict]:
    """
    Liefert Dict
        {id, title, authors, year, uploads, bib_pages:[{start,end},…]}
    oder None falls unbekannt.
    """
    h = work_hash(title, authors, year)
    _dbg("⟹ calculated hash: %s", h)

    cnx = _get_pool().get_connection()
    cur = cnx.cursor(dictionary=True)

    try:
        # ---- 1) Werk -------------------------------------------------------
        _dbg("• SELECT work by hash")
        cur.execute("SELECT id, title, authors, year FROM works WHERE hash=%s", (h,))
        work = cur.fetchone()
        _dbg("  ↳ result: %s", work)
        if not work:
            return None

        wid = work["id"]

        # ---- 2) Upload-Zähler ---------------------------------------------
        _dbg("• COUNT uploads for work_id=%s", wid)
        cur.execute("SELECT COUNT(*) AS uploads FROM documents WHERE work_id=%s", (wid,))
        uploads = cur.fetchone()["uploads"]
        _dbg("  ↳ uploads=%s", uploads)

        # ---- 3) Bibliographie-Seiten --------------------------------------
        _dbg("• SELECT bibliography_pages for work_id=%s", wid)
        cur.execute(
            """
            SELECT start_page, end_page
              FROM bibliography_pages
              JOIN documents ON documents.id = bibliography_pages.document_id
             WHERE documents.work_id = %s
            """, (wid,))
        pages = cur.fetchall()
        _dbg("  ↳ pages=%s", pages)

        return {**work, "uploads": uploads, "bib_pages": pages}

    finally:
        cur.close()
        cnx.close()

# ---------------------------------------------------------------------------
# CLI-Interface --------------------------------------------------------------
def _parse_cli() -> argparse.Namespace:  # noqa: WPS110
    ap = argparse.ArgumentParser()
    ap.add_argument("title", help="Werktitel (Pflicht)")
    ap.add_argument("authors", nargs="?", default="", help="Autor(en)")
    ap.add_argument("year", nargs="?", type=int, help="Jahr")
    ap.add_argument("--debug", action="store_true", help="verbose SQL-Debugging")
    return ap.parse_args()

def main() -> None:
    args = _parse_cli()
    _setup_logging(args.debug)

    _dbg("CLI args: %s", vars(args))
    data = get_stats(args.title, args.authors, args.year)
    print(json.dumps(data or {"error": "work not found"},
                     ensure_ascii=False, indent=2))

if __name__ == "__main__":   # pragma: no cover
    main()