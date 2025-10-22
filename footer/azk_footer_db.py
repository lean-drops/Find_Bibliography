#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
azk_footer_db.py
----------------
SQLite-DB 'azk_footer.sqlite' (WAL, FK on). Nur EXTRAKTION – KEINE DEDUPES.
Tabellen:
- runs
- documents
- pages
- footnotes
- citations
Hilfsfunktionen: ensure_schema(), start_run(), upsert_document(), upsert_page(),
insert_footnote(), insert_citation().
"""

from __future__ import annotations
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional

DB_FILENAME = "../data/azk_footer.sqlite"

def dprint(msg: str) -> None:
    print(msg, flush=True)

def _connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def ensure_schema(db_path: Path) -> None:
    dprint(f"[INFO][DB] Schema prüfen/erstellen: {db_path}")
    with _connect(db_path) as con:
        cur = con.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY,
            started_at INTEGER NOT NULL,
            params_json TEXT
        );

        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL UNIQUE,
            title TEXT,
            file_hash TEXT,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY,
            document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            page_no INTEGER NOT NULL,
            width REAL,
            height REAL,
            text_stats_json TEXT,
            layout_json TEXT,
            UNIQUE(document_id, page_no)
        );

        /* WICHTIG: KEIN UNIQUE-Constraint -> wir speichern ALLES, auch scheinbare Duplikate */
        CREATE TABLE IF NOT EXISTS footnotes (
            id INTEGER PRIMARY KEY,
            document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            page_id INTEGER REFERENCES pages(id) ON DELETE CASCADE,
            index_in_page INTEGER,
            marker TEXT,
            text TEXT NOT NULL,
            normalized_text TEXT,
            bbox_json TEXT,
            font_size_avg REAL,
            algo_score REAL,
            method TEXT,           -- 'heuristic' oder 'ocr' oder 'gpt'
            extra_json TEXT,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS citations (
            id INTEGER PRIMARY KEY,
            footnote_id INTEGER NOT NULL REFERENCES footnotes(id) ON DELETE CASCADE,
            index_in_footnote INTEGER,
            raw_text TEXT NOT NULL,
            normalized_text TEXT,
            type TEXT,           -- 'article','book','chapter','web','unknown'
            authors_json TEXT,   -- JSON-Liste von Strings
            year TEXT,
            title TEXT,
            container_title TEXT,
            volume TEXT,
            issue TEXT,
            pages TEXT,
            publisher TEXT,
            place TEXT,
            doi TEXT,
            url TEXT,
            method TEXT,         -- 'heuristic' oder 'gpt'
            extra_json TEXT,
            created_at INTEGER NOT NULL
        );
        """)
        con.commit()
    dprint("[INFO][DB] Schema OK.")

def start_run(db_path: Path, params: Dict[str, Any]) -> int:
    with _connect(db_path) as con:
        cur = con.cursor()
        cur.execute("INSERT INTO runs(started_at, params_json) VALUES (?,?)",
                    (int(time.time()), json.dumps(params, ensure_ascii=False)))
        con.commit()
        rid = cur.lastrowid
        dprint(f"[INFO][DB] Run gestartet id={rid}")
        return rid

def upsert_document(db_path: Path, path: Path, title: str, file_hash: Optional[str]) -> int:
    with _connect(db_path) as con:
        cur = con.cursor()
        cur.execute("INSERT OR IGNORE INTO documents(path, title, file_hash, created_at) VALUES (?,?,?,?)",
                    (str(path), title, file_hash or "", int(time.time())))
        cur.execute("SELECT id FROM documents WHERE path=?", (str(path),))
        doc_id = cur.fetchone()[0]
        cur.execute("UPDATE documents SET title=?, file_hash=? WHERE id=?",
                    (title, file_hash or "", doc_id))
        con.commit()
        dprint(f"[INFO][DB] Document id={doc_id} path={path}")
        return doc_id

def upsert_page(db_path: Path, document_id: int, page_no: int,
                width: float, height: float,
                text_stats: Dict[str, Any], layout: Dict[str, Any]) -> int:
    with _connect(db_path) as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO pages(document_id, page_no, width, height, text_stats_json, layout_json)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(document_id, page_no) DO UPDATE SET
                width=excluded.width,
                height=excluded.height,
                text_stats_json=excluded.text_stats_json,
                layout_json=excluded.layout_json
        """, (document_id, page_no, width, height,
              json.dumps(text_stats, ensure_ascii=False),
              json.dumps(layout, ensure_ascii=False)))
        con.commit()
        cur.execute("SELECT id FROM pages WHERE document_id=? AND page_no=?", (document_id, page_no))
        pid = cur.fetchone()[0]
        dprint(f"[DEBUG][DB] Page id={pid} doc={document_id} page_no={page_no}")
        return pid

def _normalize_text(t: str) -> str:
    import re
    t = (t or "").strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t

def insert_footnote(db_path: Path,
                    document_id: int,
                    page_id: int,
                    index_in_page: int,
                    marker: str | None,
                    text: str,
                    bbox: Dict[str, Any],
                    font_size_avg: float | None,
                    algo_score: float | None,
                    method: str,
                    extra: Dict[str, Any]) -> int:
    with _connect(db_path) as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO footnotes(
                document_id, page_id, index_in_page, marker, text, normalized_text,
                bbox_json, font_size_avg, algo_score, method, extra_json, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (document_id, page_id, index_in_page, (marker or ""), text,
              _normalize_text(text),
              json.dumps(bbox, ensure_ascii=False),
              font_size_avg if font_size_avg is not None else None,
              algo_score if algo_score is not None else None,
              method, json.dumps(extra, ensure_ascii=False), int(time.time())))
        con.commit()
        fid = cur.lastrowid
        dprint(f"[INFO][DB] Footnote inserted id={fid} page={page_id} idx={index_in_page}")
        return fid

def insert_citation(db_path: Path,
                    footnote_id: int,
                    index_in_footnote: int,
                    raw_text: str,
                    ctype: str,
                    authors: list[str],
                    year: str | None,
                    title: str | None,
                    container_title: str | None,
                    volume: str | None,
                    issue: str | None,
                    pages: str | None,
                    publisher: str | None,
                    place: str | None,
                    doi: str | None,
                    url: str | None,
                    method: str,
                    extra: Dict[str, Any]) -> int:
    with _connect(db_path) as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO citations(
                footnote_id, index_in_footnote, raw_text, normalized_text, type,
                authors_json, year, title, container_title, volume, issue, pages,
                publisher, place, doi, url, method, extra_json, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (footnote_id, index_in_footnote, raw_text, _normalize_text(raw_text), ctype,
              json.dumps(authors, ensure_ascii=False),
              year or "", title or "", container_title or "", volume or "", issue or "",
              pages or "", publisher or "", place or "", doi or "", url or "",
              method, json.dumps(extra, ensure_ascii=False), int(time.time())))
        con.commit()
        cid = cur.lastrowid
        dprint(f"[INFO][DB] Citation inserted id={cid} footnote={footnote_id} idx={index_in_footnote}")
        return cid