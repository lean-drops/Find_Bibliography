#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
footer_core_linking.py
----------------------
Erweitert NUR die Footer-DB (azk_footer.sqlite) um:
- Tabelle external_links (READ-ONLY Brücke zu db_core)
- Spalte documents.extra_json (für "core_suggest" Anzeige)

Stellt try_link_document_to_core(..) bereit:
- nutzt core_bridge.find_core_item_for_document(..)
- schreibt einen Link in external_links (source='db_core')
- ergänzt documents.extra_json.core_suggest (Titel/Autoren/DOI/…)
- schreibt NICHT in db_core.
"""

from __future__ import annotations
import json, time, sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from core_bridge import dprint, find_core_item_for_document

# ---- Footer-DB tools ---------------------------------------------------------
def _connect_footer(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def ensure_integration_schema(db_path: Path) -> None:
    con = _connect_footer(db_path)
    cur = con.cursor()
    # external_links
    cur.execute("""
    CREATE TABLE IF NOT EXISTS external_links (
      id INTEGER PRIMARY KEY,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      source TEXT NOT NULL,
      external_table TEXT NOT NULL,
      external_id TEXT NOT NULL,
      id_col TEXT,
      created_at INTEGER NOT NULL,
      UNIQUE(source, external_table, external_id)
    );
    """)
    # documents.extra_json (falls fehlt)
    cols = [r["name"] for r in cur.execute("PRAGMA table_info(documents)").fetchall()]
    if "extra_json" not in cols:
        dprint("[FOOTER] Füge Spalte documents.extra_json hinzu …")
        cur.execute("ALTER TABLE documents ADD COLUMN extra_json TEXT;")
    con.commit()
    con.close()
    dprint("[FOOTER] Integration-Schema OK.")

def _update_document_extra_json(con: sqlite3.Connection, document_id: int, patch: Dict[str, Any]) -> None:
    row = con.execute("SELECT COALESCE(extra_json,'') AS js FROM documents WHERE id=?", (document_id,)).fetchone()
    old = {}
    try:
        if row and row["js"]:
            old = json.loads(row["js"])
    except Exception:
        old = {}
    # merge patch unter "core_suggest"
    old["core_suggest"] = patch
    con.execute("UPDATE documents SET extra_json=? WHERE id=?", (json.dumps(old, ensure_ascii=False), document_id))

def try_link_document_to_core(db_path: Path,
                              pdf_path: Path,
                              document_id: int,
                              file_sha1: Optional[str],
                              title_guess: str) -> None:
    """Sucht db_core-Eintrag und verlinkt ihn READ-ONLY in Footer-DB."""
    dprint(f"[FOOTER] Versuche Core-Link: doc#{document_id} -> {pdf_path.name}")
    hit = find_core_item_for_document(pdf_path, known_sha1=file_sha1, title_guess=title_guess)
    if not hit:
        dprint("[FOOTER] Kein Core-Treffer – keine Verknüpfung.")
        return

    con = _connect_footer(db_path)
    cur = con.cursor()
    # schon verlinkt?
    row = cur.execute("""
       SELECT id FROM external_links
       WHERE source='db_core' AND external_table=? AND external_id=? LIMIT 1
    """, (hit.table, str(hit.id_val))).fetchone()
    if row:
        dprint(f"[FOOTER] Core-Link existiert bereits (link_id={row['id']}).")
        con.close()
        return

    cur.execute("""
      INSERT INTO external_links(document_id, source, external_table, external_id, id_col, created_at)
      VALUES (?,?,?,?,?,?)
    """, (document_id, "db_core", hit.table, str(hit.id_val), hit.id_col, int(time.time())))
    # Anzeigevorschlag ins documents.extra_json
    overlay = {
        "source": "db_core",
        "table": hit.table,
        "id": hit.id_val,
        "title": hit.title,
        "authors": hit.authors,
        "doi": hit.doi,
        "path": hit.path,
        "sha1": hit.sha1,
    }
    _update_document_extra_json(con, document_id, overlay)
    con.commit()
    con.close()
    dprint(f"[FOOTER] Core-Link gesetzt: {hit.table}#{hit.id_val}")

def get_core_overlay_for_document(db_path: Path, document_id: int) -> Optional[Dict[str, Any]]:
    """Liest nur das, was wir in documents.extra_json.core_suggest gespeichert haben (keine Core-DB-Abfrage)."""
    con = _connect_footer(db_path)
    row = con.execute("SELECT extra_json FROM documents WHERE id=?", (document_id,)).fetchone()
    con.close()
    if not row or not row["extra_json"]:
        return None
    try:
        data = json.loads(row["extra_json"])
        return data.get("core_suggest") or None
    except Exception:
        return None