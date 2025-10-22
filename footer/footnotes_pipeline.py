#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
footnotes_pipeline.py
---------------------
Pipeline: wählt via Tkinter EIN PDF oder EINEN Ordner (rekursiv), extrahiert
pro Seite ALLE Fußnoten und ALLE Zitate innerhalb dieser Fußnoten und speichert
alles in 'azk_footer.sqlite'. KEINE DEDUPES.

- Heuristik (footnotes_detection) für Fußnoten
- Zitat-Erkennung: heuristisch (citations_parse) + optional GPT (gpt_citations)
- Laute Debug-Prints, kein Konsolen-Input im main
"""

from __future__ import annotations
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Tuple

import fitz  # PyMuPDF

from azk_footer_db import (
    DB_FILENAME, ensure_schema, start_run, upsert_document,
    upsert_page, insert_footnote, insert_citation
)
from footnotes_detection import dprint, page_footnotes
from citations_parse import extract_citations_heuristic
from gpt_citations import refine_citations_with_gpt

# GUI
try:
    import tkinter as tk
    from tkinter import filedialog, messagebox
    TK_OK = True
except Exception:
    TK_OK = False

def pick_target_via_gui() -> List[Path]:
    if not TK_OK:
        raise RuntimeError("Tkinter nicht verfügbar.")
    root = tk.Tk(); root.withdraw()
    messagebox.showinfo("AZK Footnotes – Pipeline",
        "Wähle EIN PDF oder klicke 'Abbrechen' und wähle danach EINEN Ordner (alle PDFs rekursiv).")
    p = filedialog.askopenfilename(
        title="PDF wählen (oder Abbrechen und Ordner wählen)",
        filetypes=[("PDF", "*.pdf"), ("Alle Dateien", "*.*")]
    )
    if p:
        return [Path(p)]
    folder = filedialog.askdirectory(title="Ordner wählen (alle PDFs)")
    if not folder:
        raise RuntimeError("Kein PDF/Ordner gewählt.")
    pdfs = [q for q in Path(folder).rglob("*.pdf")]
    if not pdfs:
        raise RuntimeError("Keine PDFs im Ordner gefunden.")
    return sorted(pdfs)

def file_sha1(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1<<20), b""):
            h.update(chunk)
    return h.hexdigest()

from citations_resolver import resolve_document  # ⟵ NEU

def process_pdf(db_path: Path, pdf_path: Path) -> None:
    dprint(f"[INFO] === Verarbeite: {pdf_path} ===")
    ensure_schema(db_path)
    run_id = start_run(db_path, {"pdf": str(pdf_path)})

    doc = fitz.open(pdf_path)
    file_hash = file_sha1(pdf_path)
    doc_id = upsert_document(db_path, pdf_path, pdf_path.stem, file_hash)

    for pno in range(doc.page_count):
        # ... (deine bestehende Seite→Fußnoten→Zitate-Extraktion unverändert)
        pass  # <- hier steht dein vorhandener Code; nicht wirklich 'pass' einsetzen :)

    dprint(f"[INFO] Extraktion fertig: {pdf_path.name} – starte Auflösung (Ebd./wie Anm./ders.) …")
    resolve_document(db_path, doc_id)   # ⟵ NEU: direkt im Anschluss
    dprint(f"[INFO] Vollständig: {pdf_path.name}")
def main():
    dprint("[INFO] AZK Footnotes – Pipeline gestartet.")
    db_path = Path.cwd() / DB_FILENAME
    targets = pick_target_via_gui()
    for p in targets:
        process_pdf(db_path, p)
    dprint("[INFO] Alles erledigt.")

if __name__ == "__main__":
    main()