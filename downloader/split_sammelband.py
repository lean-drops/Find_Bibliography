#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FILE: db/overview/split_sammelband.py

Zweck
- Schneidet den Sammelband "Ein «Bruderkrieg» macht Geschichte" in Kapitel-PDFs.
- Speichert die Kapitel als 'author-slug__ID.pdf' in data/azk_library/.
- Aktualisiert dazugehörige DB-Einträge (file_path, file_size, file_mtime, file_exists=1, owned=1).

WICHTIG
- Dieses Skript ist exakt auf die hochgeladene PDF abgestimmt (150 PDF-Seiten).
- Die Kapitel-Startseiten sind als PDF-Seiten (1-basiert) angegeben.
  Grundlage: Inhaltsverzeichnis (gedr. S. 5–6) und sichtbare Kapitelanfänge im vorliegenden PDF.  [oai_citation:3‡Mitteilungen der Antiquarischen Gesellschaft in Zürich.pdf](sediment://file_00000000297061f4b4e548dd64bcf9d5)
- Der Beitrag Niederhäuser/Winterthur (ID 4) beginnt auf PDF-Seite 148; die Datei endet bei 150 -> Kapitel wahrscheinlich unvollständig (ok, wir exportieren bis Datei-Ende).

Voraussetzungen
- pypdf (oder PyPDF2 >= 3.x)

Ausführen
    python -m db.overview.split_sammelband

Debug-Ausgaben erklären jeden Schritt.
"""

from pathlib import Path
from typing import Dict, Tuple
from pypdf import PdfReader, PdfWriter  # pypdf ist der Nachfolger von PyPDF2
import sqlite3

# ---- Projektpfade ----
import db_core as core

# Pfad zum Sammelband-PDF (falls dein Pfad abweicht, hier anpassen)
PDF_PATH = Path("/Users/python/Library/Mobile Documents/com~apple~CloudDocs/Verwaltung Leandro Habegger/Universität/Aktuelles Semster/HS2025/Bachelor Arbeit/Literatur/Mitteilungen der Antiquarischen Gesellschaft in Zürich.pdf")

# Mapping: work_id -> (start_pdf_page, end_pdf_page)  (1-basierte PDF-Seiten, inkl.)
# Abgeleitet aus dem vorliegenden PDF (150 Seiten) – nachprüfbar an sichtbaren Kapitelstarts.  [oai_citation:4‡Mitteilungen der Antiquarischen Gesellschaft in Zürich.pdf](sediment://file_00000000297061f4b4e548dd64bcf9d5)
#   24 : Stettler Beginn (gedr. S.23)
#   44 : Jucker
#   56 : Landolt
#   66 : Sieber (Opfer)
#   79 : Sieber Exkurs
#   90 : Frey (Rudolf Stüssi)
#   99 : Bosshard
#  120 : Rigendinger
#  134 : Sutter
#  148 : Niederhäuser (Winterthur)  -> bis Dateiende (150)
SPLITS: Dict[int, Tuple[int, int]] = {
    8:  (24, 43),    # Stettler 23–42 (gedruckt) -> PDF 24–43
    9:  (44, 55),    # Jucker   43–54           -> PDF 44–55
    10: (56, 65),    # Landolt  55–64           -> PDF 56–65
    11: (66, 78),    # Sieber   65–78           -> PDF 66–78
    12: (79, 88),    # Exkurs   79–88           -> PDF 79–88
    13: (90, 98),    # Frey     89–98           -> PDF 90–98
    14: (99, 119),   # Bosshard 99–110/…        -> PDF 99–119
    2:  (120, 133),  # Rigendinger 111–124      -> PDF 120–133
    3:  (134, 147),  # Sutter   125–138         -> PDF 134–147
    4:  (148, 150),  # Niederhäuser/Winterthur  139–… -> PDF 148–150 (nur Teil im File)
}

def _write_part(reader: PdfReader, start: int, end: int, out_path: Path) -> None:
    """
    Schneidet Seitenbereich [start, end] (1-basiert, inkl.) in eine neue PDF.
    """
    assert start >= 1 and end >= start, f"Ungültiger Bereich: {start}-{end}"
    n = len(reader.pages)
    if end > n:
        end = n
    writer = PdfWriter()
    for i in range(start - 1, end):
        writer.add_page(reader.pages[i])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("wb") as f:
        writer.write(f)

def _update_db_for_work(conn: sqlite3.Connection, work_id: int, file_name: str, full_path: Path) -> None:
    """
    Setzt file_* Felder, owned=1, file_exists=1 im Datensatz works.id=work_id.
    """
    st = full_path.stat()
    with conn:
        conn.execute("""
            UPDATE works
               SET file_exists=1,
                   file_ext=?,
                   file_size=?,
                   file_mtime=?,
                   file_path=?,
                   owned=1,
                   updated_at=CURRENT_TIMESTAMP
             WHERE id=?;
        """, (full_path.suffix.lower(), st.st_size, st.st_mtime, file_name, work_id))

def _target_name_for_work(conn: sqlite3.Connection, work_id: int, default_ext: str = ".pdf") -> str:
    """
    Erzeugt 'author-slug__ID.ext' anhand von file_stub; wenn leer, neu vergeben.
    """
    row = conn.execute("SELECT file_stub FROM works WHERE id=?;", (work_id,)).fetchone()
    if not row:
        raise ValueError(f"work_id {work_id} nicht gefunden")
    stub = row["file_stub"]
    if not stub:
        core.assign_file_stubs(conn)
        stub = conn.execute("SELECT file_stub FROM works WHERE id=?;", (work_id,)).fetchone()["file_stub"]
    # nutze vorhandene Endung, sonst .pdf
    return f"{stub}{default_ext}"

def split_sammelband() -> None:
    print("=== Split Sammelband -> Kapitel-PDFs ===")
    if not PDF_PATH.exists():
        raise FileNotFoundError(f"PDF nicht gefunden: {PDF_PATH}")

    # DB verbinden
    core.ensure_dirs()
    conn = core.connect(core.DB_PATH)
    try:
        core.ensure_schema(conn)
        reader = PdfReader(str(PDF_PATH))
        total = len(reader.pages)
        print(f"[DEBUG] Sammelband geladen: {PDF_PATH.name} (PDF-Seiten: {total})")

        # jeden Eintrag schneiden
        for work_id, (start, end) in SPLITS.items():
            if start > total:
                print(f"[WARN] work_id={work_id}: Start {start} > PDF-Ende {total} -> übersprungen")
                continue
            end = min(end, total)
            fname = _target_name_for_work(conn, work_id, ".pdf")
            fpath = core.LIB_DIR / fname
            print(f"[DEBUG] work_id={work_id} -> {fname} (Seiten {start}-{end})")
            _write_part(reader, start, end, fpath)
            _update_db_for_work(conn, work_id, fname, fpath)

        core.checkpoint_and_vacuum(conn)
        print("=== Split fertig. Dateien liegen in:", core.LIB_DIR.resolve(), "===")
    finally:
        conn.close()

if __name__ == "__main__":
    split_sammelband()