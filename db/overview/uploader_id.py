# FILE: db/overview/uploader_id.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
uploader_id.py
----------------
Einfacher ID-basierter Upload:
- Du wählst ein PDF und gibst die Ziel-Work-ID an
- Die Datei wird unter 'author-slug__ID.ext' nach data/azk_library kopiert
- In der DB (works) werden file_* und owned=1 gesetzt

Public API:
    upload_pdf_by_id(pdf_path: str|Path, work_id: int, overwrite: bool=False) -> str (Zielpfad)

Diese Datei ist ein dünner Wrapper um die bereits vorhandene Logik in uploader.attach_to_work().
"""

from __future__ import annotations
from pathlib import Path
from typing import Optional

from .uploader import attach_to_work

def upload_pdf_by_id(pdf_path: str | Path, work_id: int, overwrite: bool = False) -> str:
    """
    Kopiert die Datei zum Werk (work_id) und aktualisiert DB-Felder.
    Gibt den Zielpfad (str) zurück.
    """
    pdf = Path(pdf_path)
    if not pdf.exists() or not pdf.is_file():
        raise FileNotFoundError(f"PDF nicht gefunden: {pdf}")
    if not isinstance(work_id, int) or work_id <= 0:
        raise ValueError(f"Ungültige work_id: {work_id!r}")

    target = attach_to_work(int(work_id), pdf, overwrite=overwrite)
    print(f"[DEBUG][uploader_id] OK -> ID {work_id} :: {target}")
    return target