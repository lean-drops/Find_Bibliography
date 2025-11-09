"""
paths.py – Pfade und Projektstruktur für den Chroniken-Finder.

Ziele
- Projekt-ROOT robust bestimmen (über 'config/chroniken_canon.json' oder heuristisch).
- DATA-Ordner korrekt im Projekt-ROOT verankern:
    ENV CHRONIK_DATA_DIR → <ROOT>/data  (kein parent-Fallback mehr)
- Standard-PDF-Ordner: <DATA>/azk_library
- PDF-Iteration rekursiv.

Nutzung
    from chronik_finder.paths import project_root, data_base_dir, default_pdf_dir, config_path, iter_pdfs
    python paths.py   # Debug-Ausgabe
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

__all__ = [
    "project_root",
    "data_base_dir",
    "default_pdf_dir",
    "config_path",
    "iter_pdfs",
]


def project_root() -> Path:
    """
    Bestimme das Projekt-ROOT:
      1) Nächstes Elternverzeichnis, das 'config/chroniken_canon.json' enthält.
      2) Nächstes Elternverzeichnis, das einen 'config' Ordner enthält.
      3) Fallback: zwei Ebenen nach oben ab Dateiort.
    """
    here = Path(__file__).resolve()
    for p in [here] + list(here.parents):
        if (p / "config" / "chroniken_canon.json").exists():
            return p
    for p in [here] + list(here.parents):
        if (p / "config").is_dir():
            return p
    try:
        return here.parents[2]
    except Exception:
        return here.parent


def data_base_dir(root: Path) -> Path:
    """
    Basisordner für Daten:
      - ENV CHRONIK_DATA_DIR → falls gesetzt, wird verwendet (und angelegt)
      - <ROOT>/data          → Standard (wird bei Bedarf angelegt)
    Hinweis: KEIN parent()-Fallback. Data liegt im Projekt-ROOT.
    """
    env_val = os.environ.get("CHRONIK_DATA_DIR")
    if env_val:
        p = Path(env_val).resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p
    p_local = (root / "data").resolve()
    p_local.mkdir(parents=True, exist_ok=True)
    return p_local


def default_pdf_dir(root: Path) -> Path:
    """Standardverzeichnis für die zu scannenden PDFs: <DATA>/azk_library"""
    return data_base_dir(root) / "azk_library"


def config_path(root: Path) -> Path:
    """Pfad zur Konfigurationsdatei: <ROOT>/config/chroniken_canon.json"""
    return root / "config" / "chroniken_canon.json"


def iter_pdfs(root: Path) -> Iterable[Path]:
    """Alle PDF-Dateien rekursiv unterhalb von 'root' liefern."""
    for ext in ("*.pdf", "*.PDF"):
        yield from root.rglob(ext)


# ---------------- Debug ----------------

def _debug_print() -> None:
    r = project_root()
    d = data_base_dir(r)
    pdf_dir = default_pdf_dir(r)
    cfg = config_path(r)
    print("[DEBUG] project_root  =", r)
    print("[DEBUG] data_base_dir =", d)
    print("[DEBUG] default_pdf_dir =", pdf_dir)
    print("[DEBUG] config_path   =", cfg)
    count = sum(1 for _ in iter_pdfs(pdf_dir)) if pdf_dir.exists() else 0
    print("[DEBUG] PDFs gefunden  =", count)


def main() -> None:
    _debug_print()


if __name__ == "__main__":
    main()