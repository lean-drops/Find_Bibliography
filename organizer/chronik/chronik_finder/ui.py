"""
Einfache UI-Hilfen: Verzeichniswahl, Umgebungsreport.
"""
from __future__ import annotations

from pathlib import Path

from .env import HAVE_TK, filedialog, tk
from .constants import TEXT_MIN_LEN

def pick_pdf_dir(std: Path) -> Path:
    if std.exists():
        print(f"[INFO] PDF-Ordner: {std}")
        return std
    if HAVE_TK and tk and filedialog:
        root = tk.Tk()  # type: ignore[call-arg]
        root.withdraw()
        root.update()
        folder = filedialog.askdirectory(title="PDF-Ordner wählen")
        root.destroy()
        if folder:
            p = Path(folder).resolve()
            print(f"[INFO] PDF-Ordner (GUI): {p}")
            return p
    print(f"[WARN] Standardordner nicht gefunden. Fallback: aktuelles Verzeichnis.")
    return Path.cwd()

def print_env() -> None:
    print("[INFO] Bibliotheken:")
    print("  PyMuPDF: OK")
    try:
        import pandas  # noqa: F401
        print("  pandas: OK")
    except Exception:
        print("  pandas: NEIN")
    try:
        import pytesseract  # noqa: F401
        print("  pytesseract: OK")
    except Exception:
        print("  pytesseract: NEIN")
    try:
        import networkx  # noqa: F401
        print("  networkx: OK")
    except Exception:
        print("  networkx: NEIN")
    try:
        import pdfplumber  # noqa: F401
        print("  pdfplumber: OK")
    except Exception:
        print("  pdfplumber: NEIN")
    try:
        from pdfminer.high_level import extract_text as _tmp  # noqa: F401
        print("  pdfminer.six: OK")
    except Exception:
        print("  pdfminer.six: NEIN")
    try:
        import pypdf  # noqa: F401
        print("  pypdf: OK")
    except Exception:
        print("  pypdf: NEIN")
    print(f"[INFO] OCR-Fallback aktiv bei < {TEXT_MIN_LEN} Zeichen pro Seite.")

