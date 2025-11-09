"""
Umgebungs- und Abhängigkeitsmanagement.
- PyMuPDF (fitz) ist zwingend.
- pandas, pytesseract, networkx, tkinter, pdfplumber, pdfminer.six, pypdf sind optional.
"""
from __future__ import annotations

# Zwingend
try:
    import fitz  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError("PyMuPDF (fitz) ist erforderlich. Installation: pip install pymupdf") from e

# Optional
try:
    import pandas as pd  # type: ignore
    HAVE_PANDAS = True
except Exception:
    pd = None  # type: ignore
    HAVE_PANDAS = False

try:
    import pytesseract  # type: ignore
    HAVE_TESS = True
except Exception:
    pytesseract = None  # type: ignore
    HAVE_TESS = False

try:
    import networkx as nx  # type: ignore
    HAVE_NX = True
except Exception:
    nx = None  # type: ignore
    HAVE_NX = False

try:
    import tkinter as tk  # type: ignore
    from tkinter import filedialog  # type: ignore
    HAVE_TK = True
except Exception:
    tk = None  # type: ignore
    filedialog = None  # type: ignore
    HAVE_TK = False

try:
    import pdfplumber  # type: ignore
    HAVE_PDFPLUMBER = True
except Exception:
    pdfplumber = None  # type: ignore
    HAVE_PDFPLUMBER = False

try:
    from pdfminer.high_level import extract_text as pdfminer_extract_text  # type: ignore
    HAVE_PDFMINER = True
except Exception:
    pdfminer_extract_text = None  # type: ignore
    HAVE_PDFMINER = False

try:
    import pypdf  # type: ignore
    HAVE_PYPDF = True
except Exception:
    pypdf = None  # type: ignore
    HAVE_PYPDF = False

__all__ = [
    "fitz",
    "pd",
    "pytesseract",
    "nx",
    "tk",
    "filedialog",
    "pdfplumber",
    "pdfminer_extract_text",
    "pypdf",
    "HAVE_PANDAS",
    "HAVE_TESS",
    "HAVE_NX",
    "HAVE_TK",
    "HAVE_PDFPLUMBER",
    "HAVE_PDFMINER",
    "HAVE_PYPDF",
]

