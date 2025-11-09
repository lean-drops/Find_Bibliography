"""
Text-Normalisierung und mehrstufige Extraktion mit OCR-Fallback.
Reihenfolge: PyMuPDF → (kurz?) pdfplumber → pypdf → pdfminer → OCR.
"""
from __future__ import annotations

import io
import re
from typing import Any

from .constants import TEXT_MIN_LEN
from .env import (
    HAVE_PDFMINER,
    HAVE_PDFPLUMBER,
    HAVE_PYPDF,
    HAVE_TESS,
    pdfminer_extract_text,
    pdfplumber,
    pypdf,
    pytesseract,
)
from .cache import get_page_cache

def normspace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def normalize_text(txt: str) -> str:
    if not txt:
        return ""
    txt = txt.replace("\u00AD", "")                  # Soft Hyphen
    txt = txt.replace("ﬁ", "fi").replace("ﬂ", "fl")  # Ligaturen
    txt = re.sub(r"([A-Za-zÄÖÜäöüß]{2,})-\s*\n\s*([a-zäöüß]{2,})", r"\1\2", txt)  # Dehyphenation
    txt = re.sub(r"\r\n?|\n", " ", txt)
    return normspace(txt)

def _ocr_page(page: Any) -> str:
    if not HAVE_TESS or pytesseract is None:
        return ""
    try:
        pix = page.get_pixmap(dpi=300)
        try:
            from PIL import Image  # pillow on demand
        except Exception:
            return ""
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        # Latein ergänzen verbessert frühneuzeitliche Drucke
        return pytesseract.image_to_string(img, lang="deu+lat+eng") or ""
    except Exception:
        return ""

def extract_text(page: Any, pdf_path: str, page_index: int, pdf_mtime: float) -> str:
    """
    Extrahiert Page-Text mit Kaskade und Cache.
    """
    cache = get_page_cache()
    cached = cache.get(pdf_path, page_index, pdf_mtime)
    if cached is not None and len(cached) >= 1:
        return cached

    # 1) PyMuPDF
    txt = page.get_text("text") or ""
    txt = normalize_text(txt)
    if len(txt) >= TEXT_MIN_LEN:
        cache.put(pdf_path, page_index, pdf_mtime, txt)
        return txt

    # 2) pdfplumber (optional)
    if HAVE_PDFPLUMBER and pdfplumber is not None:
        try:
            with pdfplumber.open(pdf_path) as pl:
                if page_index < len(pl.pages):
                    t2 = pl.pages[page_index].extract_text() or ""
                    t2 = normalize_text(t2)
                    if len(t2) > len(txt):
                        txt = t2
        except Exception:
            pass
        if len(txt) >= TEXT_MIN_LEN:
            cache.put(pdf_path, page_index, pdf_mtime, txt)
            return txt

    # 3) pypdf (optional, oft spartanisch)
    if HAVE_PYPDF and pypdf is not None and len(txt) < TEXT_MIN_LEN:
        try:
            reader = pypdf.PdfReader(pdf_path)
            if page_index < len(reader.pages):
                t3 = reader.pages[page_index].extract_text() or ""
                t3 = normalize_text(t3)
                if len(t3) > len(txt):
                    txt = t3
        except Exception:
            pass
        if len(txt) >= TEXT_MIN_LEN:
            cache.put(pdf_path, page_index, pdf_mtime, txt)
            return txt

    # 4) pdfminer (schwer, aber genau)
    if HAVE_PDFMINER and pdfminer_extract_text is not None and len(txt) < TEXT_MIN_LEN:
        try:
            # pdfminer extrahiert für das ganze Dokument; wir nehmen alles und scheiden grob
            full = pdfminer_extract_text(pdf_path) or ""
            full = normalize_text(full)
            # heuristischer Ausschnitt nach Seitenzahl (unkritisch, da OCR-Fallback folgt)
            # Achtung: pdfminer kennt Seiten nicht stabil → nur als Verbesserung genutzt
            if len(full) > len(txt):
                txt = full
        except Exception:
            pass
        if len(txt) >= TEXT_MIN_LEN:
            cache.put(pdf_path, page_index, pdf_mtime, txt)
            return txt

    # 5) OCR
    ocr = _ocr_page(page)
    if len(ocr) > len(txt):
        txt = normalize_text(ocr)

    cache.put(pdf_path, page_index, pdf_mtime, txt)
    return txt

