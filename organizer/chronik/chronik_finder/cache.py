"""
Einfacher Seiten-Cache für extrahierte Texte und OCR-Ergebnisse.
Schlüssel: SHA1(pdf_path | mtime | page_index). Atomare Writes.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Optional

from .constants import CACHE_REL_DIR
from .paths import project_root

class PageCache:
    def __init__(self) -> None:
        root = project_root()
        self.base = (root / CACHE_REL_DIR).resolve()
        self.base.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _hash_key(pdf_path: str, page_index: int, mtime: float) -> str:
        h = hashlib.sha1()
        h.update(str(pdf_path).encode("utf-8"))
        h.update(b"|")
        h.update(str(int(mtime)).encode("ascii"))
        h.update(b"|")
        h.update(str(page_index).encode("ascii"))
        return h.hexdigest()

    def path_for(self, pdf_path: str, page_index: int, mtime: float) -> Path:
        return self.base / f"{self._hash_key(pdf_path, page_index, mtime)}.txt"

    def get(self, pdf_path: str, page_index: int, mtime: float) -> Optional[str]:
        p = self.path_for(pdf_path, page_index, mtime)
        if not p.exists():
            return None
        try:
            return p.read_text(encoding="utf-8")
        except Exception:
            return None

    def put(self, pdf_path: str, page_index: int, mtime: float, text: str) -> None:
        p = self.path_for(pdf_path, page_index, mtime)
        tmp = p.with_suffix(".tmp")
        try:
            tmp.write_text(text, encoding="utf-8")
            os.replace(tmp, p)
        except Exception:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass

# Singleton-Zugriff
_CACHE: Optional[PageCache] = None

def get_page_cache() -> PageCache:
    global _CACHE
    if _CACHE is None:
        _CACHE = PageCache()
    return _CACHE

