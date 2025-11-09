"""
Zentrale Konstanten und Standard-Parameter.
"""
from __future__ import annotations

from typing import List

TEXT_MIN_LEN: int = 120            # leicht erhöht für bessere OCR-Trigger
SNIPPET_LEN: int = 160             # etwas längere Kontexte
BIB_HEADINGS: List[str] = [
    r"^\s*literatur\s*$",
    r"^\s*bibliographi[ea]\s*$",
    r"^\s*literaturverzeichnis\s*$",
    r"^\s*references\s*$",
    r"^\s*bibliography\s*$",
    r"^\s*quellen\s*(und\s*literatur)?\s*$",
]

# Steuerung (keine CLI erforderlich)
DEFAULT_SKIP_BIBLIOGRAPHY: bool = True
MAX_WORKERS_DEFAULT: int = 0  # 0 = auto (min(CPU-1, 4))
CACHE_REL_DIR: str = "data/cache/pages"

