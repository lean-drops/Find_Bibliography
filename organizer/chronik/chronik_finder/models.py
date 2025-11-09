"""
Datamodelle für Muster und Treffer.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

@dataclass(frozen=True)
class PatternEntry:
    label: str
    group: str         # "work" | "series" | "generic"
    regex: re.Pattern

@dataclass
class Hit:
    pdf_path: str
    page: int
    group: str
    label: str
    pattern: str
    context: str

