"""
chronik_finder – Paketierte Version des Chroniken-Suchers.
Exportiert die Funktion run() als stabilen Einstiegspunkt.
"""
from __future__ import annotations

from .run import run
from .models import Hit, PatternEntry  # Re-Export nützlicher Typen

__all__ = ["run", "Hit", "PatternEntry"]

