"""
Konfig laden und robuste Regex-Kompilierung.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

from .constants import BIB_HEADINGS
from .models import PatternEntry

def load_config(cfg_path: Path) -> Dict:
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)

def _flexify_spaces(alias_pat: str) -> str:
    """
    Erweitert Leerzeichen in Aliassen zu einer flexiblen Klasse: [\s NBSP -]+
    Nutzt Replacement-Funktion, damit keine Backrefs fehlschlagen.
    """
    nbsp = "\u00A0"  # echtes NBSP
    def repl(_m: re.Match) -> str:
        return f"[\\s{nbsp}\\-]+"
    return re.sub(r"\s+", repl, alias_pat)

def compile_patterns(cfg: Dict) -> Tuple[List[PatternEntry], Dict[str, float]]:
    compiled: List[PatternEntry] = []
    weights = cfg.get("weights", {"work": 1.0, "series": 1.0, "generic": 1.0})

    def add_block(items: List[Dict], group: str) -> None:
        for it in items or []:
            label = it.get("canonical") or it.get("label") or "generic"
            raw_list = it.get("aliases") or it.get("patterns") or []
            for raw in raw_list:
                try:
                    pat = _flexify_spaces(raw)
                    rgx = re.compile(pat, re.IGNORECASE)
                    compiled.append(PatternEntry(label=label, group=group, regex=rgx))
                except re.error as e:
                    print(f"[WARN] Ungültiges Regex ignoriert ({group}:{label}): {raw} ({e})")

    add_block(cfg.get("works", []), "work")
    add_block(cfg.get("series", []), "series")
    add_block(cfg.get("generic_terms", []), "generic")

    print(f"[INFO] Muster geladen: works={len(cfg.get('works', []))}, series={len(cfg.get('series', []))}, generics={len(cfg.get('generic_terms', []))}")
    print(f"[INFO] Kompilierte Regex: {len(compiled)} | Gewichte: {weights}")
    return compiled, weights

def compile_bib_heading_patterns() -> List[re.Pattern]:
    return [re.compile(p, re.IGNORECASE | re.MULTILINE) for p in BIB_HEADINGS]

def detect_bibliography_pages(doc) -> Set[int]:
    from .text import normalize_text
    pats = compile_bib_heading_patterns()
    bib_pages: Set[int] = set()
    start_seen = False
    for i in range(doc.page_count):
        raw = (doc.load_page(i).get_text("text") or "")
        txt = normalize_text(raw)
        if not start_seen and any(p.search(txt) for p in pats):
            start_seen = True
        if start_seen:
            bib_pages.add(i)
    return bib_pages

