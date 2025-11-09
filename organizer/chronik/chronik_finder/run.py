"""
Orchestrator: verbindet Pfade, Muster, Scan, Aggregation und Ausgabe.
Genauigkeit hat Vorrang vor Geschwindigkeit. Alle Ausgaben identisch zur
Monolith-Version: chroniken_mentions.csv, chroniken_summary.csv,
chroniken_report.html, chroniken_network.gexf*, session_meta.json
(*nur bei vorhandenen Abhängigkeiten).
"""
from __future__ import annotations

import os
import traceback
from pathlib import Path
from typing import List, Optional, Tuple

from .aggregate import aggregate
from .constants import DEFAULT_SKIP_BIBLIOGRAPHY, MAX_WORKERS_DEFAULT
from .env import HAVE_PANDAS
from .models import Hit
from .output import ensure_session_dir, maybe_write_gexf, write_html, write_meta, write_outputs
from .paths import config_path, default_pdf_dir, iter_pdfs, project_root
from .patterns import compile_patterns, load_config
from .scan import scan_pdfs
from .ui import pick_pdf_dir, print_env

def _auto_workers() -> int:
    if MAX_WORKERS_DEFAULT > 0:
        return MAX_WORKERS_DEFAULT
    cpu = max(1, (os.cpu_count() or 2) - 1)
    return min(cpu, 4)

def run() -> Tuple[Optional[Path], object, object]:
    """
    Führt den gesamten Pipeline-Lauf aus.
    Rückgabe: (session_dir | None, df | None, agg | None)
    """
    print_env()

    root = project_root()
    std_pdf_dir = default_pdf_dir(root)
    cfg_file = config_path(root)

    try:
        cfg = load_config(cfg_file)
    except Exception as e:
        print(f"[ERROR] Konnte Config nicht laden: {cfg_file} ({e})")
        return None, None, None

    patterns, weights = compile_patterns(cfg)
    if not patterns:
        print("[ERROR] Keine gültigen Muster. Prüfe config/chroniken_canon.json.")
        return None, None, None

    pdf_dir = pick_pdf_dir(std_pdf_dir)
    pdfs = list(iter_pdfs(pdf_dir))
    if not pdfs:
        print("[WARN] Keine PDFs gefunden.")
        return None, None, None

    session_dir = ensure_session_dir(root, pdfs)
    print(f"[INFO] Session-Ordner: {session_dir}")

    try:
        all_hits: List[Hit] = scan_pdfs(pdfs, patterns, max_workers=_auto_workers(), skip_bib=DEFAULT_SKIP_BIBLIOGRAPHY)
    except Exception as e:
        print(f"[ERROR] Gesamtscan fehlgeschlagen: {e}")
        traceback.print_exc()
        return session_dir, None, None

    if not HAVE_PANDAS:
        print("[ERROR] pandas fehlt. Installiere mit: pip install pandas")
        return session_dir, None, None

    # weiche Duplikat-Reduktion
    dedup: List[Hit] = []
    seen = set()
    for h in all_hits:
        key = (h.pdf_path, h.page, h.group, h.label, h.context)
        if key not in seen:
            seen.add(key)
            dedup.append(h)

    df, agg = aggregate(dedup, weights)
    write_meta(session_dir, root, pdf_dir, pdfs, cfg_file, weights)
    write_outputs(session_dir, df, agg)
    maybe_write_gexf(session_dir, df)
    write_html(session_dir, df, agg)

    if agg is not None and len(agg) > 0:
        print("[INFO] Top-Labels nach Weighted:")
        for _, r in agg.sort_values("weighted_mentions", ascending=False).head(15).iterrows():
            print(f"  [{r['group']}] {r['label']}: mentions={r['mentions']} docs={r['docs']} weighted={r['weighted_mentions']}")
    else:
        print("[INFO] Keine Treffer aggregiert.")
    return session_dir, df, agg