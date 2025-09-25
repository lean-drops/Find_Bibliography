#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py — PDF wählen → Bibliographie finden → Referenzen extrahieren →
RE-PARSEN (Rohzeile!) → NORMALISIEREN → sauber ausgeben + Stil/Typ-Stats.

Regeln beachtet:
- Vollständiges Skript, keine Platzhalter.
- Tkinter-Dateidialog (keine Konsoleneingaben im main-Block).
- Laute Debug-Prints in jedem Schritt.
"""

from __future__ import annotations
import os, sys, time, argparse
from importlib import import_module
from pathlib import Path
from typing import Optional, Sequence, List, Dict
try:
    import tkinter as tk
    from tkinter import filedialog, messagebox
except Exception as e:
    print(f"[FATAL] Tkinter Import fehlgeschlagen: {e}", file=sys.stderr); sys.exit(2)

from ref_normalizer import normalize_records, dedupe_records, NormalizedRef

# ——— Dynamic import helpers —————————————————————————————
def _import_detect_bibliography():
    for mod, attr in [("services.delb.find_bibliography","detect_bibliography"),
                      ("find_bibliography","detect_bibliography")]:
        try:
            print(f"[import] {mod}.{attr} …")
            fn=getattr(import_module(mod), attr)
            print(f"[import] OK: {mod}.{attr}")
            return fn
        except Exception as exc:
            print(f"[import] fail {mod}: {exc}")
    raise ImportError("detect_bibliography nicht gefunden.")

def _import_extract_references():
    for mod, attr in [("services.ref_extractor","extract_references"),
                      ("ref_extractor","extract_references")]:
        try:
            print(f"[import] {mod}.{attr} …")
            fn=getattr(import_module(mod), attr)
            print(f"[import] OK: {mod}.{attr}")
            return fn
        except Exception as exc:
            print(f"[import] fail {mod}: {exc}")
    raise ImportError("extract_references nicht gefunden.")

# ——— UI ————————————————————————————————————————————————
def _select_pdf() -> Optional[Path]:
    print("[ui] Datei-Dialog …")
    root=tk.Tk(); root.withdraw(); root.update_idletasks()
    try:
        f=filedialog.askopenfilename(
            title="PDF auswählen",
            initialdir=os.getcwd(),
            filetypes=[("PDF","*.pdf"),("Alle Dateien","*.*")]
        )
        if not f:
            print("[ui] Abbruch (keine Datei)."); return None
        p=Path(f)
        if not p.exists() or p.suffix.lower()!=".pdf":
            messagebox.showerror("Fehler","Bitte eine existierende PDF-Datei wählen."); return None
        print(f"[ui] gewählt: {p}")
        return p
    finally:
        root.destroy()

# ——— Pretty print ——————————————————————————————————————
def _people_fmt(ps: List[Dict[str,str]]) -> str:
    return "; ".join(f"{p.get('family','')}, {p.get('given','')}".strip().strip(", ") for p in ps)

def _line_fmt(i: int, r: NormalizedRef) -> str:
    who = _people_fmt(r.authors) or _people_fmt(r.editors) or "—"
    mid = f"“{r.title}”"
    right=[]
    if r.container_title and r.entry_type in ("journal-article","chapter"):
        right.append(r.container_title)
    if r.volume:
        right.append(f"Vol. {r.volume}" + (f"({r.issue})" if r.issue else ""))
    if r.pages:
        right.append(f"S./pp. {r.pages}")
    imprint=None
    if r.publisher_place and r.publisher:
        imprint=f"{r.publisher_place}: {r.publisher}"
    elif r.publisher or r.publisher_place:
        imprint=r.publisher or r.publisher_place
    if imprint: right.append(imprint)
    if r.year: right.append(str(r.year))
    tail=" — ".join(right) if right else "—"
    extras=[]
    if r.doi: extras.append(f"DOI:{r.doi}")
    if r.isbn: extras.append(f"ISBN:{r.isbn}")
    if r.url: extras.append(f"URL:{r.url}")
    extra_s=("  ["+", ".join(extras)+"]") if extras else ""
    return f"{i:4d} │ {who} — {mid} — {tail}{extra_s}"

# ——— CLI ————————————————————————————————————————————————
def parse_args(argv: Optional[Sequence[str]]=None):
    ap=argparse.ArgumentParser(description="Erkennen, Extrahieren, Re-Parsing & Normalisieren von Bibliographieeinträgen.")
    ap.add_argument("pdf", nargs="?", help="Optionaler Pfad zur PDF (überspringt Dialog)")
    ap.add_argument("--gpt", action="store_true", help="Extractor mit GPT-Fallback (falls konfiguriert)")
    ap.add_argument("--ref-debug", action="store_true", help="Detail-Parser-Logs im Extractor (ENV REF_DEBUG=1)")
    ap.add_argument("--dedupe", action="store_true", help="Deduplizierung aktivieren")
    ap.add_argument("--threshold", type=int, default=92, help="Fuzzy-Threshold für Dedupe")
    return ap.parse_args(argv)

def main(argv: Optional[Sequence[str]]=None) -> None:
    args=parse_args(argv)
    if args.ref_debug:
        os.environ["REF_DEBUG"]="1"; print("[env] REF_DEBUG=1")
    if args.gpt:
        os.environ["REF_GPT"]="1"; print("[env] REF_GPT=1")

    detect_bibliography=_import_detect_bibliography()
    extract_references=_import_extract_references()

    # PDF besorgen
    if args.pdf:
        pdf=Path(args.pdf).expanduser().resolve()
        print(f"[cli] PDF: {pdf}")
        if not pdf.exists(): print("[cli] Datei existiert nicht."); sys.exit(2)
    else:
        pdf=_select_pdf()
        if not pdf: sys.exit(1)

    t0=time.perf_counter()
    print("[run] Erkenne Bibliographie …")
    bounds=detect_bibliography(pdf)
    if not bounds:
        print("[run] Keine Bibliographie erkannt."); sys.exit(0)
    first,last=bounds
    pages0=list(range(first-1, last))
    print(f"[run] Bibliographie: {first}–{last} (1-basiert) → {pages0}")

    print(f"[run] Extrahiere Referenzen aus {len(pages0)} Seite(n) …")
    refs_raw: List[Dict]=extract_references(str(pdf), pages0, use_gpt=args.gpt)
    print(f"[run] Extraktion: {len(refs_raw)} Einträge")

    # >>> WICHTIG: Hier erwarten wir rec["raw"] oder rec["line"]; sonst bauen wir Fallback in normalize_record <<<
    print("[run] Normalisieren (mit Re-Parsing der Rohzeile) …")
    refs_norm = normalize_records(refs_raw)

    if args.dedupe:
        print("[run] Dedupliziere …")
        refs_norm = dedupe_records(refs_norm, threshold=args.threshold)

    # Ausgabe
    print("────────────────────────────────────")
    print(f"  Referenzierte Werke (normalisiert; {len(refs_norm)} Treffer)")
    print("────────────────────────────────────")
    for i,r in enumerate(refs_norm,1):
        print(_line_fmt(i,r))

    # Stats
    from collections import Counter
    styles = Counter(r.style_family for r in refs_norm)
    types  = Counter(r.entry_type for r in refs_norm)
    print("───────────────────")
    print("  Zusammenfassung")
    print("───────────────────")
    print(f"Datei:            {pdf.name}")
    print(f"Bibliographie:    Seiten {first}–{last} (1-basiert)")
    print(f"Gefundene Werke:  {len(refs_norm)}{' (deduped)' if args.dedupe else ''}")
    print("Stile:            " + ", ".join(f"{k}={v}" for k,v in styles.items()))
    print("Typen:            " + ", ".join(f"{k}={v}" for k,v in types.items()))
    print(f"Gesamtdauer:      {time.perf_counter()-t0:.2f}s")

if __name__=="__main__":
    main()
