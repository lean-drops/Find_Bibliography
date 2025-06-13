#!/usr/bin/env python3
# ─────────────────────────────────────────────────────────────────────────────
# detect_chapters.py – Kapitel- / Heading-Erkennung auf Basis *.fonts.json
# v3-debugmax · 2025-04-28 · MIT
# ─────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import concurrent.futures as cf
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# ───────────────────── Logger ------------------------------------------------
LOG = logging.getLogger("chapdet")
dbg = LOG.debug


def init_log(debug: bool) -> None:
    lvl = "DEBUG" if debug else "INFO"
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s %(levelname)-8s| %(funcName)s:%(lineno)-3d – %(message)s",
        datefmt="%H:%M:%S",
    )


# ───────────────────── Heuristik-Parameter -----------------------------------
TOP_N_FONTS: int = 10           # nur Top-Schriftarten anschauen
MIN_FONT_VOTES: int = 3         # wie oft muss ein (Font,Text) auftreten?
MAX_TITLE_LEN = 120
ALPHA_MIN, DIGIT_MAX = 0.60, 0.15

_rx_alpha = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]")


def ok_string(s: str) -> bool:
    """Heuristisch ausschließen: reine Ziffern, zu lang, zu viele Zahlen …"""
    s = s.strip()
    if not s or len(s) > MAX_TITLE_LEN:
        return False
    alpha = sum(c.isalpha() for c in s)
    digit = sum(c.isdigit() for c in s)
    cond = (alpha / len(s) >= ALPHA_MIN) and (digit / len(s) <= DIGIT_MAX)
    if LOG.isEnabledFor(logging.DEBUG):
        dbg("    »%-40.40s«  α=%3.1f%%  d=%3.1f%%  -> %s",
            s[:40], 100*alpha/len(s or '1'), 100*digit/len(s or '1'), cond)
    return cond


# ───────────────────── Analyse EINER *.fonts.json ----------------------------
def analyse_file(path: str, trace: bool = False) -> Tuple[str, Optional[Dict], Optional[str]]:
    """
    Returns  (stem, result | None, error | None)
    trace=True  → jede potentielle Heading-Zeile wird ins Debug-Log geschrieben.
    """
    try:
        stem = Path(path).stem
        dbg("---- %s ------------------------------------------------", stem)
        data = json.loads(Path(path).read_text(encoding="utf-8"))

        rank = data["font_ranking"]           #  [[font, size, style, charCount], …]
        if not rank:
            raise ValueError("font_ranking fehlt oder leer")

        body_font = tuple(rank[0][:3])        # prominentester Font des Dokuments
        body_size = body_font[1]
        dbg("Body-Font (global): %s  size=%.1f", body_font[0], body_size)

        # Sammeln
        font_votes: Dict[Tuple[Tuple, str], int] = {}
        hits: List[Dict] = []

        for p in data["pages"]:
            pg_no = p["n"]
            headings_fonts = [tuple(fk) for fk in p.get("heading", [])]
            spans = p.get("spans", [])

            dbg("  Seite %4d – headingFonts=%s  spans=%d", pg_no, headings_fonts, len(spans))

            for txt, *fk in spans:
                if not ok_string(txt):
                    continue
                font = tuple(fk)

                # ── Font-Filter ------------------------------------------------
                top10 = {tuple(r[:3]) for r in rank[:TOP_N_FONTS]}
                in_top10 = font in top10
                in_head  = font in headings_fonts
                size_ok  = font[1] >= body_size * 1.05

                if not (in_top10 or in_head or size_ok):
                    if trace:
                        dbg("      skip %-35.35s  font=%s sz=%.1f inTop10=%s head=%s",
                            txt, font[0], font[1], in_top10, in_head)
                    continue

                # gültiger Kandidat
                font_votes[(font, txt)] = font_votes.get((font, txt), 0) + 1
                hits.append({"page": pg_no, "text": txt, "font": font})
                if trace:
                    dbg("      HIT  %-35.35s  font=%s sz=%.1f", txt, font[0], font[1])

        # ── Abstimmen: genug Wiederholungen? ----------------------------------
        accepted = {k for k, v in font_votes.items() if v >= MIN_FONT_VOTES}
        chapters = [h for h in hits if (h["font"], h["text"]) in accepted]

        LOG.info("%s – %d Kapitel-Titel gefunden (aus %d Kandidaten)",
                 stem, len(chapters), len(hits))

        return stem, {
            "chapters": chapters,
            "fonts_used": rank[:TOP_N_FONTS],
            "candidates": hits if trace else None
        }, None

    except Exception as e:
        return Path(path).stem, None, str(e)


# ───────────────────── CSV-Writer -------------------------------------------
def write_csv(path: Path, ch: List[Dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["page", "title", "fontName", "size", "styleFlag"])
        for c in ch:
            w.writerow([c["page"], c["text"],
                        c["font"][0], c["font"][1], c["font"][2]])


# ───────────────────── Hilfs-Funktion: Dateiliste ---------------------------
def gather_fonts(pdf_dir: Path, fonts_dir: Path) -> List[Path]:
    """Nur jene .fonts.json zurückgeben, für die es auch ein PDF gibt."""
    out: List[Path] = []
    for pdf in sorted(pdf_dir.glob("*.pdf")):
        fp = fonts_dir / f"{pdf.stem}.fonts.json"
        if fp.exists():
            out.append(fp)
        else:
            LOG.warning("fonts.json fehlt für %-40s", pdf.name)
    return out


# ───────────────────── CLI ---------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser("Kapitel-Erkennung aus *.fonts.json")
    ap.add_argument("path", type=Path,
                    help="PDF-Ordner, fonts-Ordner oder einzelne *.fonts.json")
    ap.add_argument("--fonts", type=Path,
                    help="Ordner mit *.fonts.json (wenn nicht = PDF-Ordner)")
    ap.add_argument("-o", "--out", type=Path,
                    help="Ausgabe-Ordner (Default = --fonts)")
    ap.add_argument("-j", type=int,
                    default=max(1, (os.cpu_count() or 4) // 2),
                    help="Parallele Prozesse")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--trace", action="store_true",
                    help="extrem ausführliches Logging je Zeile")
    args = ap.parse_args()
    init_log(args.debug)

    # ───────────────── Einzeldatei ───────────────────────────────────────────
    if args.path.is_file():
        stem, res, err = analyse_file(str(args.path), trace=args.trace)
        if err:
            sys.exit(f"‼️  {err}")

        if args.out and args.out.suffix.lower() == ".csv":
            write_csv(args.out, res["chapters"])
            LOG.info("CSV geschrieben: %s", args.out)
        else:
            print(json.dumps(res, indent=2, ensure_ascii=False))
        return

    # ───────────────── Verzeichnis-Batch ─────────────────────────────────────
    if not args.path.is_dir():
        sys.exit("Pfad ist weder Datei noch Verzeichnis")

    fonts_dir = args.fonts or args.path        # default: gleiches Verzeichnis
    if not fonts_dir.is_dir():
        sys.exit(f"--fonts {fonts_dir} existiert nicht")

    if args.path == fonts_dir:
        fonts_files = sorted(fonts_dir.glob("*.fonts.json"))
    else:
        fonts_files = gather_fonts(args.path, fonts_dir)

    if not fonts_files:
        sys.exit("Keine *.fonts.json gefunden")

    out_dir = args.out or fonts_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    with cf.ProcessPoolExecutor(max_workers=args.j) as pool:
        futs = {pool.submit(analyse_file, str(f), args.trace): f for f in fonts_files}
        for fut in cf.as_completed(futs):
            stem, res, err = fut.result()
            if err:
                LOG.error("%s – ERROR: %s", stem, err)
                continue
            csv_path = out_dir / f"{stem}.chapters.csv"
            write_csv(csv_path, res["chapters"])
            LOG.info("✔ gespeichert: %-30s  %3d Kapitel-Titel",
                     csv_path.name, len(res["chapters"]))


if __name__ == "__main__":
    main()