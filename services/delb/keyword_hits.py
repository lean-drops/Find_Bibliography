#!/usr/bin/env python3
# ─────────────────────────────────────────────────────────────────────────────
# keyword_hits.py – finde Seiten mit Bibliographie-Schlagwörtern
# rev. 2025-04-27  •  v1.0
# ─────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import argparse, concurrent.futures as cf, json, logging, os, re, sys, time
from pathlib import Path
from typing import List, Dict, Tuple

import fitz                    # PyMuPDF
from tqdm import tqdm
# ──────────────── Logging ───────────────────────────────────────────────────
LOG = logging.getLogger("scanbib")
def init_log(debug: bool):
    logging.basicConfig(
        level="DEBUG" if debug else "INFO",
        format="%(asctime)s %(levelname)-7s| %(funcName)s:%(lineno)-3d – %(message)s",
        datefmt="%H:%M:%S")
dbg = LOG.debug
# ──────────────── Schlagwort-Liste (100 % aus User-Prompt) ──────────────────
_TERMS = [
# EN
"References","Reference List","List of References","Bibliography","Selected Bibliography",
"Annotated Bibliography","Works Cited","Works Consulted","Literature Cited","Source List",
"Sources","Sources Consulted","Reading List","Further Reading","Literature","Citations",
"Cited References","Citation List","Publications","List of Works","List of Sources",
"List of Citations","Bibliography and References",
# DE
"Literaturverzeichnis","Bibliographie","Bibliografie","Referenzen","Quellenverzeichnis",
"Quellen","Quellenangaben","Literatur","Literaturangaben","Quellenliste","Literaturverweise",
"Bücherverzeichnis","Verwendete Literatur","Zitierte Literatur","Quellen und Literatur",
"Quellenverweis","BIBLIOGRAPHIEN",
# FR
"Références","Liste de références","Ouvrages cités","Sources consultées",
"Références bibliographiques","Bibliographie sélective","Liste bibliographique",
"Travaux cités","Liste des sources",
# ES
"Bibliografía","Referencias","Lista de referencias","Fuentes","Fuentes consultadas",
"Obras citadas","Lista de obras citadas","Referencias bibliográficas","Bibliografía seleccionada",
"Referencias citadas","Lista de fuentes","Documentos citados","Biblioteca consultada",
# IT
"Bibliografia","Riferimenti","Riferimenti bibliografici","Elenco delle fonti","Fonti",
"Opere citate","Opere consultate","Elenco delle opere citate","Bibliografia selezionata",
"Riferimenti citati","Lista delle fonti","Fonti citate","Riferimenti utilizzati",
# PT
"Referências","Lista de referências","Fontes","Fontes consultadas","Obras consultadas",
"Referências bibliográficas","Bibliografia selecionada","Lista de fontes",
# NL
"Literatuurlijst","Bronnen","Bronnenlijst","Bronvermelding","Referenties","Lijst van literatuur",
"Geraadpleegde bronnen","Bibliografie geselecteerd",
# NO / DK / SE
"Referanser","Litteraturliste","Kildeliste","Bibliografi","Litteratur","Kilder","Referanseliste",
"Referencer","Referenser","Litteraturförteckning",
]
# ─── Statt _TERMS = [...]  folgendes einfügen ────────────────────────────
import csv
_TERMS_CSV = Path(__file__).with_name("bibliography_terms.csv")
if not _TERMS_CSV.exists():
    sys.exit("bibliography_terms.csv fehlt neben dem Skript!")

with _TERMS_CSV.open(newline="", encoding="utf-8") as fh:
    reader = csv.DictReader(fh)
    TERMS  = [row["term"].strip() for row in reader if row.get("term")]
if not TERMS:
    sys.exit("bibliography_terms.csv leer oder Spaltenname ≠ 'term'")

_TERM_RE = re.compile(r"\b(" + "|".join(map(re.escape, TERMS)) + r")\b", re.I)
# ──────────────── Seitentext lesen (ein Prozess) ────────────────────────────
def check_page(args: Tuple[str,int]) -> Tuple[int,bool,str]:
    """Liest *eine* Seite, meldet (index, Treffer?, erstes gef. Wort)"""
    pdf_path, page_idx = args
    with fitz.open(pdf_path) as doc:
        txt = doc.load_page(page_idx).get_text("text", sort=True)
    m = _TERM_RE.search(txt)
    return page_idx, bool(m), (m.group(0) if m else "")

# ──────────────── Analyse pro PDF ───────────────────────────────────────────
def analyse_pdf(pdf: Path, *, workers:int, debug:bool) -> List[int]:
    dbg("Start %s", pdf.name)
    t0 = time.perf_counter()
    with fitz.open(pdf) as doc:
        idxs = list(range(doc.page_count))
    pages_hit: List[int] = []
    with cf.ProcessPoolExecutor(max_workers=workers) as pool:
        for idx, ok, kw in pool.map(check_page,
                                    [(str(pdf), i) for i in idxs]):
            if ok:
                pages_hit.append(idx+1)          # 1-basiert
                dbg(" %s  p.%d  ->  «%s»", pdf.name, idx+1, kw)
    LOG.info("✓ %s  →  %s  (%.2fs)", pdf.name, pages_hit,
             time.perf_counter()-t0)
    return pages_hit

# ──────────────── Batch für Ordner ──────────────────────────────────────────
def batch(dir_path:Path, *, workers:int, debug:bool) -> Dict[str,List[int]]:
    out={}
    pdfs = sorted(dir_path.glob("*.pdf"))
    for p in tqdm(pdfs, desc="PDFs"):
        out[p.name] = analyse_pdf(p, workers=workers, debug=debug)
    return out

# ──────────────── CLI ───────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser("Finde Seiten mit Bibliographie-Schlagwörtern")
    ap.add_argument("path", type=Path,
                    help="PDF-Datei oder Ordner mit PDFs")
    ap.add_argument("-j","--jobs", type=int,
                    default=max(2, os.cpu_count()//2),
                    help="Worker-Prozesse (Default = halbe CPU-Kerne)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    init_log(args.debug)

    if args.path.is_file():
        pages = analyse_pdf(args.path, workers=args.jobs, debug=args.debug)
        print(json.dumps({args.path.name: pages}, indent=2, ensure_ascii=False))
        return
    if args.path.is_dir():
        res = batch(args.path, workers=args.jobs, debug=args.debug)
        print(json.dumps(res, indent=2, ensure_ascii=False))
        return
    sys.exit("Pfad ist weder Datei noch Verzeichnis")

if __name__ == "__main__":
    main()