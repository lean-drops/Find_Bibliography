"""
Kernsuche und Verarbeitung für Autor↔Autor-Referenznetz.

Usage:
  Von authors_gui.py gestartet. Nicht direkt aufrufen.

Funktionen:
  run_on_folder(base: str) -> str  # führt Analyse aus und liefert den Session-Ordnerpfad
"""
from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Tuple, Iterable
from dataclasses import asdict

from authors_utils import (
    Author, Mention, iter_pdfs, extract_authors_from_filenames,
    detect_bibliography_pages, extract_text_with_ocr, classify_section,
    strip_diacritics, compile_patterns, quick_page_filter, find_author_mentions, make_context,
    aggregate_edges, ensure_session_dir, write_csvs, write_gexf, write_html_report, write_session_meta,
    log_info, log_warn, log_error, environment_report
)

# ---------------------- Worker ----------------------
def _process_pdf(pdf_path: str, src_id: str, authors_plain: Dict[str, dict]) -> List[Mention]:
    # authors_plain: tgt_id -> dict(serialized Author)
    mentions: List[Mention] = []
    try:
        import fitz  # local import in worker
        doc = fitz.open(pdf_path)
    except Exception as e:
        log_warn(f"Konnte PDF nicht öffnen: {pdf_path} ({e})")
        return mentions

    try:
        bib_pages = detect_bibliography_pages(doc)
        # Cache: kompiliere Patterns pro Zielautor genau einmal pro PDF
        compiled_cache: Dict[str, List] = {}
        for pidx in range(doc.page_count):
            page = doc.load_page(pidx)
            txt = extract_text_with_ocr(page)
            if not txt:
                continue
            section_bib = classify_section(pidx, bib_pages) == "bib"
            txt_folded = strip_diacritics(txt).casefold()

            for tgt_id, a_dict in authors_plain.items():
                folded_surnames = tuple(a_dict["folded_surnames"])
                if not quick_page_filter(txt_folded, folded_surnames):
                    continue
                if tgt_id not in compiled_cache:
                    compiled_cache[tgt_id] = compile_patterns(a_dict["pattern_strs"])
                raw_hits = find_author_mentions(txt, compiled_cache[tgt_id])
                if not raw_hits:
                    continue
                seen = set()
                for span, pat in raw_hits:
                    if span in seen:
                        continue
                    seen.add(span)
                    ctx = make_context(txt, span)
                    mentions.append(Mention(
                        src_id=src_id,
                        tgt_id=tgt_id,
                        pdf_path=pdf_path,
                        page=pidx + 1,
                        in_bib=section_bib,
                        pattern=pat,
                        context=ctx
                    ))
    finally:
        try:
            doc.close()
        except Exception:
            pass
    return mentions

# ---------------------- Orchestrierung ----------------------
def _serialize_authors(authors: Dict[str, Author]) -> Dict[str, dict]:
    # Nur JSON-fähige Felder mitgeben
    out: Dict[str, dict] = {}
    for aid, a in authors.items():
        d = asdict(a)
        out[aid] = {
            "author_id": d["author_id"],
            "display": d["display"],
            "surnames": list(d["surnames"]),
            "pattern_strs": list(d["pattern_strs"]),
            "folded_surnames": list(d["folded_surnames"]),
        }
    return out

def run_on_folder(base: str) -> str:
    environment_report()
    pdfs = list(iter_pdfs(base))
    if not pdfs:
        raise RuntimeError("Keine PDFs gefunden.")

    log_info(f"PDFs gefunden: {len(pdfs)}")
    authors = extract_authors_from_filenames(pdfs)
    log_info(f"Autoren erkannt: {len(authors)}")
    for aid, a in authors.items():
        print(f"   - {aid} → {a.display} | Nachnamen: {', '.join(a.surnames)}")

    # Quelle pro Datei
    file2author: Dict[str, str] = {}
    for p in pdfs:
        left = os.path.splitext(os.path.basename(p))[0].split("__")[0].replace("_", "-").strip("-")
        file2author[p] = left

    # Aufgaben vorbereiten
    tasks = []
    for pdf in pdfs:
        src_id = file2author.get(pdf, "")
        if not src_id or src_id not in authors:
            log_warn(f"Kein gültiger Quell-Autor für {pdf} erkannt, überspringe.")
            continue
        targets = {aid: a for aid, a in authors.items() if aid != src_id}
        if not targets:
            continue
        tasks.append((pdf, src_id, _serialize_authors(targets)))

    mentions_all: List[Mention] = []
    if not tasks:
        log_warn("Keine verarbeitbaren PDFs. Abbruch.")
    else:
        cpu = max(1, os.cpu_count() or 1)
        log_info(f"Starte Parallelverarbeitung mit {cpu} Prozessen.")
        with ProcessPoolExecutor(max_workers=cpu) as ex:
            futs = {ex.submit(_process_pdf, pdf, src, tgts): (pdf, src) for (pdf, src, tgts) in tasks for tgts in [tgts]}
            done = 0
            total = len(futs)
            for fut in as_completed(futs):
                done += 1
                pdf, src = futs[fut]
                try:
                    res = fut.result()
                    mentions_all.extend(res)
                except Exception as e:
                    log_warn(f"Fehler bei {pdf}: {e}")
                if done % 1 == 0:
                    log_info(f"Fortschritt: {done}/{total}")

    # Ausgabe
    session_dir = ensure_session_dir(base, pdfs)
    log_info(f"Session-Ordner: {session_dir}")

    from authors_utils import HAVE_PANDAS  # check flag
    if not HAVE_PANDAS:
        raise RuntimeError("pandas fehlt. Installiere mit: pip install pandas")

    df_nodes, df_edges = aggregate_edges(authors, mentions_all)
    write_session_meta(session_dir, base, pdfs)
    write_csvs(session_dir, df_nodes, df_edges)
    write_gexf(session_dir, authors, df_edges)
    write_html_report(session_dir, authors, df_edges)

    # Kurzsummary
    if not df_edges.empty:
        log_info("Top-Kanten:")
        for _, r in df_edges.sort_values("weighted", ascending=False).head(10).iterrows():
            print(f"  {authors[r['src']].display} → {authors[r['tgt']].display} | "
                  f"total={r['total']} bib={r['bib_hits']} text={r['text_hits']}")
    else:
        log_warn("Keine Kanten gefunden.")
    log_info("Fertig.")
    return session_dir

