#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""db/overview/uploader.py – PDF hochladen, auto-matchen, anhängen."""

from __future__ import annotations
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import re, unicodedata, shutil

try:
    from rapidfuzz import fuzz as rf_fuzz
    HAVE_RAPID = True
except Exception:
    HAVE_RAPID = False
    import difflib

try:
    import PyPDF2
    HAVE_PYPDF2 = True
except Exception:
    HAVE_PYPDF2 = False

import db_core as core

def debug(msg: str) -> None:
    print(f"[DEBUG][uploader] {msg}")

WS = re.compile(r"\s+")
QUOTES = str.maketrans({"«": '"', "»": '"', "‚": "'", "’": "'", "“": '"', "”": '"'})

def norm(s: Optional[str]) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s).translate(QUOTES)
    s = "".join(ch for ch in s if not unicodedata.combining(ch)).lower()
    s = WS.sub(" ", s).strip()
    return s

def author_last(author: Optional[str]) -> str:
    if not author: return ""
    primary = author.split(";")[0].strip()
    last = primary.split(",")[0].strip() if "," in primary else (primary.split()[-1] if primary.split() else primary)
    return norm(last)

def sim(a: str, b: str) -> float:
    if not a or not b: return 0.0
    if HAVE_RAPID: return float(rf_fuzz.token_sort_ratio(a, b))
    return difflib.SequenceMatcher(None, a, b).ratio() * 100.0

def _pdf_extract(pdf: Path, max_pages: int = 3) -> Tuple[Dict[str,str], str]:
    meta, sample = {}, ""
    if HAVE_PYPDF2:
        try:
            with open(pdf, "rb") as f:
                reader = PyPDF2.PdfReader(f, strict=False)
                info = reader.metadata or {}
                for key in ("/Title","/Author","/Subject"):
                    v = info.get(key)
                    if v: meta[key[1:].lower()] = str(v)
                for i in range(min(max_pages, len(reader.pages))):
                    try: sample += "\n" + (reader.pages[i].extract_text() or "")
                    except Exception: pass
        except Exception as e:
            debug(f"PyPDF2 Fehler: {e}")
    if not meta.get("title"):
        meta["title"] = pdf.stem.replace("_"," ").replace("-"," ")
    if not sample: sample = meta.get("title") or pdf.stem
    return {k:norm(v) for k,v in meta.items()}, norm(sample)

def match_pdf_to_work(conn, pdf_path: Path, author_hint: Optional[str]=None) -> Dict[str,Any]:
    meta, sample = _pdf_extract(pdf_path, max_pages=3)
    doc = " ".join([meta.get("title",""), meta.get("author",""), sample, norm(author_hint)])
    rows = [dict(r) for r in conn.execute("SELECT id, author, title, year, container_title FROM works;").fetchall()]
    cands: List[Tuple[int,float,str,str]] = []
    for w in rows:
        cand = " ".join([author_last(w.get("author")), norm(w.get("title")), str(w.get("year") or ""), norm(w.get("container_title"))])
        cands.append((w["id"], sim(doc, cand), w.get("title") or "", w.get("author") or ""))
    cands.sort(key=lambda x: x[1], reverse=True)
    best = cands[0] if cands else (None,0.0,"","")
    debug(f"Best Match: id={best[0]} score={best[1]:.1f} title={best[2]} author={best[3]}")
    return {"work_id": best[0], "score": best[1], "best_title": best[2], "best_author": best[3], "candidates": cands[:5]}

def attach_to_work(work_id: int, pdf_path: str | Path, overwrite: bool=False) -> str:
    pdf = Path(pdf_path); assert pdf.exists(), pdf
    conn = core.connect(core.DB_PATH)
    try:
        core.ensure_schema(conn)
        core.assign_file_stubs(conn)
        stub = conn.execute("SELECT file_stub FROM works WHERE id=?;", (work_id,)).fetchone()
        if not stub: raise ValueError(f"work_id {work_id} nicht gefunden")
        ext = pdf.suffix.lower() or ".pdf"
        target = core.LIB_DIR / f"{stub['file_stub']}{ext}"
        if target.exists() and not overwrite:
            st = target.stat()
            with conn:
                conn.execute("""
                    UPDATE works SET file_exists=1, file_ext=?, file_size=?, file_mtime=?, file_path=?, owned=1, updated_at=CURRENT_TIMESTAMP
                    WHERE id=?;""", (ext, st.st_size, st.st_mtime, target.name, work_id))
            return str(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(pdf, target)
        st = target.stat()
        with conn:
            conn.execute("""
                UPDATE works SET file_exists=1, file_ext=?, file_size=?, file_mtime=?, file_path=?, owned=1, updated_at=CURRENT_TIMESTAMP
                WHERE id=?;""", (ext, st.st_size, st.st_mtime, target.name, work_id))
        return str(target)
    finally:
        conn.close()

def upload_pdf(pdf_path: str | Path, author_hint: Optional[str]=None, overwrite: bool=False) -> Dict[str,Any]:
    pdf = Path(pdf_path); assert pdf.exists(), pdf
    conn = core.connect(core.DB_PATH)
    try:
        core.ensure_schema(conn)
        res = match_pdf_to_work(conn, pdf, author_hint=author_hint)
        if res["work_id"] is None or res["score"] < 60.0:
            res.update({"status":"no-match","target":None}); return res
        target = attach_to_work(int(res["work_id"]), pdf, overwrite=overwrite)
        res.update({"status":"ok","target":target}); return res
    finally:
        conn.close()