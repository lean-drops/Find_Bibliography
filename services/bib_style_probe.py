#!/usr/bin/env python3
"""find_bibliography – nahezu unfehlbare Erkennung + strukturierte Feld‑Extraktion
=================================================================================
Erweitert um **Publisher‑Erkennung** (Ort : Verlag) sowie year/locator‑Parsing.
Alle bisherigen Features (PageLabels, Scoring, GPT‑Refinement …) bleiben erhalten.
"""
from __future__ import annotations

import argparse
# ... (Imports & init unverändert) -------------------------------------------------

# Neues Regex‑Triple ----------------------------------------------------------------
import re, os, ssl, math, json, csv, asyncio, logging, fitz
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from statistics import median, mean
from dataclasses import dataclass
from typing import Final, Optional, Sequence, List, Tuple
from dotenv import load_dotenv
from tqdm import tqdm

# ────────────────────────── Logging (wie gehabt) ───────────────────────────────────
LOG = logging.getLogger("find_bib")
LOG.addHandler(logging.StreamHandler())
LOG.setLevel(logging.INFO)

def _d(msg: str,*a): LOG.debug(msg,*a)

# ────────────────────────── OpenAI Init (gekürzt) ─────────────────────────────────-
load_dotenv();
try: import openai  # type: ignore
except ModuleNotFoundError: openai=None  # type: ignore
if openai and (key:=os.getenv("OPENAI_API_KEY")): openai.api_key=key

GPT_MODEL:Final="gpt-4o-mini";  GPT_SYS:Final="You see a PDF page. Reply BIB or NO.";  GPT_TOK:Final=1

# ────────────────────────── Regex‑Pools ────────────────────────────────────────────
BIB_TERMS:Final=set("bibliography references reference list works cited literatur literaturverzeichnis quellen".split())
_RE_HDR      = re.compile(r"|".join(re.escape(t) for t in BIB_TERMS),re.I)
_RE_DOI      = re.compile(r"10\.[0-9]{4,9}/[-._;()/:A-Za-z0-9]+",re.I)
_RE_YEAR     = re.compile(r"\b(1[5-9][0-9]{2}|20[0-4][0-9])\b")
_RE_NUM      = re.compile(r"^\s*\[?\d{1,3}\]?[:.) ]")
_RE_AUTHOR   = re.compile(r"^[A-ZÄÖÜ][\w’'\-]+,\s+[A-Z](?:[A-Z]|[a-z]+)?\.")
# **NEU**  Ort:Verlag  (z. B. "Oxford: Oxford University Press")
_RE_PUBLISH  = re.compile(r"(?P<place>[A-Z][^:]+?):\s*(?P<publisher>[A-Z][^,.]+)")

MIN_CITE_RATIO:Final=0.25; CAPS_HDR_MIN:Final=0.45; MIN_BLOCK_LEN:Final=2

@dataclass(slots=True)
class PageInfo:
    idx:int; label:str; text:str; score:float=0.0

# ────────────────────────── Feature‑Checker ─────────────────────────────────────---

def _is_cite_line(line:str)->bool:
    return bool(_RE_DOI.search(line) or _RE_NUM.match(line) or (_RE_AUTHOR.match(line) and _RE_YEAR.search(line)) or _RE_YEAR.search(line))

# ---------- NEU: Feld‑Extraktion für Statistiken / Debug ---------------------------

def extract_fields(line:str)->tuple[str|None,str|None,str|None,str|None]:
    """liefert (author_year, locator, place, publisher) – Felder können None sein"""
    year = _RE_YEAR.search(line)
    loc  = re.search(r",\s*(\d+[\d–\-]*)\.?$", line)
    pub  = _RE_PUBLISH.search(line)
    return (
        year.group(0) if year else None,
        loc.group(1) if loc else None,
        pub.group("place") if pub else None,
        pub.group("publisher") if pub else None,
    )

# ────────────────────────── Page‑Scoring & GPT‑Refinement --------------------------

def _score_page(txt:str)->float:
    if not txt.strip(): return 0.0
    lines=[l.strip() for l in txt.splitlines() if l.strip()]
    hdr=" ".join(lines[:6]); tokens=re.findall(r"\w+",hdr)
    caps=sum(t.isupper() or t.istitle() for t in tokens)/(len(tokens) or 1)
    hdr_hit=_RE_HDR.search(hdr) and caps>=CAPS_HDR_MIN
    cite=sum(_is_cite_line(l) for l in lines)/(len(lines) or 1)
    punct=(hdr.count(";")+hdr.count("."))/(len(hdr) or 1)
    return (1.0 if hdr_hit else 0.4)*0.4 + min(cite/MIN_CITE_RATIO,1)*0.5 + min(punct/0.05,1)*0.1

async def _gpt_flag(txt:str)->bool:
    if not(openai and openai.api_key): return False
    r=await openai.ChatCompletion.acreate(model=GPT_MODEL,messages=[{"role":"system","content":GPT_SYS},{"role":"user","content":txt[:1000]}],temperature=0,max_tokens=GPT_TOK)
    return r.choices[0].message.content.strip().upper().startswith("B")

# ────────────────────────── Kern‑Detection ------------------------------------------

def _logical_label(page:fitz.Page,off:int|None)->str:
    return page.label or str(page.number+1-off) if off is not None else str(page.number+1)

def _load_sample(doc:fitz.Document,head:float,tail:float)->list[PageInfo]:
    n=doc.page_count; head_n=math.ceil(n*head); tail_n=math.ceil(n*tail)
    off=None
    for i in range(min(20,n)):
        txt=doc.load_page(i).get_text("text",sort=True)
        if re.search(r"\b1\b",txt.splitlines()[-1]): off=i; break
    pages=[]; idxs=list(range(head_n))+list(range(n-tail_n,n))
    for idx in dict.fromkeys(idxs):
        p=doc.load_page(idx)
        pages.append(PageInfo(idx,_logical_label(p,off),p.get_text("text",sort=True)))
    return pages


def _best_interval(scores:Sequence[float])->tuple[int,int]|None:
    med=median(scores); cur=None; best=None
    for i,sc in enumerate(scores):
        if sc>=med and cur is None: cur=i
        elif sc<med and cur is not None:
            if i-cur>=MIN_BLOCK_LEN: best=(cur,i-1) if best is None or i-cur>best[1]-best[0]+1 else best
            cur=None
    if cur is not None and len(scores)-cur>=MIN_BLOCK_LEN:
        best=(cur,len(scores)-1) if best is None or len(scores)-cur>best[1]-best[0]+1 else best
    return best


def detect(pdf:str|Path,*,head:float=0.08,tail:float=0.30,use_gpt:bool=False)->Optional[tuple[int,int]]:
    pdf=Path(pdf); doc=fitz.open(pdf)
    pages=_load_sample(doc,head,tail)
    if not pages: return None
    with ThreadPoolExecutor(max_workers=min(8,os.cpu_count() or 4)) as tp:
        for p,s in zip(pages,tp.map(_score_page,(pg.text for pg in pages))): p.score=s
    if use_gpt and openai and openai.api_key:
        med=median(p.score for p in pages)
        async def refine():
            tasks=[_gpt_flag(p.text) if p.score<med else asyncio.sleep(0) for p in pages]
            for p,ok in zip(pages,await asyncio.gather(*tasks)): p.score=1.0 if ok else p.score
        asyncio.run(refine())
    iv=_best_interval([p.score for p in pages])
    if not iv: return None
    return pages[iv[0]].idx+1,pages[iv[1]].idx+1

# ────────────────────────── Batch & CLI (gekürzt) ----------------------------------

def detect_all(dir:Path,**kw):
    pdfs=[p for p in Path(dir).glob("*.pdf")]; out={}
    with ProcessPoolExecutor() as pool:
        futs={pool.submit(detect,p,**kw):p.name for p in pdfs}
        for fut in tqdm(as_completed(futs),total=len(futs),desc="PDFs"):
            out[futs[fut]]=fut.result()
    return out

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("path",type=Path); ap.add_argument("--gpt",action="store_true"); ns=ap.parse_args()
    LOG.setLevel(logging.DEBUG)
    if ns.path.is_file(): print(detect(ns.path,use_gpt=ns.gpt))
    else: print(json.dumps(detect_all(ns.path,use_gpt=ns.gpt),indent=2,ensure_ascii=False))
