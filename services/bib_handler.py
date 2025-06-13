#!/usr/bin/env python3
# ─────────────────────────────────────────────────────────────────────────────
#  services/bib_handler.py        ·   rev. 2025-05-04
#  ---------------------------------------------------------------------------
#  • holt die Bibliographie-Seiten eines Dokuments
#  • parst Referenzen → legt neue works (analysed = 0) an
#  • legt Kanten in citations ab
# ─────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import asyncio, hashlib, logging, os, random, re, tempfile, time, unicodedata
from functools      import wraps
from pathlib        import Path
from typing         import Any, Dict, List, Optional, Tuple, TypeVar

import fitz                                           # PyMuPDF
import mysql.connector as mysql
from dotenv          import load_dotenv

from db.db_config            import get_conn
from services.ref_extractor  import extract_references

# ───────────────────────────  Konfiguration / Logging  ──────────────────────
load_dotenv()

LOG = logging.getLogger("bib_handler")
LOG.setLevel(os.getenv("BIB_LOG", "INFO").upper())

UPLOAD_DIR   = Path(os.getenv("UPLOAD_DIR", tempfile.gettempdir()))
REF_DEBUG    = os.getenv("REF_DEBUG",        "0") == "1"
REF_PRINT    = os.getenv("REF_PRINT_LINES",  "0") == "1"
REF_W_PRINT  = os.getenv("REF_PRINT_WORKS",  "0") == "1"

MAX_RETRIES  = int(os.getenv("DB_RETRY",      "4"))
RETRY_BASE   = float(os.getenv("DB_RETRY_BASE", "0.4"))

# ───────────────────────────  Regex-Pools  ──────────────────────────────────
YEAR_RE   = re.compile(r"(1[5-9]\d{2}|20\d{2})[a-z]?")

_PATTERNS: Tuple[re.Pattern[str], ...] = (
    # Chicago NB
    re.compile(r"^(?P<authors>.+?)\.\s+(?P<title>.+?)\.\s+(?P<place>.+?:\s+)?"
               r"(?P<publisher>.+?),\s+(?P<year>\d{4}[a-z]?)\.\s*$", re.U),
    # Chicago AD
    re.compile(r"^(?P<authors>.+?)\s+(?P<year>\d{4}[a-z]?)\.\s+(?P<title>.+?)\.\s*$", re.U),
    # MLA
    re.compile(r"^(?P<authors>.+?)\.\s+(?P<title>.+?)\.\s+(?P<publisher>.+?),\s+"
               r"(?P<year>\d{4}[a-z]?)\.\s*$", re.U),
    # Harvard
    re.compile(r"^(?P<authors>.+?),\s+(?P<year>\d{4}[a-z]?)\.\s+(?P<title>.+?)\.\s*", re.U),
    # MHRA / Oxford
    re.compile(r"^(?P<authors>.+?),\s+(?P<title>.+?)\s+\((?P<place>.+?:\s+)?"
               r"(?P<publisher>.+?),\s+(?P<year>\d{4}[a-z]?)\)\.\s*$", re.U),
    # IEEE / nummerisch
    re.compile(r"^\s*\[?\d+\]?\s+(?P<authors>.+?),\s+['“\"]?(?P<title>.+?)['”\"]?,\s+"
               r"(?P<year>\d{4}[a-z]?)", re.U),
)

# ───────────────────────────  Parser-Filter  ────────────────────────────────
BANNED_PREFIX = re.compile(r"^[)\-–•●]|^\d+\.$")                 # Aufzählungszeichen
BANNED_PHRASE = re.compile(r"\b(chapter|figure|table|slide|agenda)\b", re.I)
MIN_TITLE_WORDS = 4

# ───────────────────────────  Helper  ───────────────────────────────────────
def _norm(txt: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKD", txt).lower()).strip()

def _work_hash(title: str, authors: str,
               publisher: str|None, year: Optional[int]) -> str:
    return hashlib.sha1(
        f"{_norm(title)}|{_norm(authors)}|{_norm(publisher or '')}|{year or ''}"
        .encode()).hexdigest()

def _plausible(rec: Dict[str, Any]) -> bool:
    return (
        ',' in rec["authors"] and
        len(rec["title"].split()) >= MIN_TITLE_WORDS
    )

def _parse_line(line: str) -> Optional[Dict[str, Any]]:
    if REF_PRINT:
        LOG.debug("[RAW] %s", line)

    ln = line.strip()
    if (not ln or
        BANNED_PREFIX.match(ln) or
        BANNED_PHRASE.search(ln)):
        return None

    # 1)  kuratierte Patterns ----------------------------------------------
    for pat in _PATTERNS:
        if (m := pat.match(ln)):
            rec = {
                "authors"  : m["authors"].rstrip(" ,.;"),
                "title"    : m["title"].rstrip(" .;"),
                "publisher": (m.groupdict().get("publisher") or "").strip(" ,.;"),
                "year"     : int(YEAR_RE.search(m["year"]).group()[:4]),
            }
            if _plausible(rec):
                if REF_DEBUG:
                    LOG.debug("✓ %s – %s (%s)",
                              rec['authors'][:40], rec['title'][:60], rec['year'])
                return rec
            return None

    # 2) heuristischer Minimal-Fallback ------------------------------------
    if (m := YEAR_RE.search(ln)):
        year   = int(m.group()[:4])
        before = ln[:m.start()].strip(" ,.;–-")
        after  = ln[m.end():].strip(" .;:,-")
        title  = re.split(r"[.;:]", after, 1)[0].strip()
        rec = {"authors": before, "title": title,
               "publisher": "", "year": year}
        if _plausible(rec):
            if REF_DEBUG:
                LOG.debug("✓ %s – %s (%s)  [fallback]",
                          rec['authors'][:40], rec['title'][:60], rec['year'])
            return rec
    return None

# ─────────────────────────── Retry-Decorator  ──────────────────────────────
T = TypeVar("T")
def with_retry(fn):
    @wraps(fn)
    def _wrap(*args, **kw) -> T:
        for attempt in range(MAX_RETRIES + 1):
            try:
                return fn(*args, **kw)
            except mysql.Error as err:
                if attempt == MAX_RETRIES or err.errno not in (1205, 1213, 2006):
                    raise
                back = RETRY_BASE * (2 ** attempt) * (0.7 + random.random()*0.6)
                LOG.warning("MySQL-retry (%s) in %.2fs – %s", attempt+1, back, err)
                time.sleep(back)
    return _wrap

# ─────────────────────────── Referenzen holen  ─────────────────────────────
def _extract_refs(pdf: Path, start: int, end: int) -> List[Dict]:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return extract_references(
            pdf,
            range(start-1, end),             # 0-basiert
            use_gpt=os.getenv("REF_GPT", "0") == "1",
            line_parser=_parse_line,
        )
    finally:
        loop.close()
        asyncio.set_event_loop(None)

# ─────────────────────────── DB-Upserts  ────────────────────────────────────
@with_retry
def _upsert_refs(cnx: mysql.connection_cext.CMySQLConnection,
                 refs: List[Dict], *, src_work: int) -> int:
    ins_cnt = 0
    with cnx.cursor(dictionary=True) as cur:
        for r in refs:
            h = _work_hash(r["title"], r["authors"], r["publisher"], r["year"])

            # works (analysed = 0, weil nur zitiert)
            cur.execute("SELECT id FROM works WHERE hash=%s", (h,))
            row = cur.fetchone()
            created = False
            if row:
                dst_id = row["id"]
            else:
                cur.execute("""
                    INSERT INTO works (hash,title,authors,publisher,year,analysed)
                    VALUES (%s,%s,%s,%s,%s,0)
                """, (h,
                      r["title"][:512],
                      r["authors"][:255],
                      r["publisher"][:200],
                      r["year"]))
                dst_id = cur.lastrowid
                created = True

            if REF_W_PRINT:
                tag = "NEW" if created else "HIT"
                LOG.info("%5s│ %3s │ %-60s │ %s", dst_id, tag,
                         r["title"][:60], r["year"])

            # citations
            cur.execute("""
                INSERT INTO citations (from_work_id,to_work_id,count)
                VALUES (%s,%s,1)
                ON DUPLICATE KEY UPDATE count = count + 1
            """, (src_work, dst_id))
            ins_cnt += 1
    return ins_cnt

# ─────────────────────────── Public Entry-Point  ───────────────────────────
def process_document(doc_id: int, *, strict: bool = False) -> int:
    try:
        with get_conn() as cnx, cnx.cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT d.filename, d.work_id,
                       bp.start_page, bp.end_page
                  FROM documents d
                  JOIN bibliography_pages bp ON bp.document_id = d.id
                 WHERE d.id=%s
            """, (doc_id,))
            meta = cur.fetchone()
            if not meta:
                raise ValueError("bibliography_pages fehlt")

            pdf_path = Path(meta["filename"])
            if not pdf_path.is_absolute():
                pdf_path = UPLOAD_DIR / pdf_path
            if not pdf_path.exists():
                raise FileNotFoundError(pdf_path)

            LOG.info("⇢ process_document id=%s  (%s  pages %s-%s)",
                     doc_id, pdf_path.name, meta["start_page"], meta["end_page"])

            refs = _extract_refs(pdf_path,
                                 meta["start_page"], meta["end_page"])
            if not refs:
                LOG.warning("doc_id=%s – keine Referenzen erkannt", doc_id)
                return 0
            LOG.debug("   %d Referenzen erkannt", len(refs))

            ins_cnt = _upsert_refs(cnx, refs, src_work=meta["work_id"])
            cnx.commit()

            LOG.info("doc_id=%s – %d Referenzen gespeichert", doc_id, ins_cnt)
            return ins_cnt

    except Exception as exc:
        LOG.error("process_document failed (doc_id=%s) – %s", doc_id, exc)
        if strict:
            raise
        return 0

# ─────────────────────────── CLI-Test  (python -m …)  ──────────────────────
if __name__ == "__main__":
    import argparse, sys

    cli = argparse.ArgumentParser("Manual run of bib_handler")
    g   = cli.add_mutually_exclusive_group(required=True)
    g.add_argument("--doc-id", type=int, help="row-id aus Tabelle documents")
    g.add_argument("filename", nargs="?",
                   help="PDF-Filename (lookup in documents.filename)")
    cli.add_argument("--strict", action="store_true")
    a = cli.parse_args()

    if a.filename:
        with get_conn() as cnx, cnx.cursor() as cur:
            cur.execute("SELECT id FROM documents WHERE filename=%s",
                        (Path(a.filename).name,))
            r = cur.fetchone()
        if not r:
            print("❌ File nicht in DB"); sys.exit(1)
        a.doc_id = r[0]

    n = process_document(a.doc_id, strict=a.strict)
    print(f"✓ {n} references stored.")