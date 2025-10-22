#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GUI-only PDF Bibliography/References Extractor -> SQLite DB

- Startet nur GUI-Dialoge (Tkinter):
  1) Ordner mit PDFs wählen
  2) Ziel-DB bestätigen/auswählen (Standard: <gewählter_Ordner>/extracted_refs.sqlite)
- Extraktion:
  - Heuristik (strenge Überschrifts-Erkennung + Referenzmuster)
  - Fallback/Ergänzung via OpenAI (Responses API), Key aus .env (OPENAI_API_KEY)
- Ergebnisse:
  - werden in normalisierte SQLite-Tabellen geschrieben (idempotent, Upserts)
  - detaillierte Debug-Prints in der Konsole

Tabellen:
  runs(id, started_at_utc, finished_at_utc, source_dir, config_json)
  documents(id, path UNIQUE, filename, page_count, sha256, mtime_utc)
  extractions(id, run_id, document_id, method, pages_json, ref_count, UNIQUE(run_id, document_id))
  references(id, document_id, extraction_id, position, entry, page_from, page_to,
             UNIQUE(document_id, entry))

Hinweise:
- Vollständige 100%-Garantie gibt es mit KI nicht; Script kombiniert robuste Heuristik mit LLM.
"""

import os
import re
import sys
import json
import time
import math
import hashlib
import sqlite3
import concurrent.futures
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Iterable, Optional, Dict

# === Third-party ===
try:
    import fitz  # PyMuPDF
except Exception as e:
    print("[FATAL] PyMuPDF (fitz) nicht installiert. -> pip install pymupdf", file=sys.stderr)
    raise

try:
    from dotenv import load_dotenv
except Exception:
    print("[FATAL] python-dotenv nicht installiert. -> pip install python-dotenv", file=sys.stderr)
    raise

# OpenAI SDK (Responses API)
try:
    from openai import OpenAI
except Exception:
    print("[WARN] openai-Paket fehlt oder ist inkompatibel. -> pip install openai", file=sys.stderr)
    OpenAI = None  # optional


# === GUI ===
try:
    import tkinter as tk
    from tkinter import filedialog, messagebox
except Exception:
    print("[FATAL] Tkinter ist nicht verfügbar. Bitte Python mit Tk-Unterstützung verwenden.", file=sys.stderr)
    raise


# === Konfiguration ===
DEFAULT_DIR = Path("/Users/python/Python Projekte/docker_shift/Find Bibliography/db/data/azk_library")
FALLBACK_DIR = Path("/mnt/data")

# Strengere Überschriften-Muster (exakte Zeilen, nicht irgendwo im Fließtext)
BIB_HEADINGS_STRICT = [
    r"^(?:Literatur)$",
    r"^(?:Bibliograf[iy]e|Bibliograph[iy])$",
    r"^(?:Quellen\s+und\s+Literatur)$",
    r"^(?:References|Referenzen)$",
    r"^(?:Anmerkungen)$",
    r"^(?:Quellen)$",
]

STOP_HEADINGS_STRICT = [
    r"^(?:Abbildungen|Résumé|Summary|Zusammenfassung|Index|Register|Anhang|Appendix)$"
]

# Referenzstart-Heuristik
RE_NUM_START = re.compile(r"^\s*([0-9]{1,3})[.)\s]\s+")
RE_AUTHOR_YEAR_START = re.compile(
    r"""^\s*
        [A-ZÄÖÜ][A-Za-zÄÖÜäöüß\.\-\'\s]+   # Autor(en)
        [,;:]?\s*
        \(?\d{4}[a-z]?\)?                  # Jahr
        (?:\s*[,.;:)]|\s+-|\s+)
    """,
    re.VERBOSE
)
RE_ALL_DIGITS = re.compile(r"^\s*[0-9]{1,4}\s*$")

# OpenAI
OPENAI_MODEL_CLASSIFY = "gpt-4o-mini"
OPENAI_MODEL_PARSE = "gpt-4o-mini"
OPENAI_MAX_PAGE_CHARS = 7000
OPENAI_REQUEST_CONCURRENCY = 3
OPENAI_RETRIES = 3
OPENAI_MIN_CONFIDENCE = 0.6  # etwas strenger

# Heuristik-Parameter
DEFAULT_MAX_FOLLOW_PAGES = 6
SCAN_TAIL_RATIO = 0.35          # nur die letzten ~35% scannen (Biblios meist hinten)
MIN_REF_LIKE_LINES_RATIO = 0.20 # Seite gilt als Liste, wenn >=20% Zeilen wie Referenzen starten
MIN_HEURISTIC_ENTRIES_TO_ACCEPT = 3


# === Logging ===
def debug(msg: str) -> None:
    print(f"[DEBUG] {msg}")

def info(msg: str) -> None:
    print(f"[INFO]  {msg}")

def warn(msg: str) -> None:
    print(f"[WARN]  {msg}", file=sys.stderr)

def err(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)


# === Helpers ===
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def file_sha256(path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def safe_json(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "{}"


# === DB Layer ===
class RefDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        info(f"DB geöffnet: {db_path}")

    def close(self):
        try:
            self.conn.close()
            info("DB Verbindung geschlossen.")
        except Exception:
            pass

    def ensure_schema(self):
        info("DB Schema prüfen/erstellen …")
        cur = self.conn.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT,
            source_dir TEXT NOT NULL,
            config_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL UNIQUE,
            filename TEXT NOT NULL,
            page_count INTEGER NOT NULL,
            sha256 TEXT NOT NULL,
            mtime_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS extractions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            document_id INTEGER NOT NULL,
            method TEXT NOT NULL,           -- 'heuristic' | 'llm' | 'combined'
            pages_json TEXT NOT NULL,       -- z.B. [161,162,163]
            ref_count INTEGER NOT NULL,
            FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
            FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE,
            UNIQUE(run_id, document_id)
        );

        CREATE TABLE IF NOT EXISTS references (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            extraction_id INTEGER NOT NULL,
            position INTEGER NOT NULL,      -- 1..N
            entry TEXT NOT NULL,
            page_from INTEGER,
            page_to INTEGER,
            FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE,
            FOREIGN KEY(extraction_id) REFERENCES extractions(id) ON DELETE CASCADE,
            UNIQUE(document_id, entry)
        );

        CREATE INDEX IF NOT EXISTS idx_documents_sha ON documents(sha256);
        CREATE INDEX IF NOT EXISTS idx_refs_doc ON references(document_id);
        """)
        self.conn.commit()
        info("DB Schema bereit.")

    def begin_run(self, source_dir: Path, config: dict) -> int:
        started = utc_now_iso()
        cfg = safe_json(config)
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO runs (started_at_utc, source_dir, config_json) VALUES (?, ?, ?)",
            (started, str(source_dir), cfg)
        )
        run_id = cur.lastrowid
        self.conn.commit()
        info(f"Run gestartet: run_id={run_id}")
        return run_id

    def finish_run(self, run_id: int):
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE runs SET finished_at_utc=? WHERE id=?",
            (utc_now_iso(), run_id)
        )
        self.conn.commit()
        info(f"Run abgeschlossen: run_id={run_id}")

    def upsert_document(self, pdf_path: Path, page_count: int) -> int:
        path = str(pdf_path)
        filename = pdf_path.name
        sha = file_sha256(pdf_path)
        mtime = datetime.fromtimestamp(pdf_path.stat().st_mtime, tz=timezone.utc).isoformat()

        cur = self.conn.cursor()
        # Versuche Insert; wenn path schon existiert -> update (page_count/mtime/sha)
        try:
            cur.execute("""
                INSERT INTO documents (path, filename, page_count, sha256, mtime_utc)
                VALUES (?, ?, ?, ?, ?)
            """, (path, filename, page_count, sha, mtime))
            doc_id = cur.lastrowid
            info(f"Dokument registriert (neu): id={doc_id}, {filename}")
        except sqlite3.IntegrityError:
            # Update & fetch id
            cur.execute("""
                UPDATE documents
                SET page_count=?, sha256=?, mtime_utc=?, filename=?
                WHERE path=?
            """, (page_count, sha, mtime, filename, path))
            cur.execute("SELECT id FROM documents WHERE path=?", (path,))
            row = cur.fetchone()
            doc_id = int(row[0]) if row else -1
            info(f"Dokument aktualisiert: id={doc_id}, {filename}")

        self.conn.commit()
        return doc_id

    def record_extraction(self,
                          run_id: int,
                          document_id: int,
                          method: str,
                          pages: List[int],
                          refs: List[str]) -> Tuple[int, int]:
        """
        Schreibt extraction + references (idempotent). Gibt (extraction_id, inserted_refs_count) zurück.
        """
        pages_json = safe_json([p + 1 for p in pages])  # Speichere 1-basiert in DB
        ref_count = len(refs)
        cur = self.conn.cursor()
        try:
            cur.execute("""
                INSERT INTO extractions (run_id, document_id, method, pages_json, ref_count)
                VALUES (?, ?, ?, ?, ?)
            """, (run_id, document_id, method, pages_json, ref_count))
            extraction_id = cur.lastrowid
            created_new = True
        except sqlite3.IntegrityError:
            cur.execute("""
                SELECT id FROM extractions WHERE run_id=? AND document_id=?
            """, (run_id, document_id))
            row = cur.fetchone()
            extraction_id = int(row[0]) if row else -1
            created_new = False

        inserted = 0
        pos = 1
        for entry in refs:
            entry_norm = re.sub(r"\s+", " ", entry).strip(" ,;")
            try:
                cur.execute("""
                    INSERT OR IGNORE INTO references (document_id, extraction_id, position, entry, page_from, page_to)
                    VALUES (?, ?, ?, ?, NULL, NULL)
                """, (document_id, extraction_id, pos, entry_norm))
                if cur.rowcount > 0:
                    inserted += 1
            except Exception as e:
                warn(f"Fehler beim Speichern einer Referenz (pos={pos}): {e}")
            pos += 1

        self.conn.commit()
        info(f"Extraction gespeichert (run={run_id}, doc={document_id}, method={method}, pages={pages_json}) – "
             f"Referenzen eingefügt: {inserted}/{ref_count}, extraction_id={extraction_id}, neu={created_new}")
        return extraction_id, inserted


# === OpenAI Helpers ===
def load_api_client() -> Optional["OpenAI"]:
    load_dotenv()
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        warn("OPENAI_API_KEY fehlt (.env). LLM-Funktionen werden übersprungen.")
        return None
    if OpenAI is None:
        warn("openai-Paket nicht verfügbar – LLM-Funktionen werden übersprungen.")
        return None
    try:
        client = OpenAI(api_key=api_key)
        debug("OpenAI-Client initialisiert.")
        return client
    except Exception as e:
        err(f"Konnte OpenAI-Client nicht initialisieren: {e}")
        return None

def _extract_first_json_blob(text: str):
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    m2 = re.search(r"\[.*\]", text, flags=re.DOTALL)
    if m2:
        try:
            return json.loads(m2.group(0))
        except Exception:
            pass
    return None

def openai_call_json(client: "OpenAI", prompt: str, purpose: str, model: str) -> Optional[dict]:
    if client is None:
        return None
    for attempt in range(1, OPENAI_RETRIES + 1):
        try:
            debug(f"[OpenAI] {purpose} – Versuch {attempt}/{OPENAI_RETRIES}")
            resp = client.responses.create(
                model=model,
                instructions="Return ONLY compact JSON. No commentary. No markdown.",
                input=prompt,
            )
            out = getattr(resp, "output_text", None) or ""
            data = _extract_first_json_blob(out)
            if isinstance(data, (dict, list)):
                return data
            warn("[OpenAI] Kein valides JSON – Retry …")
        except Exception as e:
            warn(f"[OpenAI] API-Fehler: {e} – Retry …")
        time.sleep(0.6 * attempt)
    return None


# === Text/Heuristik ===
def page_text(doc: "fitz.Document", page_index: int) -> str:
    try:
        page = doc.load_page(page_index)
        txt = page.get_text("text") or ""
        return txt
    except Exception as e:
        warn(f"Text-Extraktion fehlgeschlagen (Seite {page_index+1}): {e}")
        return ""

def lines_strict_headings(text: str, patterns: Iterable[str], max_lines: int = 50) -> Optional[str]:
    """
    Strenge Überschriftenerkennung: exakte Zeilen im oberen Seitenbereich.
    """
    head = text.splitlines()[:max_lines]
    for ln in head:
        ln_clean = re.sub(r"\s+", " ", ln).strip()
        for pat in patterns:
            if re.match(pat, ln_clean, flags=re.IGNORECASE):
                return pat
    return None

def looks_like_reference_start(line: str) -> bool:
    return bool(RE_NUM_START.match(line) or RE_AUTHOR_YEAR_START.match(line))

def clean_lines(lines: List[str]) -> List[str]:
    cleaned: List[str] = []
    buffer = ""
    for raw in lines:
        line = raw.rstrip()
        if RE_ALL_DIGITS.match(line.strip()):
            continue
        if re.search(r"(?:\-|­)\s*$", line):
            buffer += re.sub(r"(?:\-|­)\s*$", "", line)
            continue
        else:
            line_norm = re.sub(r"\s+", " ", (buffer + line)).strip()
            if line_norm:
                cleaned.append(line_norm)
            buffer = ""
    if buffer:
        cleaned.append(re.sub(r"\s+", " ", buffer).strip())
    return cleaned

def ref_like_ratio(text: str) -> float:
    lines = clean_lines(text.splitlines())
    if not lines:
        return 0.0
    starts = sum(1 for ln in lines if looks_like_reference_start(ln))
    return starts / max(1, len(lines))

def split_references_from_text(text: str) -> List[str]:
    lines = clean_lines(text.splitlines())
    if not lines:
        return []
    refs: List[str] = []
    current: List[str] = []

    def flush():
        if current:
            entry = " ".join(current).strip()
            entry = re.sub(r"\s*[;,:]\s*$", "", entry)
            if entry:
                refs.append(entry)

    for ln in lines:
        if looks_like_reference_start(ln):
            flush()
            current = [RE_NUM_START.sub("", ln).strip()]
        else:
            if not current:
                continue
            current.append(ln)
    flush()
    return refs

def extract_from_pages(doc: "fitz.Document", page_indices: List[int]) -> str:
    merged = []
    for p in page_indices:
        merged.append(page_text(doc, p))
    return "\n".join(merged)

def truncate_for_llm(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    head = text[: limit // 2]
    tail = text[- limit // 2 :]
    return head + "\n...\n" + tail


# === Seitenfindung ===
def heuristic_bib_pages(doc: "fitz.Document", max_follow: int = DEFAULT_MAX_FOLLOW_PAGES) -> List[int]:
    pages_in_bib: List[int] = []
    n = doc.page_count
    # nur die hinteren ~35% scannen
    start_idx = max(0, int(math.floor(n * (1.0 - SCAN_TAIL_RATIO))))
    info(f"Heuristik: scanne Seiten {start_idx+1}..{n} ({n} gesamt) …")

    i = start_idx
    while i < n:
        txt = page_text(doc, i)
        if not txt.strip():
            i += 1
            continue

        hit = lines_strict_headings(txt, BIB_HEADINGS_STRICT)
        stop_hit = lines_strict_headings(txt, STOP_HEADINGS_STRICT)
        ratio = ref_like_ratio(txt)

        if hit and (ratio >= MIN_REF_LIKE_LINES_RATIO or not stop_hit):
            info(f"   ▲ Heading '{hit}' (Seite {i+1}), ratio={ratio:.2f}")
            pages_in_bib.append(i)
            follow, j = 0, i + 1
            while j < n and follow < max_follow:
                t2 = page_text(doc, j)
                if not t2.strip():
                    break
                if lines_strict_headings(t2, STOP_HEADINGS_STRICT):
                    info(f"   ■ Stop-Überschrift Seite {j+1}")
                    break
                r2 = ref_like_ratio(t2)
                if r2 >= MIN_REF_LIKE_LINES_RATIO:
                    pages_in_bib.append(j)
                    follow += 1
                    j += 1
                else:
                    break
            i = j
            continue
        i += 1

    pages_sorted = sorted(set(pages_in_bib))
    info(f"Heuristik-Ergebnis: {len(pages_sorted)} Seite(n) -> {', '.join(str(p+1) for p in pages_sorted) or '–'}")
    return pages_sorted


# === OpenAI: Klassifikation & Parsing ===
def llm_classify_page(client: "OpenAI", page_text_input: str, page_index: int) -> Tuple[int, str, float]:
    snippet = truncate_for_llm(page_text_input, OPENAI_MAX_PAGE_CHARS)
    prompt = (
        "You are a precise document page classifier.\n"
        "Classes: BIBLIOGRAPHY, REFERENCES, NOTES, NONE.\n"
        "Consider multilingual headings (German/English), and list-like structure.\n"
        f"PAGE_INDEX:{page_index}\n"
        "Return ONLY JSON: {\"label\":\"...\",\"confidence\":0..1}\n"
        "=== TEXT START ===\n"
        f"{snippet}\n"
        "=== TEXT END ==="
    )
    data = openai_call_json(client, prompt, purpose=f"Classify page {page_index+1}", model=OPENAI_MODEL_CLASSIFY)
    if isinstance(data, dict):
        label = str(data.get("label", "NONE")).upper().strip()
        try:
            conf = float(data.get("confidence", 0.0))
        except Exception:
            conf = 0.0
        return page_index, label, conf
    return page_index, "NONE", 0.0

def llm_find_bib_pages(client: "OpenAI", doc: "fitz.Document") -> List[int]:
    n = doc.page_count
    start_idx = max(0, int(math.floor(n * (1.0 - SCAN_TAIL_RATIO))))
    info(f"LLM-Klassifikation: Seiten {start_idx+1}..{n} bewerten …")
    results: List[Tuple[int, str, float]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=OPENAI_REQUEST_CONCURRENCY) as ex:
        futs = []
        for i in range(start_idx, n):
            txt = page_text(doc, i)
            if txt.strip():
                futs.append(ex.submit(llm_classify_page, client, txt, i))
        for fut in concurrent.futures.as_completed(futs):
            idx, label, conf = fut.result()
            debug(f"[LLM] Seite {idx+1}: label={label}, conf={conf:.2f}")
            results.append((idx, label, conf))

    cand = sorted([i for (i, lab, cf) in results if lab in {"BIBLIOGRAPHY", "REFERENCES", "NOTES"} and cf >= OPENAI_MIN_CONFIDENCE])

    # Folge-Seiten mit Listen-Charakter anfügen
    extended = set(cand)
    for i in list(cand):
        j = i + 1
        if j < n:
            t2 = page_text(doc, j)
            if ref_like_ratio(t2) >= MIN_REF_LIKE_LINES_RATIO:
                extended.add(j)

    pages_sorted = sorted(extended)
    info(f"LLM-Ergebnis: {len(pages_sorted)} Seite(n) -> {', '.join(str(p+1) for p in pages_sorted) or '–'}")
    return pages_sorted

def llm_extract_references(client: "OpenAI", merged_text: str, page_indices: List[int]) -> List[str]:
    snippet = truncate_for_llm(merged_text, OPENAI_MAX_PAGE_CHARS * 2)
    prompt = (
        "Extract bibliographic references from the text below.\n"
        "Return ONLY a JSON array of strings; each string is one full reference.\n"
        f"Pages: {[i+1 for i in page_indices]}\n"
        "=== TEXT START ===\n"
        f"{snippet}\n"
        "=== TEXT END ==="
    )
    data = openai_call_json(client, prompt, purpose="Parse references", model=OPENAI_MODEL_PARSE)
    if isinstance(data, list):
        out = []
        seen = set()
        for it in data:
            s = re.sub(r"\s+", " ", str(it)).strip(" ,;")
            if s and s not in seen:
                out.append(s)
                seen.add(s)
        return out
    return []


# === PDF-Verarbeitung ===
def process_pdf(pdf_path: Path, client: Optional["OpenAI"]) -> Tuple[Path, List[int], List[str], str]:
    """
    Returns: (pdf_path, selected_pages, references, method)
    """
    info(f"Öffne PDF: {pdf_path}")
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        err(f"Konnte PDF nicht öffnen: {pdf_path} -> {e}")
        return (pdf_path, [], [], "error")

    # 1) Heuristik
    pages = heuristic_bib_pages(doc, max_follow=DEFAULT_MAX_FOLLOW_PAGES)
    method = "heuristic"

    # 2) LLM nur, wenn Heuristik leer ist
    if (not pages) and client is not None:
        info("Heuristik ohne Treffer – LLM-Seitenfindung …")
        pages = llm_find_bib_pages(client, doc)
        method = "llm"

    if not pages:
        warn(f"Keine Bibliographie-/Referenz-Seiten erkannt in {pdf_path.name}")
        return (pdf_path, [], [], method)

    # 3) Referenzen extrahieren
    merged_text = extract_from_pages(doc, pages)
    refs = split_references_from_text(merged_text)
    info(f"Heuristische Referenz-Extraktion: {len(refs)} Einträge")

    # 4) Optional: LLM-Parsing zur Ergänzung, wenn wenig
    if client is not None and len(refs) < MIN_HEURISTIC_ENTRIES_TO_ACCEPT:
        info("Wenig/keine Einträge – LLM-Parsing ergänzen …")
        llm_refs = llm_extract_references(client, merged_text, pages)
        if llm_refs:
            s = set(refs)
            add = [r for r in llm_refs if r not in s]
            refs.extend(add)
            if method == "heuristic" and add:
                method = "combined"
            info(f"LLM-Parsing ergänzt: +{len(add)} -> total {len(refs)}")

    return (pdf_path, pages, refs, method)


# === GUI ===
def pick_dir_gui() -> Optional[Path]:
    debug("Initialisiere GUI …")
    root = tk.Tk()
    root.withdraw()
    try:
        start_dir = DEFAULT_DIR if DEFAULT_DIR.exists() else (FALLBACK_DIR if FALLBACK_DIR.exists() else Path.home())
        debug(f"Ordner-Dialog öffnen (Startordner: {start_dir})")
        chosen = filedialog.askdirectory(
            title="Wähle ein Verzeichnis mit PDFs",
            initialdir=str(start_dir)
        )
        if not chosen:
            if DEFAULT_DIR.exists():
                messagebox.showinfo("Info", f"Kein Ordner gewählt.\nVerwende Standardordner:\n{DEFAULT_DIR}")
                return DEFAULT_DIR
            elif FALLBACK_DIR.exists():
                messagebox.showinfo("Info", f"Kein Ordner gewählt.\nVerwende Fallback:\n{FALLBACK_DIR}")
                return FALLBACK_DIR
            else:
                messagebox.showerror("Fehler", "Kein Ordner gewählt und keine Default-Pfade verfügbar.")
                return None
        return Path(chosen)
    finally:
        try:
            root.destroy()
        except Exception:
            pass

def confirm_or_choose_db(default_db: Path) -> Optional[Path]:
    root = tk.Tk()
    root.withdraw()
    try:
        msg = f"DB-Datei verwenden?\n\n{default_db}\n\nDu kannst auch eine andere Datei wählen."
        if messagebox.askyesno("DB bestätigen", msg):
            return default_db
        else:
            f = filedialog.asksaveasfilename(
                title="Ziel-DB wählen",
                initialdir=str(default_db.parent),
                initialfile=default_db.name,
                defaultextension=".sqlite",
                filetypes=[("SQLite DB", "*.sqlite"), ("Alle Dateien", "*.*")]
            )
            if not f:
                messagebox.showerror("Fehler", "Keine DB gewählt.")
                return None
            return Path(f)
    finally:
        try:
            root.destroy()
        except Exception:
            pass


# === Orchestrierung ===
def discover_pdfs(directory: Path) -> List[Path]:
    info(f"Durchsuche PDF-Dateien in: {directory}")
    if not directory.exists():
        err(f"Verzeichnis existiert nicht: {directory}")
        return []
    pdfs = sorted([p for p in directory.rglob("*.pdf") if p.is_file()])
    info(f"Gefundene PDFs: {len(pdfs)}")
    return pdfs

def main():
    info("Starte GUI-only Bibliographie-Extractor -> DB …")

    # LLM-Client laden (optional)
    client = load_api_client()

    # 1) Ordner wählen
    base_dir = pick_dir_gui()
    if base_dir is None:
        err("Abbruch: Kein gültiger Ordner verfügbar.")
        sys.exit(2)

    # 2) DB bestätigen/auswählen
    default_db = base_dir / "extracted_refs.sqlite"
    db_path = confirm_or_choose_db(default_db)
    if db_path is None:
        err("Abbruch: Keine DB gewählt.")
        sys.exit(2)

    # 3) PDFs finden
    pdf_list = discover_pdfs(base_dir)
    if not pdf_list:
        warn("Keine PDFs gefunden – Ende.")
        return

    # 4) DB öffnen + Schema
    db = RefDB(db_path)
    db.ensure_schema()

    # 5) Run beginnen
    run_cfg = {
        "heuristics": {
            "tail_ratio": SCAN_TAIL_RATIO,
            "min_ref_like_ratio": MIN_REF_LIKE_LINES_RATIO,
            "max_follow_pages": DEFAULT_MAX_FOLLOW_PAGES
        },
        "llm": {
            "enabled": client is not None,
            "model_classify": OPENAI_MODEL_CLASSIFY,
            "model_parse": OPENAI_MODEL_PARSE,
            "min_conf": OPENAI_MIN_CONFIDENCE
        }
    }
    run_id = db.begin_run(base_dir, run_cfg)

    # 6) Parallel verarbeiten
    max_workers = min(8, os.cpu_count() or 4)
    info(f"Verarbeite {len(pdf_list)} Datei(en) mit {max_workers} Worker …")

    total_inserted_refs = 0
    processed_docs = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = []
            for pdf in pdf_list:
                futs.append(ex.submit(process_pdf, pdf, client))
            for fut in concurrent.futures.as_completed(futs):
                pdf_path, pages, refs, method = fut.result()
                # Dokument in DB registrieren/aktualisieren
                try:
                    # ggf. erneut öffnen, um page_count zu kennen (oder aus Cache holen)
                    doc = fitz.open(pdf_path)
                    page_count = doc.page_count
                except Exception:
                    page_count = 0
                doc_id = db.upsert_document(pdf_path, page_count)

                # Extraction + References persistieren
                extraction_id, ins = db.record_extraction(
                    run_id=run_id,
                    document_id=doc_id,
                    method=method,
                    pages=pages,
                    refs=refs
                )
                total_inserted_refs += ins
                processed_docs += 1

                # Konsolen-Ausgabe je Datei
                print(f"\n# {pdf_path.name}: {len(refs)} Referenz(en) erkannt (Methode: {method})")
                if refs:
                    for i, r in enumerate(refs, 1):
                        print(f"{i:>3}. {r}")
                else:
                    print("   (keine Referenzen gefunden)")

    finally:
        # 7) Run abschließen + DB schließen
        db.finish_run(run_id)
        db.close()

    info("\n==================== ZUSAMMENFASSUNG ====================")
    info(f"Verarbeitete Dokumente: {processed_docs}")
    info(f"In DB neu eingefügte Referenzen: {total_inserted_refs}")
    info(f"DB: {db_path}")
    info("=========================================================")


if __name__ == "__main__":
    main()