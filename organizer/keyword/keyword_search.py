"""
GUI-Tool: Friedens-/Kriegsbeendigungs-Signale in Sekundärliteratur finden und als JSON je Session speichern.

Zweck
------
Scannt Sekundärliteratur (PDF/TXT optional DOCX) in:
    /Users/programming/PycharmProjects/Find_Bibliography_NEw/data/azk_library
und ignoriert jeden Unterordner "chroniken" (case-insensitive).
Speichert Ergebnisse in einem neuen Session-Ordner unter:
    /Users/programming/PycharmProjects/Find_Bibliography_NEw/data/keyword_data/<SESSION_NAME>/

Erfasst werden: Trefferzahlen pro Datei, Kategorien, Term-Hits, KWIC-Beispiele, Fehler.
Ambige Terme (z. B. "Abschied", "Bund", "Stillstand") zählen nur bei Kontexttreffern.

Nutzung
--------
Einfach ausführen. Eine Tkinter-GUI öffnet sich. Session-Namen eingeben, "Scan starten".
Nach Abschluss "JSON exportieren". Debug-Ausgaben erscheinen in der Konsole.

Hinweise
--------
- PDF-Extraktion: versucht 'pypdf' oder 'PyPDF2'. Bei Fehlschlag wird die Datei übersprungen.
- DOCX: optional; wird nur verarbeitet, wenn 'python-docx' verfügbar ist.
- Keine CLI-Argumente, keine Warteprompts. Feste Defaults mit GUI.
"""

from __future__ import annotations

import json
import os
import platform
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# GUI
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

# Optional I/O libs
_PYPDF_AVAILABLE = False
_PYPDF2_AVAILABLE = False
_DOCX_AVAILABLE = False
try:
    import pypdf  # type: ignore
    _PYPDF_AVAILABLE = True
except Exception:
    try:
        import PyPDF2  # type: ignore
        _PYPDF2_AVAILABLE = True
    except Exception:
        pass

try:
    import docx  # type: ignore
    _DOCX_AVAILABLE = True
except Exception:
    pass


# ----------------------------- Datamodel ----------------------------- #

@dataclass
class Term:
    name: str
    category: str
    pattern: re.Pattern
    requires_context: bool = False
    context_terms: Tuple[re.Pattern, ...] = field(default_factory=tuple)

@dataclass
class Sample:
    term: str
    category: str
    page: Optional[int]
    kwic: str

@dataclass
class FileResult:
    path: str
    file_type: str
    pages: Optional[int]
    hits_total: int
    hits_by_category: Dict[str, int]
    hits_by_term: Dict[str, int]
    samples: List[Sample]
    error: Optional[str] = None


# ----------------------------- Config ----------------------------- #

DEFAULT_LIBRARY = "/Users/programming/PycharmProjects/Find_Bibliography_NEw/data/azk_library"
DEFAULT_OUTPUT_BASE = "/Users/programming/PycharmProjects/Find_Bibliography_NEw/data/keyword_data"

CATEGORIES = [
    "core_peace",            # Frieden, pax, concord-
    "truce",                 # Waffenstillstand, Treuga/Induciae
    "settlement",            # Vergleich, Sühne, Versöhnung, Einigung
    "legal_instrument",      # Vertrag, Landfrieden, Abschied (kontextuell), Bund/Bündnis (kontextuell)
    "end_of_hostilities",    # Beendigung/Einstellung Feindseligkeiten, beigelegt, Kapitulation
]

AMB_CONTEXT_NEAR_WORDS = [
    r"frieden", r"krieg", r"kriegs", r"kriege", r"kampf\w*", r"feindselig\w*",
    r"waffen\w*", r"vertrag\w*", r"beschluss\w*", r"beschieden", r"vereinbar\w*",
    r"schließ\w*|schliess\w*", r"schluss\w*", r"tagsatzung\w*", r"landfrieden\w*",
    r"zwischen", r"zu", r"von"
]

CONTEXT_PATTERNS = tuple(re.compile(rf"\b(?:{w})\b", re.IGNORECASE) for w in AMB_CONTEXT_NEAR_WORDS)


def build_lexicon(require_context_for_ambiguous: bool = True) -> List[Term]:
    """Erzeugt das Termlexikon mit Regexen und Kontextsteuerung."""
    terms: List[Term] = []

    # Core peace
    core_patterns = [
        r"frieden[srm]?", r"friden[srm]?", r"pax(?:is|em|e|i)?",
        r"concord(?:ia|iae|iam|ias|is|at(?:io|ionis|iones)?)",
        r"reconcili(?:atio(?:nes)?|ation|are|atio|atus|e?\w*)"
    ]
    for p in core_patterns:
        terms.append(Term(name=p, category="core_peace", pattern=re.compile(rf"\b{p}\b", re.IGNORECASE)))

    # Truce
    truce_patterns = [
        r"waffenstillstand", r"waffenruhe", r"feuerpause",
        r"induci(?:ae|arum|as|is|a)", r"treug(?:a|ae|arum|is|as)"
    ]
    for p in truce_patterns:
        terms.append(Term(name=p, category="truce", pattern=re.compile(rf"\b{p}\b", re.IGNORECASE)))

    # Settlement
    settlement_patterns = [
        r"vergleich\w*", r"sühne\w*|suehne\w*", r"versöhn\w*|versoehn\w*",
        r"einigung\w*", r"einung\w*", r"schlicht\w*", r"schiedsspruch\w*",
        r"invernehmen"
    ]
    for p in settlement_patterns:
        terms.append(Term(name=p, category="settlement", pattern=re.compile(rf"\b{p}\b", re.IGNORECASE)))

    # Legal instrument
    legal_strict = [r"landfrieden\w*", r"vertrag\w*", r"pact\w*|pakt\w*", r"abkommen\w*"]
    for p in legal_strict:
        terms.append(Term(name=p, category="legal_instrument", pattern=re.compile(rf"\b{p}\b", re.IGNORECASE)))

    legal_amb = [r"abschied\w*", r"bündnis\w*|buendnis\w*", r"bund(?!es)\w*"]
    for p in legal_amb:
        terms.append(Term(
            name=p, category="legal_instrument",
            pattern=re.compile(rf"\b{p}\b", re.IGNORECASE),
            requires_context=require_context_for_ambiguous,
            context_terms=CONTEXT_PATTERNS
        ))

    # End of hostilities
    endhost_amb = [
        r"beendig\w*", r"einstellung\w*", r"beilegen\w*|beigelegt\w*|beilegt\w*",
    ]
    for p in endhost_amb:
        terms.append(Term(
            name=p, category="end_of_hostilities",
            pattern=re.compile(rf"\b{p}\b", re.IGNORECASE),
            requires_context=True,
            context_terms=CONTEXT_PATTERNS
        ))
    endhost_strict = [r"kapitulation\w*", r"fristfrieden\w*"]  # selten, aber eindeutig
    for p in endhost_strict:
        terms.append(Term(name=p, category="end_of_hostilities", pattern=re.compile(rf"\b{p}\b", re.IGNORECASE)))

    # English (falls einzelne Titel/Abstracts EN sind)
    english = [
        ("peace", "core_peace"),
        ("truce", "truce"),
        ("cease[-\s]?fire|ceasefire", "truce"),
        ("cessation of hostilities", "end_of_hostilities"),
        ("settlement", "settlement"),
        ("reconciliation", "settlement"),
        ("treaty|accord|pact", "legal_instrument"),
    ]
    for p, cat in english:
        terms.append(Term(name=p, category=cat, pattern=re.compile(rf"\b{p}\b", re.IGNORECASE)))

    return terms


# ----------------------------- Extraction ----------------------------- #

def read_pdf_pages(path: Path) -> List[str]:
    """Extrahiert Text seitenweise aus PDF. Nutzt pypdf oder PyPDF2. Wirft Exception bei Totalfehlschlag."""
    if _PYPDF_AVAILABLE:
        reader = pypdf.PdfReader(str(path))
        pages = []
        for i, page in enumerate(reader.pages):
            try:
                pages.append(page.extract_text() or "")
            except Exception as e:
                print(f"[DEBUG] PDF-Seite {i+1} von {path} nicht extrahierbar: {e}")
                pages.append("")
        return pages
    elif _PYPDF2_AVAILABLE:
        reader = PyPDF2.PdfReader(str(path))  # type: ignore
        pages = []
        for i, page in enumerate(reader.pages):  # type: ignore
            try:
                pages.append(page.extract_text() or "")
            except Exception as e:
                print(f"[DEBUG] PDF-Seite {i+1} von {path} nicht extrahierbar: {e}")
                pages.append("")
        return pages
    else:
        raise RuntimeError("Keine PDF-Bibliothek (pypdf/PyPDF2) verfügbar.")


def read_txt(path: Path) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def read_docx(path: Path) -> str:
    if not _DOCX_AVAILABLE:
        raise RuntimeError("python-docx nicht installiert.")
    d = docx.Document(str(path))  # type: ignore
    return "\n".join(p.text for p in d.paragraphs)


def normalize(s: str) -> str:
    """Normiert für Matches: Kleinschreibung, ß->ss, Mehrfachwhitespace reduzieren."""
    s2 = s.casefold().replace("ß", "ss")
    s2 = re.sub(r"\s+", " ", s2)
    return s2


def find_matches_in_text(
    terms: List[Term], original_text: str, kwic_chars: int
) -> Tuple[int, Dict[str, int], Dict[str, int], List[Sample]]:
    """Findet Matches und erzeugt KWIC-Beispiele im Originaltext."""
    text_norm = normalize(original_text)
    hits_total = 0
    hits_by_term: Dict[str, int] = {}
    hits_by_category: Dict[str, int] = {c: 0 for c in CATEGORIES}
    samples: List[Sample] = []

    # Für Kontextprüfung: list of start indices for contexts
    for t in terms:
        for m in t.pattern.finditer(text_norm):
            start, end = m.span()
            snippet_norm = text_norm[max(0, start - kwic_chars): min(len(text_norm), end + kwic_chars)]
            if t.requires_context:
                if not any(ctx.search(snippet_norm) for ctx in t.context_terms):
                    continue  # Kontext fehlt → nicht zählen
            hits_total += 1
            hits_by_term[t.name] = hits_by_term.get(t.name, 0) + 1
            hits_by_category[t.category] += 1

            # KWIC im Originaltext ausgeben
            start_orig = max(0, start - kwic_chars)
            end_orig = min(len(original_text), end + kwic_chars)
            snippet_orig = original_text[start_orig:end_orig]

            # Match hervorheben (robust, ohne Original-Groß/klein zu verlieren)
            before = original_text[start_orig:start]
            match_text = original_text[start:end]
            after = original_text[end:end_orig]
            highlighted = f"{before}[[{match_text}]]{after}"

            # Begrenze Beispielanzahl pro Term grob
            if sum(1 for s in samples if s.term == t.name) < 5:
                samples.append(Sample(term=t.name, category=t.category, page=None, kwic=highlighted))

    return hits_total, hits_by_term, hits_by_category, samples


def analyze_file(path: Path, terms: List[Term], kwic_chars: int) -> FileResult:
    """Analysiert eine Datei. PDF seitenweise, andere als Ganzes."""
    ext = path.suffix.lower()
    try:
        if ext == ".pdf":
            pages = read_pdf_pages(path)
            hits_total = 0
            hits_by_term: Dict[str, int] = {}
            hits_by_category: Dict[str, int] = {c: 0 for c in CATEGORIES}
            samples: List[Sample] = []

            for pi, page_text in enumerate(pages, start=1):
                if not page_text:
                    continue
                ht, hbt, hbc, smp = find_matches_in_text(terms, page_text, kwic_chars)
                hits_total += ht
                # merge dicts
                for k, v in hbt.items():
                    hits_by_term[k] = hits_by_term.get(k, 0) + v
                for k, v in hbc.items():
                    hits_by_category[k] = hits_by_category.get(k, 0) + v
                # annotate page for samples
                for s in smp:
                    s.page = pi
                samples.extend(smp)

            return FileResult(
                path=str(path), file_type="pdf", pages=len(pages),
                hits_total=hits_total, hits_by_category=hits_by_category,
                hits_by_term=hits_by_term, samples=samples
            )

        elif ext == ".txt":
            text = read_txt(path)
            ht, hbt, hbc, smp = find_matches_in_text(terms, text, kwic_chars)
            return FileResult(
                path=str(path), file_type="txt", pages=None,
                hits_total=ht, hits_by_category=hbc, hits_by_term=hbt, samples=smp
            )

        elif ext == ".docx":
            if not _DOCX_AVAILABLE:
                raise RuntimeError("DOCX-Datei, aber python-docx nicht verfügbar.")
            text = read_docx(path)
            ht, hbt, hbc, smp = find_matches_in_text(terms, text, kwic_chars)
            return FileResult(
                path=str(path), file_type="docx", pages=None,
                hits_total=ht, hits_by_category=hbc, hits_by_term=hbt, samples=smp
            )

        else:
            raise RuntimeError(f"Nicht unterstützte Dateiendung: {ext}")

    except Exception as e:
        print(f"[DEBUG] Fehler bei {path}: {e}")
        return FileResult(
            path=str(path), file_type=ext.strip("."), pages=None, hits_total=0,
            hits_by_category={c: 0 for c in CATEGORIES}, hits_by_term={},
            samples=[], error=str(e)
        )


# ----------------------------- Session Logic ----------------------------- #

class Analyzer:
    def __init__(self, library_dir: Path, output_base: Path, require_context_amb: bool, include_docx: bool, kwic_chars: int):
        self.library_dir = library_dir
        self.output_base = output_base
        self.require_context_amb = require_context_amb
        self.include_docx = include_docx
        self.kwic_chars = kwic_chars
        self.terms = build_lexicon(require_context_for_ambiguous=require_context_amb)
        self.results: List[FileResult] = []
        self.errors: List[str] = []
        self.started_at = None
        self.finished_at = None

    def eligible(self, path: Path) -> bool:
        if path.is_dir():
            return path.name.lower() != "chroniken"
        if path.is_file():
            ext = path.suffix.lower()
            if ext in [".pdf", ".txt"]:
                return True
            if ext == ".docx" and self.include_docx and _DOCX_AVAILABLE:
                return True
        return False

    def iter_files(self) -> List[Path]:
        files: List[Path] = []
        for root, dirs, filenames in os.walk(self.library_dir):
            # Verzeichnisse "chroniken" ausschließen
            dirs[:] = [d for d in dirs if d.lower() != "chroniken"]
            for fn in filenames:
                p = Path(root) / fn
                ext = p.suffix.lower()
                if ext in [".pdf", ".txt"] or (ext == ".docx" and self.include_docx and _DOCX_AVAILABLE):
                    files.append(p)
        return files

    def run(self, progress_cb: Optional[Any] = None) -> None:
        self.started_at = datetime.utcnow().isoformat() + "Z"
        files = self.iter_files()
        total = len(files)
        print(f"[DEBUG] Starte Scan. Dateien: {total}. Library: {self.library_dir}")
        self.results.clear()
        self.errors.clear()

        for idx, fp in enumerate(files, start=1):
            res = analyze_file(fp, self.terms, self.kwic_chars)
            if res.error:
                self.errors.append(f"{fp}: {res.error}")
            self.results.append(res)
            if progress_cb:
                progress_cb(idx, total)
        self.finished_at = datetime.utcnow().isoformat() + "Z"
        print(f"[DEBUG] Scan abgeschlossen. Ergebnisse: {len(self.results)} Dateien.")

    def to_json(self, session_name: str) -> Dict[str, Any]:
        totals_by_category: Dict[str, int] = {c: 0 for c in CATEGORIES}
        term_totals: Dict[str, int] = {}
        file_totals: List[Tuple[str, int]] = []

        for r in self.results:
            file_totals.append((r.path, r.hits_total))
            for c, v in r.hits_by_category.items():
                totals_by_category[c] = totals_by_category.get(c, 0) + v
            for t, v in r.hits_by_term.items():
                term_totals[t] = term_totals.get(t, 0) + v

        top_terms = sorted(term_totals.items(), key=lambda x: x[1], reverse=True)[:20]
        top_files = sorted(file_totals, key=lambda x: x[1], reverse=True)[:20]

        data = {
            "session": {
                "name": session_name,
                "created_at": datetime.utcnow().isoformat() + "Z",
            },
            "config": {
                "library_dir": str(self.library_dir),
                "output_base": str(self.output_base),
                "require_context_for_ambiguous": self.require_context_amb,
                "include_docx": self.include_docx,
                "kwic_chars": self.kwic_chars,
                "versions": {
                    "pypdf": _PYPDF_AVAILABLE,
                    "PyPDF2": _PYPDF2_AVAILABLE,
                    "python_docx": _DOCX_AVAILABLE
                }
            },
            "lexicon": [
                {
                    "name": t.name,
                    "category": t.category,
                    "pattern": t.pattern.pattern,
                    "requires_context": t.requires_context,
                    "context_terms": [c.pattern for c in t.context_terms]
                } for t in self.terms
            ],
            "scan": {
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "source_dir": str(self.library_dir),
                "excluded_dirs": ["chroniken"],
                "files_scanned": len(self.results),
                "files_with_hits": sum(1 for r in self.results if r.hits_total > 0),
                "errors": self.errors
            },
            "files": [
                {
                    "path": r.path,
                    "type": r.file_type,
                    "pages": r.pages,
                    "hits_total": r.hits_total,
                    "hits_by_category": r.hits_by_category,
                    "hits_by_term": r.hits_by_term,
                    "samples": [
                        {"term": s.term, "category": s.category, "page": s.page, "kwic": s.kwic}
                        for s in r.samples
                    ],
                } for r in self.results
            ],
            "summary": {
                "totals_by_category": totals_by_category,
                "top_terms": top_terms,
                "top_files": top_files
            }
        }
        return data


# ----------------------------- GUI ----------------------------- #

class App(ttk.Frame):
    def __init__(self, master: tk.Tk):
        super().__init__(master)
        master.title("AZK – Friedenssignale-Scanner (Sekundärliteratur)")
        master.geometry("1100x700")
        self.pack(fill="both", expand=True)

        # State
        self.library_var = tk.StringVar(value=DEFAULT_LIBRARY)
        self.output_var = tk.StringVar(value=DEFAULT_OUTPUT_BASE)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_var = tk.StringVar(value=f"session_{ts}")
        self.kwic_var = tk.IntVar(value=80)
        self.docx_var = tk.BooleanVar(value=False)
        self.amb_ctx_var = tk.BooleanVar(value=True)

        self.analyzer: Optional[Analyzer] = None
        self.current_results: List[FileResult] = []

        self._build_ui()

    def _build_ui(self) -> None:
        # Top controls
        top = ttk.Frame(self)
        top.pack(side="top", fill="x", padx=8, pady=8)

        ttk.Label(top, text="Library:").grid(row=0, column=0, sticky="w")
        lib_entry = ttk.Entry(top, textvariable=self.library_var, width=80)
        lib_entry.grid(row=0, column=1, sticky="we", padx=4)
        ttk.Button(top, text="…", command=self._choose_library).grid(row=0, column=2, padx=2)

        ttk.Label(top, text="Output:").grid(row=1, column=0, sticky="w")
        out_entry = ttk.Entry(top, textvariable=self.output_var, width=80)
        out_entry.grid(row=1, column=1, sticky="we", padx=4)
        ttk.Button(top, text="…", command=self._choose_output).grid(row=1, column=2, padx=2)

        ttk.Label(top, text="Session:").grid(row=2, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.session_var, width=40).grid(row=2, column=1, sticky="w", padx=4)

        ttk.Label(top, text="KWIC-Zeichen:").grid(row=0, column=3, sticky="e")
        ttk.Entry(top, textvariable=self.kwic_var, width=6).grid(row=0, column=4, sticky="w")

        ttk.Checkbutton(top, text="DOCX einbeziehen", variable=self.docx_var).grid(row=1, column=3, columnspan=2, sticky="w")
        ttk.Checkbutton(top, text="Kontextpflicht für ambige Terme", variable=self.amb_ctx_var).grid(row=2, column=3, columnspan=2, sticky="w")

        self.scan_btn = ttk.Button(top, text="Scan starten", command=self._start_scan)
        self.scan_btn.grid(row=0, column=5, rowspan=2, padx=8, sticky="ns")

        self.export_btn = ttk.Button(top, text="JSON exportieren", command=self._export_json, state="disabled")
        self.export_btn.grid(row=2, column=5, padx=8, sticky="e")

        # Progress
        prog = ttk.Frame(self)
        prog.pack(fill="x", padx=8)
        self.pb = ttk.Progressbar(prog, orient="horizontal", mode="determinate")
        self.pb.pack(fill="x")
        self.status_var = tk.StringVar(value="Bereit.")
        ttk.Label(prog, textvariable=self.status_var).pack(anchor="w")

        # Split
        split = ttk.Panedwindow(self, orient="horizontal")
        split.pack(fill="both", expand=True, padx=8, pady=8)

        # Left: table
        left = ttk.Frame(split)
        cols = ("file", "hits", "core_peace", "truce", "settlement", "legal_instrument", "end_of_hostilities")
        self.tree = ttk.Treeview(left, columns=cols, show="headings", height=20)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=150 if c == "file" else 110, anchor="center")
        self.tree.pack(fill="both", expand=True)
        self.tree.bind("<<TreeviewSelect>>", self._on_select_row)
        split.add(left, weight=3)

        # Right: details
        right = ttk.Frame(split)
        ttk.Label(right, text="Details & KWIC (max 5 pro Term):").pack(anchor="w")
        self.detail = tk.Text(right, height=20, wrap="word")
        self.detail.pack(fill="both", expand=True)
        split.add(right, weight=2)

        # Footer
        foot = ttk.Frame(self)
        foot.pack(fill="x", padx=8, pady=6)
        ttk.Button(foot, text="Ausgabeordner öffnen", command=self._open_output).pack(side="right")

        for i in range(6):
            top.grid_columnconfigure(i, weight=1)

    # UI helpers
    def _choose_library(self) -> None:
        d = filedialog.askdirectory(initialdir=self.library_var.get(), title="Sekundärliteratur-Ordner wählen")
        if d:
            self.library_var.set(d)

    def _choose_output(self) -> None:
        d = filedialog.askdirectory(initialdir=self.output_var.get(), title="Output-Basisordner wählen")
        if d:
            self.output_var.set(d)

    def _update_progress(self, done: int, total: int) -> None:
        self.pb["maximum"] = max(total, 1)
        self.pb["value"] = done
        self.status_var.set(f"Verarbeitet: {done}/{total}")
        self.update_idletasks()

    def _start_scan(self) -> None:
        # Validate paths
        lib = Path(self.library_var.get())
        out = Path(self.output_var.get())
        if not lib.exists() or not lib.is_dir():
            messagebox.showerror("Fehler", f"Library-Pfad existiert nicht:\n{lib}")
            return
        out.mkdir(parents=True, exist_ok=True)

        self.scan_btn.config(state="disabled")
        self.export_btn.config(state="disabled")
        self.pb["value"] = 0
        self.status_var.set("Scan läuft…")
        self.detail.delete("1.0", "end")
        self.tree.delete(*self.tree.get_children())

        require_ctx = self.amb_ctx_var.get()
        include_docx = self.docx_var.get()
        kwic_chars = int(self.kwic_var.get())

        self.analyzer = Analyzer(lib, out, require_ctx, include_docx, kwic_chars)

        def worker():
            print("[DEBUG] Worker-Thread gestartet.")
            start = time.time()
            try:
                self.analyzer.run(progress_cb=lambda d, t: self.after(0, self._update_progress, d, t))
            except Exception as e:
                self.after(0, messagebox.showerror, "Fehler", str(e))
            duration = time.time() - start
            print(f"[DEBUG] Worker-Thread fertig. Dauer: {duration:.2f}s")
            self.after(0, self._scan_finished)

        threading.Thread(target=worker, daemon=True).start()

    def _scan_finished(self) -> None:
        self.status_var.set("Scan abgeschlossen.")
        self.export_btn.config(state="normal")
        self.scan_btn.config(state="normal")
        if not self.analyzer:
            return
        self.current_results = self.analyzer.results

        # Fill table
        for r in self.current_results:
            vals = (
                Path(r.path).name,
                r.hits_total,
                r.hits_by_category.get("core_peace", 0),
                r.hits_by_category.get("truce", 0),
                r.hits_by_category.get("settlement", 0),
                r.hits_by_category.get("legal_instrument", 0),
                r.hits_by_category.get("end_of_hostilities", 0),
            )
            self.tree.insert("", "end", iid=r.path, values=vals)

        # Details summary
        total_docs = len(self.current_results)
        with_hits = sum(1 for r in self.current_results if r.hits_total > 0)
        self.detail.insert("end", f"Dokumente gesamt: {total_docs}\n")
        self.detail.insert("end", f"Dokumente mit Treffern: {with_hits}\n\n")
        if self.analyzer.errors:
            self.detail.insert("end", "Fehler:\n")
            for e in self.analyzer.errors[:10]:
                self.detail.insert("end", f" - {e}\n")
            if len(self.analyzer.errors) > 10:
                self.detail.insert("end", f" ... weitere {len(self.analyzer.errors)-10} Fehler gekürzt ...\n")

    def _on_select_row(self, _event=None) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        path = sel[0]
        r = next((x for x in self.current_results if x.path == path), None)
        if not r:
            return
        self.detail.delete("1.0", "end")
        self.detail.insert("end", f"Datei: {r.path}\nTyp: {r.file_type}  Seiten: {r.pages or '-'}\n")
        self.detail.insert("end", f"Treffer gesamt: {r.hits_total}\n\n")
        self.detail.insert("end", "Treffer nach Kategorie:\n")
        for c in CATEGORIES:
            self.detail.insert("end", f"  - {c}: {r.hits_by_category.get(c, 0)}\n")
        self.detail.insert("end", "\nTop Terme:\n")
        for term, cnt in sorted(r.hits_by_term.items(), key=lambda x: x[1], reverse=True)[:20]:
            self.detail.insert("end", f"  - {term}: {cnt}\n")
        self.detail.insert("end", "\nKWIC-Beispiele:\n")
        for s in r.samples[:50]:
            page = f" (S. {s.page})" if s.page else ""
            self.detail.insert("end", f"[{s.category}] {s.term}{page}: {s.kwic}\n---\n")

    def _export_json(self) -> None:
        if not self.analyzer:
            messagebox.showerror("Fehler", "Keine Ergebnisse vorhanden.")
            return
        session_name = self.session_var.get().strip() or f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        safe_name = re.sub(r"[^A-Za-z0-9_\-\.]+", "_", session_name)
        out_dir = Path(self.output_var.get()) / safe_name
        out_dir.mkdir(parents=True, exist_ok=True)

        data = self.analyzer.to_json(session_name=safe_name)
        out_path = out_dir / "results.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self.status_var.set(f"JSON exportiert: {out_path}")
        messagebox.showinfo("Export", f"JSON gespeichert:\n{out_path}")

    def _open_output(self) -> None:
        path = Path(self.output_var.get())
        if not path.exists():
            messagebox.showerror("Fehler", f"Ordner nicht gefunden:\n{path}")
            return
        system = platform.system()
        try:
            if system == "Darwin":
                os.system(f'open "{path}"')
            elif system == "Windows":
                os.startfile(str(path))  # type: ignore
            else:
                os.system(f'xdg-open "{path}"')
        except Exception as e:
            messagebox.showerror("Fehler", f"Konnte Ordner nicht öffnen: {e}")


def main() -> None:
    print("[DEBUG] Programmstart")
    print(f"[DEBUG] PDF-Libs: pypdf={_PYPDF_AVAILABLE} PyPDF2={_PYPDF2_AVAILABLE} DOCX={_DOCX_AVAILABLE}")
    root = tk.Tk()
    style = ttk.Style()
    try:
        style.theme_use("clam")
    except Exception:
        pass
    app = App(root)
    root.mainloop()
    print("[DEBUG] Programmende")


if __name__ == "__main__":
    main()