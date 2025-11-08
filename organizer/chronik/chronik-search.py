"""
chroniken_library-Finder – robuste Regex-Kompilierung für dein Projektlayout.

Zweck:
- PDFs aus <PROJECT_ROOT>/data/azk_library (oder GUI-Fallback) scannen.
- Canon/Patterns aus <PROJECT_ROOT>/config/chroniken_canon.json laden.
- Volltext + OCR-Fallback durchsuchen (Works, Serien, generische Gattungsbegriffe).
- Ausgaben nach <PROJECT_ROOT>/data/chronik_data/session_YYYYMMDD_HHMMSS_<hash>/:
  - chroniken_mentions.csv, chroniken_summary.csv, chroniken_report.html, chroniken_network.gexf*, session_meta.json
    *nur wenn networkx verfügbar ist.

Wichtiges Fix:
- Flexible Leerzeichenersetzung erzeugte zuvor ungültige Regex ("\s" im Replacement-Kontext).
- Jetzt wird mit einer Replacement-Funktion gearbeitet und NBSP als echtes Zeichen eingefügt.
"""

from __future__ import annotations

import io
import json
import html
import hashlib
import os
import re
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

# Drittanbieter
try:
    import fitz  # PyMuPDF
except Exception as e:
    raise RuntimeError("PyMuPDF (fitz) ist erforderlich. Installation: pip install pymupdf") from e

try:
    import pandas as pd
    HAVE_PANDAS = True
except Exception:
    HAVE_PANDAS = False

try:
    import pytesseract
    HAVE_TESS = True
except Exception:
    HAVE_TESS = False

try:
    import networkx as nx
    HAVE_NX = True
except Exception:
    HAVE_NX = False

# GUI-Fallback
try:
    import tkinter as tk
    from tkinter import filedialog
    HAVE_TK = True
except Exception:
    HAVE_TK = False


# ---------------------- Parameter ----------------------

TEXT_MIN_LEN: int = 100
SNIPPET_LEN: int = 140
BIB_HEADINGS: List[str] = [
    r"^\s*literatur\s*$", r"^\s*bibliographi[ea]\s*$", r"^\s*literaturverzeichnis\s*$",
    r"^\s*references\s*$", r"^\s*bibliography\s*$", r"^\s*quellen\s*(und\s*literatur)?\s*$",
]


# ---------------------- Modelle ----------------------

@dataclass(frozen=True)
class PatternEntry:
    label: str
    group: str         # "work" | "series" | "generic"
    regex: re.Pattern

@dataclass
class Hit:
    pdf_path: str
    page: int
    group: str
    label: str
    pattern: str
    context: str


# ---------------------- Pfade ----------------------

def project_root() -> Path:
    here = Path(__file__).resolve()
    try:
        return here.parents[2]
    except IndexError:
        return here.parent.parent

def default_pdf_dir(root: Path) -> Path:
    return root / "data" / "azk_library"

def config_path(root: Path) -> Path:
    return root / "config" / "chroniken_canon.json"


# ---------------------- Text/Regex Utils ----------------------

def normspace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def normalize_text(txt: str) -> str:
    if not txt:
        return ""
    txt = txt.replace("\u00AD", "")                  # Soft Hyphen
    txt = txt.replace("ﬁ", "fi").replace("ﬂ", "fl")  # Ligaturen
    txt = re.sub(r"([A-Za-zÄÖÜäöüß]{2,})-\s*\n\s*([a-zäöüß]{2,})", r"\1\2", txt)  # Dehyphenation
    txt = re.sub(r"\r\n?|\n", " ", txt)
    return normspace(txt)

def compile_bib_heading_patterns() -> List[re.Pattern]:
    return [re.compile(p, re.IGNORECASE | re.MULTILINE) for p in BIB_HEADINGS]

def detect_bibliography_pages(doc: "fitz.Document") -> Set[int]:
    pats = compile_bib_heading_patterns()
    bib_pages: Set[int] = set()
    start_seen = False
    for i in range(doc.page_count):
        raw = (doc.load_page(i).get_text("text") or "")
        txt = normalize_text(raw)
        if not start_seen and any(p.search(txt) for p in pats):
            start_seen = True
        if start_seen:
            bib_pages.add(i)
    return bib_pages

def extract_text(page: "fitz.Page") -> str:
    txt = page.get_text("text") or ""
    if len(txt) >= TEXT_MIN_LEN or not HAVE_TESS:
        return normalize_text(txt)
    try:
        pix = page.get_pixmap(dpi=300)
        import PIL.Image  # pillow via pytesseract
        img = PIL.Image.open(io.BytesIO(pix.tobytes("png")))
        ocr = pytesseract.image_to_string(img, lang="deu+eng")
        return normalize_text(ocr or txt)
    except Exception:
        return normalize_text(txt)

def iter_pdfs(root: Path) -> Iterable[Path]:
    yield from root.rglob("*.pdf")


# ---------------------- Canon laden/kompilieren ----------------------

def load_config(cfg_path: Path) -> Dict:
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)

def _flexify_spaces(alias_pat: str) -> str:
    """
    Erweitert einfache Leerzeichen im Alias zu einer flexiblen Klasse:
      [\s NBSP -]+
    Wichtig: Replacement-Funktion verwenden, damit Backslashes NICHT als Backrefs interpretiert werden.
    """
    nbsp = "\u00A0"                # echtes NBSP-Zeichen
    def repl(_m: re.Match) -> str:
        return f"[\\s{nbsp}\\-]+"
    return re.sub(r"\s+", repl, alias_pat)

def compile_patterns(cfg: Dict) -> Tuple[List[PatternEntry], Dict[str, float]]:
    compiled: List[PatternEntry] = []
    weights = cfg.get("weights", {"work": 1.0, "series": 1.0, "generic": 1.0})

    def add_block(items: List[Dict], group: str) -> None:
        for it in items:
            label = it.get("canonical") or it.get("label") or "generic"
            raw_list = it.get("aliases") or it.get("patterns") or []
            for raw in raw_list:
                try:
                    pat = _flexify_spaces(raw)
                    rgx = re.compile(pat, re.IGNORECASE)
                    compiled.append(PatternEntry(label=label, group=group, regex=rgx))
                except re.error as e:
                    # Rohmuster anzeigen, damit die JSON-Quelle leicht korrigiert werden kann.
                    print(f"[WARN] Ungültiges Regex ignoriert ({group}:{label}): {raw} ({e})")

    add_block(cfg.get("works", []), "work")
    add_block(cfg.get("series", []), "series")
    add_block(cfg.get("generic_terms", []), "generic")

    print(f"[INFO] Muster geladen: works={len(cfg.get('works', []))}, series={len(cfg.get('series', []))}, generics={len(cfg.get('generic_terms', []))}")
    print(f"[INFO] Kompilierte Regex: {len(compiled)} | Gewichte: {weights}")
    return compiled, weights


# ---------------------- Scan/Aggregation ----------------------

@dataclass
class ScanResult:
    hits: List[Hit]

def scan_pdf(pdf_path: Path, patterns: List[PatternEntry]) -> List[Hit]:
    hits: List[Hit] = []
    try:
        doc = fitz.open(str(pdf_path))
    except Exception as e:
        print(f"[WARN] Konnte PDF nicht öffnen: {pdf_path} ({e})")
        return hits
    try:
        for pidx in range(doc.page_count):
            page = doc.load_page(pidx)
            txt = extract_text(page)
            if not txt:
                continue
            for pe in patterns:
                for m in pe.regex.finditer(txt):
                    s, e = m.span()
                    ctx = make_context(txt, (s, e))
                    hits.append(Hit(
                        pdf_path=str(pdf_path),
                        page=pidx + 1,
                        group=pe.group,
                        label=pe.label,
                        pattern=pe.regex.pattern,
                        context=ctx
                    ))
    finally:
        doc.close()
    return hits

def make_context(text: str, span: Tuple[int, int], length: int = SNIPPET_LEN) -> str:
    s, e = span
    mid = (s + e) // 2
    start = max(0, mid - length // 2)
    end = min(len(text), start + length)
    return normspace(text[start:end])

def aggregate(hits: List[Hit], weights: Dict[str, float]):
    if not HAVE_PANDAS:
        raise RuntimeError("pandas ist erforderlich. Installation: pip install pandas")
    rows = [{
        "pdf_file": h.pdf_path, "page": h.page, "group": h.group, "label": h.label,
        "pattern": h.pattern, "context": h.context
    } for h in hits]
    df = pd.DataFrame(rows)
    agg = df.groupby(["group", "label"]).agg(
        mentions=("pdf_file", "count"),
        docs=("pdf_file", lambda s: len(set(s)))
    ).reset_index()
    agg["weight"] = agg["group"].map(lambda g: float(weights.get(g, 1.0)))
    agg["weighted_mentions"] = (agg["mentions"] * agg["weight"]).round(3)
    return df, agg

def build_co_mention_network(df):
    if not HAVE_NX or df.empty:
        return None
    dfw = df[df["group"] == "work"].copy()
    if dfw.empty:
        return None
    edges: Dict[Tuple[str, str], int] = {}
    for _, sub in dfw.groupby("pdf_file"):
        labels = sorted(set(sub["label"]))
        for i in range(len(labels)):
            for j in range(i + 1, len(labels)):
                key = (labels[i], labels[j])
                edges[key] = edges.get(key, 0) + 1
    G = nx.Graph()
    for (a, b), w in edges.items():
        G.add_edge(a, b, weight=float(w))
    return G


# ---------------------- Ausgabe ----------------------

def ensure_session_dir(root: Path, pdfs: List[Path]) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    h = hashlib.sha1("|".join(sorted(p.name for p in pdfs)).encode("utf-8")).hexdigest()[:8]
    out = root / "data" / "chronik_data" / f"session_{ts}_{h}"
    out.mkdir(parents=True, exist_ok=True)
    return out

def write_outputs(out_dir: Path, df, agg) -> Tuple[Path, Path]:
    det = out_dir / "chroniken_mentions.csv"
    smy = out_dir / "chroniken_summary.csv"
    df.to_csv(det, index=False, sep=";")
    agg.sort_values(["group", "weighted_mentions"], ascending=[True, False]).to_csv(smy, index=False, sep=";")
    print(f"[INFO] CSV geschrieben: {det}")
    print(f"[INFO] CSV geschrieben: {smy}")
    return det, smy

def _html_mark(text: str, patterns: List[str]) -> str:
    esc = html.escape(text, quote=True)
    out = esc
    for p in patterns[:3]:
        try:
            rgx = re.compile(p, re.IGNORECASE)
            out = rgx.sub(lambda m: f"<mark>{html.escape(m.group(0))}</mark>", out, count=1)
        except re.error:
            continue
    return out

def write_html(out_dir: Path, df, agg) -> Path:
    def esc(x): return html.escape(str(x), quote=True)
    chips = []
    for _, r in agg.sort_values("weighted_mentions", ascending=False).iterrows():
        chips.append(f"<span class='chip {esc(r['group'])}'>{esc(r['label'])}&nbsp;&nbsp;{int(r['mentions'])} ({r['weighted_mentions']})</span>")
    rows = []
    for _, r in agg.sort_values(["group", "weighted_mentions"], ascending=[True, False]).iterrows():
        rows.append("<tr>"
                    f"<td>{esc(r['group'])}</td>"
                    f"<td>{esc(r['label'])}</td>"
                    f"<td>{int(r['mentions'])}</td>"
                    f"<td>{int(r['docs'])}</td>"
                    f"<td>{r['weighted_mentions']}</td>"
                    "</tr>")
    details = []
    if len(df) > 0:
        sample = df.sample(min(40, len(df)), random_state=0)
        for _, r in sample.iterrows():
            details.append("<tr>"
                           f"<td>{esc(Path(r['pdf_file']).name)}</td>"
                           f"<td>{int(r['page'])}</td>"
                           f"<td>{esc(r['group'])}</td>"
                           f"<td>{esc(r['label'])}</td>"
                           f"<td><code>{esc(r['pattern'])}</code></td>"
                           f"<td>{_html_mark(str(r['context']), [str(r['pattern'])])}</td>"
                           "</tr>")

    html_parts = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        "<title>chroniken_library – Volltext-Erwähnungen</title>",
        "<style>",
        "body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:24px}",
        "h1{margin-top:0} table{border-collapse:collapse;width:100%}",
        "th,td{border:1px solid #ddd;padding:8px;vertical-align:top}",
        "th{background:#f5f5f5;text-align:left}", ".chips{margin:8px 0}",
        ".chip{display:inline-block;background:#eef;border:1px solid #ccd;padding:2px 8px;border-radius:12px;margin:2px;font-size:12px}",
        ".chip.work{background:#eef} .chip.series{background:#efe} .chip.generic{background:#fee}",
        ".small{color:#666;font-size:12px}",
        "</style></head><body>",
        "<h1>chroniken_library – Volltext-Erwähnungen</h1>",
        f"<p class='small'>Treffer: <b>{len(df)}</b> | Labels: <b>{len(set(agg['label']))}</b> | Dateien: <b>{len(set(df['pdf_file']))}</b></p>",
        "<div class='chips'>", *chips, "</div>",
        "<h2>Aggregat</h2>",
        "<table><thead><tr><th>Gruppe</th><th>Label</th><th>Mentions</th><th>Dokumente</th><th>Weighted</th></tr></thead><tbody>",
        *rows, "</tbody></table>",
        "<h2>Detailstichprobe</h2>",
        "<table><thead><tr><th>Datei</th><th>Seite</th><th>Gruppe</th><th>Label</th><th>Muster</th><th>Kontext</th></tr></thead><tbody>",
        *details, "</tbody></table>",
        "</body></html>"
    ]
    path = out_dir / "chroniken_report.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(html_parts))
    print(f"[INFO] HTML geschrieben: {path}")
    return path

def write_meta(out_dir: Path, root: Path, pdf_dir: Path, pdfs: List[Path], cfg_file: Path, weights: Dict[str, float]) -> Path:
    meta = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(root),
        "pdf_dir": str(pdf_dir),
        "num_pdfs": len(pdfs),
        "pdfs": [str(p.relative_to(root)) if str(p).startswith(str(root)) else str(p) for p in pdfs],
        "config_file": str(cfg_file),
        "weights": {str(k): float(v) for k, v in weights.items()},
        "env": {
            "pymupdf": True,
            "pandas": HAVE_PANDAS,
            "pytesseract": HAVE_TESS,
            "networkx": HAVE_NX,
            "ocr_threshold_chars": int(TEXT_MIN_LEN)
        }
    }
    out = out_dir / "session_meta.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    print(f"[INFO] Session-Metadaten geschrieben: {out}")
    return out


# ---------------------- Main ----------------------

def pick_pdf_dir(std: Path) -> Path:
    if std.exists():
        print(f"[INFO] PDF-Ordner: {std}")
        return std
    if HAVE_TK:
        root = tk.Tk(); root.withdraw(); root.update()
        folder = filedialog.askdirectory(title="PDF-Ordner wählen")
        root.destroy()
        if folder:
            p = Path(folder).resolve()
            print(f"[INFO] PDF-Ordner (GUI): {p}")
            return p
    print(f"[WARN] Standardordner nicht gefunden. Fallback: aktuelles Verzeichnis.")
    return Path.cwd()

def print_env() -> None:
    print("[INFO] Bibliotheken:")
    print("  PyMuPDF: OK")
    print(f"  pandas: {'OK' if HAVE_PANDAS else 'NEIN'}")
    print(f"  pytesseract: {'OK' if HAVE_TESS else 'NEIN'}")
    print(f"  networkx: {'OK' if HAVE_NX else 'NEIN'}")
    print(f"[INFO] OCR-Fallback aktiv bei < {TEXT_MIN_LEN} Zeichen pro Seite.")

def main() -> None:
    print("[INFO] chroniken_library-Finder startet…")
    print_env()

    root = project_root()
    std_pdf_dir = default_pdf_dir(root)
    cfg_file = config_path(root)

    try:
        cfg = load_config(cfg_file)
    except Exception as e:
        print(f"[ERROR] Konnte Config nicht laden: {cfg_file} ({e})")
        return

    patterns, weights = compile_patterns(cfg)
    if not patterns:
        print("[ERROR] Keine gültigen Muster. Prüfe config/chroniken_canon.json.")
        return

    pdf_dir = pick_pdf_dir(std_pdf_dir)
    pdfs = list(iter_pdfs(pdf_dir))
    if not pdfs:
        print("[WARN] Keine PDFs gefunden.")
        return

    session_dir = ensure_session_dir(root, pdfs)
    print(f"[INFO] Session-Ordner: {session_dir}")

    all_hits: List[Hit] = []
    for i, pdf in enumerate(pdfs, 1):
        print(f"[INFO] ({i}/{len(pdfs)}) Verarbeite: {pdf}")
        try:
            hs = scan_pdf(pdf, patterns)
            print(f"       Treffer: {len(hs)}")
            all_hits.extend(hs)
        except Exception as e:
            print(f"[ERROR] Fehler bei {pdf}: {e}")
            traceback.print_exc()

    if not HAVE_PANDAS:
        print("[ERROR] pandas fehlt. Installiere mit: pip install pandas")
        return

    # leichte Dubletten filtern
    dedup: List[Hit] = []
    seen: Set[Tuple[str, int, str, str, str]] = set()
    for h in all_hits:
        key = (h.pdf_path, h.page, h.group, h.label, h.context)
        if key not in seen:
            seen.add(key)
            dedup.append(h)

    df, agg = aggregate(dedup, weights)
    write_meta(session_dir, root, pdf_dir, pdfs, cfg_file, weights)
    write_outputs(session_dir, df, agg)
    if HAVE_NX:
        G = build_co_mention_network(df)
        if G is not None:
            path = session_dir / "chroniken_network.gexf"
            nx.write_gexf(G, str(path))
            print(f"[INFO] GEXF geschrieben: {path}")
    write_html(session_dir, df, agg)

    print("[INFO] Top-Labels nach Weighted:")
    for _, r in agg.sort_values("weighted_mentions", ascending=False).head(15).iterrows():
        print(f"  [{r['group']}] {r['label']}: mentions={r['mentions']} docs={r['docs']} weighted={r['weighted_mentions']}")
    print("[INFO] Fertig.")


if __name__ == "__main__":
    main()