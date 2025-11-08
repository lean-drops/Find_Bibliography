"""
Utility-Funktionen und Modelle für Autor↔Autor-Referenznetz.

Usage:
  Wird von authors_search.py und authors_gui.py importiert.
  Nicht direkt ausführen.

Benötigt:
  - Python 3.9+
  - PyMuPDF (fitz)
  - pandas (für Aggregation/Export)
  - optional: pytesseract (+ Tesseract), pillow
  - optional: networkx
"""
from __future__ import annotations

import os
import re
import io
import json
import html
import hashlib
import unicodedata
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Iterable, Set
from datetime import datetime

# Drittanbieter
try:
    import fitz  # PyMuPDF
except Exception as e:
    raise RuntimeError("PyMuPDF (fitz) ist erforderlich. Installation: pip install pymupdf") from e

# Flags erst lazy in Funktionen geprüft
try:
    import pandas as pd  # noqa
    HAVE_PANDAS = True
except Exception:
    HAVE_PANDAS = False

try:
    import pytesseract  # noqa
    HAVE_TESS = True
except Exception:
    HAVE_TESS = False

try:
    import networkx as nx  # noqa
    HAVE_NX = True
except Exception:
    HAVE_NX = False

# ---------------------- Konfiguration ----------------------
TEXT_MIN_LEN = 120                 # OCR-Fallback wenn Text kurz
SNIPPET_LEN = 140                  # Kontextlänge
YEAR_RE = r"(1[4-9]\d{2}|20\d{2})" # 1400–2099
BIB_HEADINGS = [
    r"^\s*literatur\s*$",
    r"^\s*bibliographi[ea]\s*$",
    r"^\s*literaturverzeichnis\s*$",
    r"^\s*references\s*$",
    r"^\s*bibliography\s*$",
    r"^\s*quellen\s*(und\s*literatur)?\s*$",
]
LOG_TS_FMT = "%H:%M:%S"

# ---------------------- Modelle ----------------------
@dataclass(frozen=True)
class Author:
    author_id: str
    display: str
    surnames: Tuple[str, ...]
    pattern_strs: Tuple[str, ...]
    folded_surnames: Tuple[str, ...]


@dataclass(frozen=True)
class Mention:
    src_id: str
    tgt_id: str
    pdf_path: str
    page: int
    in_bib: bool
    pattern: str
    context: str


# ---------------------- Logging ----------------------
def _ts() -> str:
    return datetime.now().strftime(LOG_TS_FMT)

def log_info(msg: str) -> None:
    print(f"[{_ts()}][INFO] {msg}", flush=True)

def log_warn(msg: str) -> None:
    print(f"[{_ts()}][WARN] {msg}", flush=True)

def log_error(msg: str) -> None:
    print(f"[{_ts()}][ERROR] {msg}", flush=True)


# ---------------------- Utils ----------------------
def normspace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def strip_diacritics(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def diacritic_class(ch: str) -> str:
    sets = {
        'a': "[aàáâäãåā]", 'A': "[AÀÁÂÄÃÅĀ]",
        'o': "[oòóôöõō]", 'O': "[OÒÓÔÖÕŌ]",
        'u': "[uùúûüū]", 'U': "[UÙÚÛÜŪ]",
        'e': "[eèéêëē]", 'E': "[EÈÉÊËĒ]",
        'i': "[iìíîïī]", 'I': "[IÌÍÎÏĪ]",
        'y': "[yýÿ]", 'Y': "[YÝŸ]",
        's': "[sß]", 'S': "[Sẞ]",
        'c': "[cçćč]", 'C': "[CÇĆČ]",
        'z': "[zžźż]", 'Z': "[ZŽŹŻ]",
        'n': "[nñńň]", 'N': "[NÑŃŇ]",
    }
    return sets.get(ch, re.escape(ch))

def make_diacritic_regex(token: str) -> str:
    token = token.replace(".", r"\.")
    out = "".join(diacritic_class(c) for c in token)
    out = out.replace("-", r"[-\s]+")
    return out

def iter_pdfs(root: str) -> Iterable[str]:
    for d, _, files in os.walk(root):
        for fn in files:
            if fn.lower().endswith(".pdf"):
                yield os.path.join(d, fn)

def _display_from_tokens(tokens: List[str]) -> str:
    if len(tokens) >= 2:
        last = tokens[-1].capitalize()
        firsts = " ".join(t.capitalize() for t in tokens[:-1] if len(t) > 1)
        return f"{last}, {firsts}" if firsts else last
    return tokens[0].capitalize() if tokens else "Unbekannt"

def extract_authors_from_filenames(pdf_paths: List[str]) -> Dict[str, Author]:
    authors: Dict[str, Author] = {}
    for p in pdf_paths:
        base = os.path.splitext(os.path.basename(p))[0]
        left = base.split("__")[0].replace("_", "-").strip("-")
        if not left:
            continue
        tokens = [t for t in left.split("-") if t]
        display = _display_from_tokens(tokens)
        # Nachnamenkandidaten
        surs: List[str] = []
        if tokens:
            surs.append(tokens[0])
            if tokens[-1] != tokens[0]:
                surs.append(tokens[-1])
        surs = [s for s in surs if len(s) > 1 and not s.isdigit()]
        if not surs:
            continue
        # Musterstrings bauen (keine kompilierten Regex für Pickle-Freundlichkeit)
        pattern_strs: List[str] = []
        for sur in dict.fromkeys(surs):
            srx = make_diacritic_regex(sur)
            pat1 = rf"\b{srx}\b\s*,\s*[A-ZÄÖÜ][A-Za-zÄÖÜäöüß\.\-]{{1,}}"
            pat2 = rf"\b[A-ZÄÖÜ][A-Za-zÄÖÜäöüß\.\-]{{1,}}\s+{srx}\b"
            pat3 = rf"\b{srx}\b[^\n]{{0,30}}\b{YEAR_RE}\b"
            pattern_strs.extend([pat1, pat2, pat3])
        folded = tuple(strip_diacritics(s).casefold() for s in dict.fromkeys(surs))
        authors[left] = Author(
            author_id=left,
            display=display,
            surnames=tuple(dict.fromkeys(surs)),
            pattern_strs=tuple(pattern_strs),
            folded_surnames=folded,
        )
    return authors

def compile_patterns(pattern_strs: Iterable[str]) -> List[re.Pattern]:
    return [re.compile(p, re.IGNORECASE) for p in pattern_strs]

def compile_bib_heading_patterns() -> List[re.Pattern]:
    return [re.compile(p, re.IGNORECASE | re.MULTILINE) for p in BIB_HEADINGS]

def detect_bibliography_pages(doc: "fitz.Document") -> Set[int]:
    pats = compile_bib_heading_patterns()
    bib_pages: Set[int] = set()
    start_seen = False
    for i in range(doc.page_count):
        page = doc.load_page(i)
        txt = (page.get_text("text") or "").strip()
        if not start_seen:
            for pat in pats:
                if pat.search(txt):
                    start_seen = True
                    break
        if start_seen:
            bib_pages.add(i)
    return bib_pages

def extract_text_with_ocr(page: "fitz.Page") -> str:
    txt = page.get_text("text") or ""
    if len(txt) >= TEXT_MIN_LEN:
        return txt
    if not HAVE_TESS:
        return txt
    try:
        pix = page.get_pixmap(dpi=300)
        from PIL import Image  # pillow
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        ocr = pytesseract.image_to_string(img, lang="deu+eng")
        return ocr or txt
    except Exception:
        return txt

def classify_section(page_index: int, bib_pages: Set[int]) -> str:
    return "bib" if page_index in bib_pages else "text"

def quick_page_filter(text_folded: str, target_folded_surnames: Tuple[str, ...]) -> bool:
    # Sehr schneller Vorfilter ohne Regex, diakritikfrei und casefolded
    return any(sur in text_folded for sur in target_folded_surnames)

def find_author_mentions(text: str, compiled_patterns: List[re.Pattern]) -> List[Tuple[Tuple[int, int], str]]:
    hits: List[Tuple[Tuple[int, int], str]] = []
    for pat in compiled_patterns:
        for m in pat.finditer(text):
            hits.append((m.span(), pat.pattern))
    return hits

def make_context(text: str, span: Tuple[int, int], length: int = SNIPPET_LEN) -> str:
    s, e = span
    mid = (s + e) // 2
    start = max(0, mid - length // 2)
    end = min(len(text), start + length)
    return normspace(text[start:end])

# ---------------------- Aggregation & Output ----------------------
def aggregate_edges(authors: Dict[str, Author], mentions: List[Mention]):
    if not HAVE_PANDAS:
        raise RuntimeError("pandas ist erforderlich. Installation: pip install pandas")
    import pandas as pd  # local import

    nodes = [{"author_id": aid, "display": a.display, "surnames": "|".join(a.surnames)} for aid, a in authors.items()]
    df_nodes = pd.DataFrame(nodes)

    rows = [{
        "src": m.src_id,
        "tgt": m.tgt_id,
        "pdf_file": m.pdf_path,
        "page": m.page,
        "in_bib": int(m.in_bib),
        "pattern": m.pattern,
        "context": m.context
    } for m in mentions]
    df_raw = pd.DataFrame(rows)
    if df_raw.empty:
        df_edges = pd.DataFrame(columns=["src", "tgt", "total", "bib_hits", "text_hits", "examples", "weighted"])
        return df_nodes, df_edges

    agg = df_raw.groupby(["src", "tgt"]).agg(
        total=("pdf_file", "count"),
        bib_hits=("in_bib", "sum"),
        examples=("context", lambda s: " | ".join(s.head(3)))
    ).reset_index()
    agg["text_hits"] = agg["total"] - agg["bib_hits"]
    agg["weighted"] = agg["bib_hits"] * 2 + agg["text_hits"] * 1
    return df_nodes, agg

def ensure_session_dir(base: str, pdfs: List[str]) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    h = hashlib.sha1(("|".join(sorted(os.path.basename(p) for p in pdfs))).encode("utf-8")).hexdigest()[:8]
    root = os.path.join(base, "data", "authors_data")
    session_dir = os.path.join(root, f"session_{ts}_{h}")
    os.makedirs(session_dir, exist_ok=True)
    return session_dir

def write_csvs(out_dir: str, df_nodes, df_edges) -> Tuple[str, str]:
    nodes_path = os.path.join(out_dir, "authors_nodes.csv")
    edges_path = os.path.join(out_dir, "authors_edges.csv")
    df_nodes.to_csv(nodes_path, index=False, sep=";")
    df_edges.to_csv(edges_path, index=False, sep=";")
    log_info(f"CSV geschrieben: {nodes_path}")
    log_info(f"CSV geschrieben: {edges_path}")
    return nodes_path, edges_path

def write_gexf(out_dir: str, authors: Dict[str, Author], df_edges) -> Optional[str]:
    if not HAVE_NX:
        log_info("networkx nicht verfügbar. GEXF wird übersprungen.")
        return None
    import networkx as nx  # local import
    G = nx.DiGraph()
    for aid, a in authors.items():
        G.add_node(aid, label=a.display)
    for _, r in df_edges.iterrows():
        G.add_edge(r["src"], r["tgt"], weight=float(r["weighted"]), total=int(r["total"]),
                   bib=int(r["bib_hits"]), text=int(r["text_hits"]))
    out = os.path.join(out_dir, "authors_network.gexf")
    nx.write_gexf(G, out)
    log_info(f"GEXF geschrieben: {out}")
    return out

def write_html_report(out_dir: str, authors: Dict[str, Author], df_edges) -> str:
    import pandas as pd  # type: ignore
    df_out = df_edges.copy()
    df_out["src_label"] = df_out["src"].map(lambda x: authors[x].display if x in authors else x)
    df_out["tgt_label"] = df_out["tgt"].map(lambda x: authors[x].display if x in authors else x)

    top_citers = df_out.groupby("src_label")["weighted"].sum().sort_values(ascending=False).head(10)
    top_cited  = df_out.groupby("tgt_label")["weighted"].sum().sort_values(ascending=False).head(10)

    def esc(s: str) -> str:
        return html.escape(str(s), quote=True)

    html_parts = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        "<title>Autor↔Autor Referenznetz</title>",
        "<style>",
        "body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:24px}",
        "h1,h2{margin:0.2em 0}",
        "table{border-collapse:collapse;width:100%;margin-top:8px}",
        "th,td{border:1px solid #ddd;padding:6px;vertical-align:top}",
        "th{background:#f6f6f6;text-align:left}",
        ".grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}",
        ".small{color:#666;font-size:12px}",
        "</style></head><body>",
        "<h1>Autor↔Autor Referenznetz</h1>",
        f"<p class='small'>Kanten: <b>{len(df_out)}</b> | Autoren: <b>{len(authors)}</b></p>",
        "<div class='grid'>",
        "<div><h2>Top Zitierende</h2><table><thead><tr><th>Autor</th><th>Score</th></tr></thead><tbody>"
    ]
    for name, val in top_citers.items():
        html_parts.append(f"<tr><td>{esc(name)}</td><td>{int(val)}</td></tr>")
    html_parts.append("</tbody></table></div>")

    html_parts.append("<div><h2>Top Zitierte</h2><table><thead><tr><th>Autor</th><th>Score</th></tr></thead><tbody>")
    for name, val in top_cited.items():
        html_parts.append(f"<tr><td>{esc(name)}</td><td>{int(val)}</td></tr>")
    html_parts.append("</tbody></table></div></div>")

    html_parts.append("<h2>Alle Kanten (Quelle → Ziel)</h2><table><thead><tr>"
                      "<th>Quelle</th><th>Ziel</th><th>Total</th><th>Bibliographie</th><th>Text</th><th>Beispiele</th>"
                      "</tr></thead><tbody>")
    for _, r in df_out.sort_values("weighted", ascending=False).iterrows():
        html_parts.append("<tr>"
                          f"<td>{esc(r['src_label'])}</td>"
                          f"<td>{esc(r['tgt_label'])}</td>"
                          f"<td>{int(r['total'])}</td>"
                          f"<td>{int(r['bib_hits'])}</td>"
                          f"<td>{int(r['text_hits'])}</td>"
                          f"<td>{esc(str(r['examples']))}</td>"
                          "</tr>")
    html_parts.append("</tbody></table></body></html>")

    out = os.path.join(out_dir, "authors_report.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(html_parts))
    log_info(f"HTML geschrieben: {out}")
    return out

def write_session_meta(out_dir: str, base: str, pdfs: List[str]) -> str:
    meta = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "base_folder": os.path.abspath(base),
        "num_pdfs": len(pdfs),
        "pdfs": [os.path.relpath(p, base) for p in pdfs],
        "env": {
            "pymupdf": True,
            "pandas": HAVE_PANDAS,
            "pytesseract": HAVE_TESS,
            "networkx": HAVE_NX,
            "ocr_threshold_chars": TEXT_MIN_LEN
        }
    }
    out = os.path.join(out_dir, "session_meta.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    log_info(f"Session-Metadaten geschrieben: {out}")
    return out

def environment_report() -> None:
    log_info("Bibliotheken:")
    print("  PyMuPDF: OK")
    print(f"  pandas: {'OK' if HAVE_PANDAS else 'NEIN'}")
    print(f"  pytesseract: {'OK' if HAVE_TESS else 'NEIN'}")
    print(f"  networkx: {'OK' if HAVE_NX else 'NEIN'}")
    if HAVE_TESS:
        try:
            ver = pytesseract.get_tesseract_version()
            print(f"  Tesseract-Version: {ver}")
        except Exception:
            pass
    log_info(f"OCR-Fallback aktiv bei < {TEXT_MIN_LEN} Zeichen.")

