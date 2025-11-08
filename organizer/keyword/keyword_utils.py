"""
keyword_utils.py — Deduplikation, Varianten, Regex und DETAIL-SCAN mit Term×Datei-Matrix.

Nutzung
-------
python organizer/keyword/keyword_utils.py

Funktionen
----------
- deduplicate_terms(terms, threshold=0.88) -> Liste eindeutiger Begriffe
- build_families(terms, add_historic_variants=True) -> [{"root": str, "variants": [str, ...]}, ...]
- compile_family_patterns(families) -> re.Pattern (Master-Regex, Wortgrenzen)
- scan_library_for_pattern(base_dir, master_pattern, exclude_dir_name, kwic_chars) -> simple Scan
- scan_library_for_families_detailed(base_dir, families, ...) -> DETAIL: Termcounts je Datei, Autoren-Heuristik,
  Ko-Auftreten, KWIC pro Root. Eignet sich für HTML-Report.

Besonderheiten
--------------
- Historische Orthographie: ß/ss, ä/ae, ö/oe, ü/ue, i/j, u/v
- PDF-Extraktion via pypdf oder PyPDF2; TXT/DOCX optional
"""

from __future__ import annotations

import difflib
import os
import re
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import List, Dict, Any, Iterable, Tuple, Optional

# Optionale Reader
_PYPDF = False
_PYPDF2 = False
_DOCX = False
try:
    import pypdf  # type: ignore
    _PYPDF = True
except Exception:
    try:
        import PyPDF2  # type: ignore
        _PYPDF2 = True
    except Exception:
        pass

try:
    import docx  # type: ignore
    _DOCX = True
except Exception:
    pass


def _norm(s: str) -> str:
    s2 = s.casefold()
    s2 = s2.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    s2 = s2.replace("j", "i").replace("v", "u")
    s2 = re.sub(r"\s+", " ", s2).strip()
    return s2


def deduplicate_terms(terms: Iterable[str], threshold: float = 0.88) -> List[str]:
    uniq: List[str] = []
    for t in terms:
        nt = _norm(t)
        if not nt:
            continue
        if all(difflib.SequenceMatcher(None, nt, _norm(u)).ratio() < threshold for u in uniq):
            uniq.append(t)
    return uniq


def _variant_token_regex(token: str) -> str:
    out = []
    for c in token:
        if c in "äÄ":
            out.append("[aäAÄ]")
        elif c in "öÖ":
            out.append("[oöOÖ]")
        elif c in "üÜ":
            out.append("[uüUÜ]")
        elif c == "ß":
            out.append("(?:ß|ss)")
        elif c in "jJ":
            out.append("[ijIJ]")
        elif c in "vV":
            out.append("[uvUV]")
        else:
            out.append(re.escape(c))
    return "".join(out)


def _expand_word_variants(word: str) -> List[str]:
    w = word.strip()
    variants = {w}
    if re.search(r"[b-df-hj-np-tv-z]$", w, re.IGNORECASE):
        variants.update({w + "e", w + "en", w + "es", w + "er"})
    if w.endswith(("en", "eln", "ern")):
        variants.update({w[:-2], w[:-1]})
    return sorted(variants)


def build_families(terms: Iterable[str], add_historic_variants: bool = True) -> List[Dict[str, List[str]]]:
    families: List[Dict[str, List[str]]] = []
    for t in terms:
        root = t.strip()
        if not root:
            continue
        var_words = _expand_word_variants(root)
        variants: List[str] = []
        for v in var_words:
            variants.append(_variant_token_regex(v) if add_historic_variants else re.escape(v))
        families.append({"root": root, "variants": sorted(set(variants))})
    return families


def compile_family_patterns(families: List[Dict[str, List[str]]]) -> re.Pattern:
    alts: List[str] = []
    for fam in families:
        alts.extend(fam["variants"])
    if not alts:
        alts = [r"UNMATCHABLE_TOKEN_12345"]
    expr = r"\b(?:%s)\b" % "|".join(sorted(set(alts), key=lambda s: (len(s), s), reverse=True))
    return re.compile(expr, re.IGNORECASE)


def _compile_per_root(families: List[Dict[str, List[str]]]) -> List[Tuple[str, re.Pattern]]:
    compiled: List[Tuple[str, re.Pattern]] = []
    for fam in families:
        root = fam["root"]
        alts = fam["variants"] or [re.escape(root)]
        expr = r"\b(?:%s)\b" % "|".join(sorted(set(alts), key=lambda s: (len(s), s), reverse=True))
        compiled.append((root, re.compile(expr, re.IGNORECASE)))
    return compiled


def _read_pdf_pages(path: Path) -> List[str]:
    if _PYPDF:
        reader = pypdf.PdfReader(str(path))  # type: ignore
        out = []
        for p in reader.pages:
            try:
                out.append(p.extract_text() or "")
            except Exception:
                out.append("")
        return out
    if _PYPDF2:
        reader = PyPDF2.PdfReader(str(path))  # type: ignore
        out = []
        for p in reader.pages:  # type: ignore
            try:
                out.append(p.extract_text() or "")
            except Exception:
                out.append("")
        return out
    raise RuntimeError("Keine PDF-Bibliothek verfügbar (pypdf/PyPDF2).")


def _read_txt(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def _read_docx(path: Path) -> str:
    if not _DOCX:
        raise RuntimeError("DOCX nicht unterstützt (python-docx fehlt).")
    d = docx.Document(str(path))  # type: ignore
    return "\n".join(p.text for p in d.paragraphs)


def _normalize_for_search(s: str) -> str:
    s2 = s.replace("ß", "ss")
    s2 = re.sub(r"\s+", " ", s2)
    return s2


def _kwic(text: str, start: int, end: int, window: int) -> str:
    b = max(0, start - window)
    e = min(len(text), end + window)
    return text[b:start] + "[[" + text[start:end] + "]]" + text[end:e]


def _guess_author_from_filename(filename: str) -> str:
    """
    Heuristik:
    - Nimmt Basisname ohne Endung.
    - Versucht 'Nachname, Vorname' → 'Nachname'.
    - Sonst erster Tokenblock vor '_' oder '-' mit Großbuchstabenstart.
    - Fallback: Basisname gekürzt.
    """
    base = re.sub(r"\.[^.]+$", "", filename)
    cand = base.replace("–", "-")
    m = re.search(r"^([A-ZÄÖÜ][A-Za-zÄÖÜäöüß\-]+),\s*[A-ZÄÖÜ]", cand)
    if m:
        return m.group(1)
    part = re.split(r"[_\-]", cand)[0].strip()
    m2 = re.match(r"^[A-ZÄÖÜ][A-Za-zÄÖÜäöüß\-]+(?:\s+[A-ZÄÖÜ][A-Za-zÄÖÜäöüß\-]+)?", part)
    if m2:
        return m2.group(0)
    return (part or base)[:40]


def scan_library_for_pattern(
    base_dir: Path,
    master_pattern: re.Pattern,
    exclude_dir_name: str = "chroniken",
    kwic_chars: int = 80,
) -> Dict[str, Any]:
    """Einfacher Scan (Kompatibilität)."""
    files: List[Path] = []
    for root, dirs, fnames in os.walk(base_dir):
        dirs[:] = [d for d in dirs if d.lower() != exclude_dir_name.lower()]
        for fn in fnames:
            p = Path(root) / fn
            if p.suffix.lower() in (".pdf", ".txt") or (p.suffix.lower() == ".docx" and _DOCX):
                files.append(p)

    results: List[Dict[str, Any]] = []
    total_hits = 0
    for fp in files:
        try:
            if fp.suffix.lower() == ".pdf":
                pages = _read_pdf_pages(fp)
                page_hits = 0
                samples = []
                for i, page in enumerate(pages, start=1):
                    text = _normalize_for_search(page)
                    for m in master_pattern.finditer(text):
                        page_hits += 1
                        if len(samples) < 6:
                            samples.append({"page": i, "kwic": _kwic(text, m.start(), m.end(), kwic_chars)})
                total_hits += page_hits
                results.append({"path": str(fp), "hits": page_hits, "samples": samples, "pages": len(pages)})

            elif fp.suffix.lower() == ".txt":
                text = _normalize_for_search(_read_txt(fp))
                hits = sum(1 for _ in master_pattern.finditer(text))
                total_hits += hits
                samples = []
                for i, m in enumerate(master_pattern.finditer(text)):
                    if i < 6:
                        samples.append({"kwic": _kwic(text, m.start(), m.end(), kwic_chars)})
                results.append({"path": str(fp), "hits": hits, "samples": samples, "pages": None})

            elif fp.suffix.lower() == ".docx" and _DOCX:
                text = _normalize_for_search(_read_docx(fp))
                hits = sum(1 for _ in master_pattern.finditer(text))
                total_hits += hits
                samples = []
                for i, m in enumerate(master_pattern.finditer(text)):
                    if i < 6:
                        samples.append({"kwic": _kwic(text, m.start(), m.end(), kwic_chars)})
                results.append({"path": str(fp), "hits": hits, "samples": samples, "pages": None})
        except Exception as e:
            results.append({"path": str(fp), "hits": 0, "error": str(e), "pages": None})

    results.sort(key=lambda x: x["hits"], reverse=True)
    summary = {
        "files_scanned": len(files),
        "files_with_hits": sum(1 for r in results if r.get("hits", 0) > 0),
        "total_hits": total_hits,
        "top_files": [{"path": r["path"], "hits": r["hits"]} for r in results[:20]],
    }
    return {"summary": summary, "files": results}


def scan_library_for_families_detailed(
    base_dir: Path,
    families: List[Dict[str, List[str]]],
    exclude_dir_name: str = "chroniken",
    kwic_chars: int = 80,
    max_samples_per_root: int = 4,
) -> Dict[str, Any]:
    """
    DETAIL-SCAN: zählt Treffer pro Root-Familie je Datei, liefert KWICs und Aggregationen.

    Rückgabe:
    {
      "summary": {
        "files_scanned": int,
        "files_with_hits": int,
        "total_hits": int,
        "roots": [str, ...],
        "totals_by_root": [{"root": str, "hits": int}, ...],
        "authors_top": [{"author": str, "hits": int}, ...],
        "cooccurrence_top": [{"root1": str, "root2": str, "docs": int}, ...],
        "top_files": [{"path": str, "hits": int}, ...],
      },
      "files": [
        {
          "path": str, "pages": int|None, "author_guess": str,
          "hits_total": int, "by_root": {root: int, ...},
          "samples_by_root": {root: [{"page": int|None, "kwic": str}, ...], ...}
        }, ...
      ],
      "detailed": True
    }
    """
    base_dir = Path(base_dir)
    comp = _compile_per_root(families)
    roots = [r for r, _ in comp]

    files: List[Path] = []
    for root, dirs, fnames in os.walk(base_dir):
        dirs[:] = [d for d in dirs if d.lower() != exclude_dir_name.lower()]
        for fn in fnames:
            p = Path(root) / fn
            if p.suffix.lower() in (".pdf", ".txt") or (p.suffix.lower() == ".docx" and _DOCX):
                files.append(p)

    results: List[Dict[str, Any]] = []
    totals_by_root = Counter()
    author_totals = Counter()
    co_docs = Counter()  # pair -> doccount
    total_hits = 0

    for fp in files:
        author_guess = _guess_author_from_filename(fp.name)
        file_counts = Counter()
        samples_by_root: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        try:
            if fp.suffix.lower() == ".pdf":
                pages = _read_pdf_pages(fp)
                for pi, page in enumerate(pages, start=1):
                    text = _normalize_for_search(page)
                    for root_name, pat in comp:
                        for m in pat.finditer(text):
                            file_counts[root_name] += 1
                            if len(samples_by_root[root_name]) < max_samples_per_root:
                                samples_by_root[root_name].append({"page": pi, "kwic": _kwic(text, m.start(), m.end(), kwic_chars)})
                pages_n = len(pages)

            elif fp.suffix.lower() == ".txt":
                text = _normalize_for_search(_read_txt(fp))
                for root_name, pat in comp:
                    for m in pat.finditer(text):
                        file_counts[root_name] += 1
                        if len(samples_by_root[root_name]) < max_samples_per_root:
                            samples_by_root[root_name].append({"page": None, "kwic": _kwic(text, m.start(), m.end(), kwic_chars)})
                pages_n = None

            elif fp.suffix.lower() == ".docx" and _DOCX:
                text = _normalize_for_search(_read_docx(fp))
                for root_name, pat in comp:
                    for m in pat.finditer(text):
                        file_counts[root_name] += 1
                        if len(samples_by_root[root_name]) < max_samples_per_root:
                            samples_by_root[root_name].append({"page": None, "kwic": _kwic(text, m.start(), m.end(), kwic_chars)})
                pages_n = None

            else:
                pages_n = None

            hits_total = sum(file_counts.values())
            total_hits += hits_total
            results.append({
                "path": str(fp),
                "pages": pages_n,
                "author_guess": author_guess,
                "hits_total": hits_total,
                "by_root": dict(file_counts),
                "samples_by_root": samples_by_root,
            })

            totals_by_root.update(file_counts)
            if hits_total:
                author_totals[author_guess] += hits_total
                present = [r for r, c in file_counts.items() if c > 0]
                for a, b in combinations(sorted(present), 2):
                    co_docs[(a, b)] += 1

        except Exception as e:
            results.append({
                "path": str(fp),
                "pages": None,
                "author_guess": author_guess,
                "hits_total": 0,
                "by_root": {},
                "samples_by_root": {},
                "error": str(e),
            })

    results.sort(key=lambda x: x.get("hits_total", 0), reverse=True)
    files_with_hits = sum(1 for r in results if r.get("hits_total", 0) > 0)

    summary = {
        "files_scanned": len(files),
        "files_with_hits": files_with_hits,
        "total_hits": total_hits,
        "roots": roots,
        "totals_by_root": [{"root": r, "hits": c} for r, c in totals_by_root.most_common()],
        "authors_top": [{"author": a, "hits": h} for a, h in author_totals.most_common()],
        "cooccurrence_top": [{"root1": a, "root2": b, "docs": n} for (a, b), n in sorted(co_docs.items(), key=lambda x: x[1], reverse=True)],
        "top_files": [{"path": r["path"], "hits": r["hits_total"]} for r in results[:20]],
    }
    return {"summary": summary, "files": results, "detailed": True}


def main() -> None:
    print("[DEBUG] keyword_utils.py: Minimaltest")
    terms = ["Frieden", "Friede", "friden", "Landfrieden", "Waffenstillstand", "Stillstand"]
    ded = deduplicate_terms(terms, 0.86)
    fams = build_families(ded)
    pat = compile_family_patterns(fams)
    print("[DEBUG] ded:", ded)
    print("[DEBUG] regex:", pat.pattern[:120], "…")
    print("[DEBUG] Ende keyword_utils.py")


if __name__ == "__main__":
    main()


