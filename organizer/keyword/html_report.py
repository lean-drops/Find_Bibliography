"""
html_report.py — Umfangreicher HTML-Report: Datei×Begriff, Autoren, Ko-Auftreten, KWIC pro Root.

Nutzung
-------
python organizer/keyword/html_report.py

Funktion
--------
render_html_report(payload: dict, out_dir: Path) -> Dict[str, str]
Schreibt:
- report.html  (verlinkt report.css)
- report.css
- report.scss

Erwartete payload-Keys
----------------------
- session, seed, config, generated_raw.terms_by_category, expanded_terms, master_regex
- scan_result (von scan_library_for_families_detailed)
"""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Dict, Any, List


_CSS = """
/* report.css */
:root {
  --bg:#111214; --panel:#1b1d20; --text:#e6e6e6; --muted:#a9a9a9;
  --accent:#0e639c; --accent2:#15a0bf; --border:#2a2c30; --ok:#58a55c; --bad:#c65d5d;
}
*{box-sizing:border-box} body{margin:0;font:16px/1.5 'Segoe UI',Arial,sans-serif;color:var(--text);background:var(--bg)}
.wrap{max-width:1200px;margin:24px auto;padding:0 16px}
.header{background:linear-gradient(135deg,var(--panel),#20242a);border:1px solid var(--border);border-radius:12px;padding:16px 20px;margin-bottom:16px}
.h1{margin:0 0 6px;font-size:24px;font-weight:700}
.meta{color:var(--muted);font-size:14px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:14px}
.card h2{margin:0 0 8px;font-size:18px;border-bottom:1px solid var(--border);padding-bottom:8px}
mono{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:13px;background:#0b0c0f;border-radius:8px;padding:10px;display:block;white-space:pre-wrap;word-break:break-word}
.badges{display:flex;flex-wrap:wrap;gap:6px}
.badge{background:#0b1e28;color:#dff1ff;padding:4px 8px;border:1px solid #113447;border-radius:999px;font-size:12px}
.table{width:100%;border-collapse:collapse}
.table th,.table td{border-bottom:1px solid var(--border);padding:6px 8px;text-align:left}
.table th{color:var(--muted);font-weight:600}
small{font-size:13px;color:var(--muted)}
.num{text-align:right}
.ok{color:var(--ok)}
.ac{color:var(--accent2)}
.scroll{overflow:auto;max-height:480px}
"""

_SCSS = """
// report.scss (Design-Quelle) – siehe kompiliertes CSS in report.css
"""


def _esc(s: Any) -> str:
    return html.escape(str(s)) if s is not None else ""


def _render_terms(terms_by_category: Dict[str, List[str]]) -> str:
    parts = []
    for cat, terms in terms_by_category.items():
        badges = "".join(f'<span class="badge">{_esc(t)}</span>' for t in terms)
        parts.append(f'<div class="card"><h2>Kategorie: {_esc(cat)}</h2><div class="badges">{badges or "<small>Keine</small>"}</div></div>')
    return "\n".join(parts)


def _render_top_files(scan: Dict[str, Any]) -> str:
    if not scan:
        return '<p class="small">Keine Scan-Daten.</p>'
    top = scan.get("summary", {}).get("top_files", [])
    rows = "\n".join(f"<tr><td>{_esc(e['path'])}</td><td class='num'>{_esc(e['hits'])}</td></tr>" for e in top)
    return f'<table class="table"><thead><tr><th>Datei</th><th class="num">Treffer</th></tr></thead><tbody>{rows}</tbody></table>'


def _render_totals_by_root(scan: Dict[str, Any]) -> str:
    tot = scan.get("summary", {}).get("totals_by_root", [])
    rows = "\n".join(f"<tr><td>{_esc(e['root'])}</td><td class='num'>{_esc(e['hits'])}</td></tr>" for e in tot)
    return f'<table class="table"><thead><tr><th>Begriff (Root)</th><th class="num">Treffer gesamt</th></tr></thead><tbody>{rows}</tbody></table>'


def _render_authors(scan: Dict[str, Any]) -> str:
    au = scan.get("summary", {}).get("authors_top", [])
    rows = "\n".join(f"<tr><td>{_esc(e['author'])}</td><td class='num'>{_esc(e['hits'])}</td></tr>" for e in au)
    return f'<table class="table"><thead><tr><th>Autor (Heuristik)</th><th class="num">Treffer</th></tr></thead><tbody>{rows}</tbody></table>'


def _render_cooccurrence(scan: Dict[str, Any], limit: int = 50) -> str:
    co = scan.get("summary", {}).get("cooccurrence_top", [])
    rows = "\n".join(
        f"<tr><td>{_esc(e['root1'])} × {_esc(e['root2'])}</td><td class='num'>{_esc(e['docs'])}</td></tr>"
        for e in co[:limit]
    )
    return f'<table class="table"><thead><tr><th>Ko-Auftreten (Dokumentebene)</th><th class="num">Dokumente</th></tr></thead><tbody>{rows}</tbody></table>'


def _render_file_by_root_table(scan: Dict[str, Any], cols: int = 8, limit_rows: int = 200) -> str:
    files = scan.get("files", [])
    totals = scan.get("summary", {}).get("totals_by_root", [])
    top_roots = [e["root"] for e in totals[:cols]] if totals else []
    head = "".join(f"<th class='num'>{_esc(r)}</th>" for r in top_roots)
    rows = []
    for f in files[:limit_rows]:
        by_root = f.get("by_root", {})
        other = sum(v for k, v in by_root.items() if k not in top_roots)
        cells = "".join(f"<td class='num'>{_esc(by_root.get(r, 0))}</td>" for r in top_roots)
        rows.append(
            f"<tr><td>{_esc(f.get('path'))}</td><td>{_esc(f.get('author_guess') or '')}</td>"
            f"<td class='num'><b>{_esc(f.get('hits_total', 0))}</b></td>{cells}<td class='num'>{_esc(other)}</td></tr>"
        )
    body = "\n".join(rows)
    return (
        "<div class='scroll'>"
        "<table class='table'><thead><tr>"
        "<th>Datei</th><th>Autor</th><th class='num'>Hits</th>"
        f"{head}<th class='num'>Sonstige</th>"
        "</tr></thead><tbody>"
        f"{body}"
        "</tbody></table></div>"
    )


def _render_kwic_by_root(scan: Dict[str, Any], limit_roots: int = 10) -> str:
    totals = scan.get("summary", {}).get("totals_by_root", [])
    order = [e["root"] for e in totals[:limit_roots]]
    files = scan.get("files", [])
    blocks = []
    for root in order:
        examples = []
        for f in files:
            for s in (f.get("samples_by_root", {}) or {}).get(root, []):
                p = f"(S. {s.get('page')}) " if s.get('page') else ""
                examples.append(f"<div class='mono small'>{_esc(p + s.get('kwic',''))}</div>")
                if len(examples) >= 8:
                    break
            if len(examples) >= 8:
                break
        body = "\n".join(examples) or "<small>Keine Beispiele.</small>"
        blocks.append(f"<div class='card'><h2>KWIC: {_esc(root)}</h2>{body}</div>")
    return "\n".join(blocks)


def render_html_report(payload: Dict[str, Any], out_dir: Path) -> Dict[str, str]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / "report.html"
    css_path = out_dir / "report.css"
    scss_path = out_dir / "report.scss"

    css_path.write_text(_CSS, encoding="utf-8")
    scss_path.write_text(_SCSS, encoding="utf-8")

    session = payload.get("session", {})
    seed = payload.get("seed", "")
    conf = payload.get("config", {})
    gen = payload.get("generated_raw", {}) or {}
    tbc = gen.get("terms_by_category", {})
    expanded = payload.get("expanded_terms", {})
    master_regex = payload.get("master_regex", "")
    scan = payload.get("scan_result", {}) or {}

    html_doc = f"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Konzept-Report – {_esc(session.get('name',''))}</title>
  <link rel="stylesheet" href="report.css">
</head>
<body>
  <div class="wrap">
    <header class="header">
      <h1 class="h1">Konzept-Report</h1>
      <div class="meta">
        Session: <span class="ac">{_esc(session.get('name',''))}</span> ·
        Seed: <strong>{_esc(seed)}</strong> ·
        Sprache: {_esc(conf.get('language',''))} ·
        Modell: {_esc(conf.get('model',''))} ·
        KWIC: {_esc(conf.get('kwic_chars',''))} ·
        Erstellt: {_esc(session.get('created_at',''))}
      </div>
    </header>

    <section class="grid">
      <div class="card">
        <h2>Master-Regex</h2>
        <mono>{_esc(master_regex) or '<small>–</small>'}</mono>
      </div>
      <div class="card">
        <h2>Konfiguration</h2>
        <table class="table">
          <tr><th>Library</th><td><small>{_esc(conf.get('library_dir',''))}</small></td></tr>
          <tr><th>Model</th><td>{_esc(conf.get('model',''))}</td></tr>
          <tr><th>Language</th><td>{_esc(conf.get('language',''))}</td></tr>
          <tr><th>KWIC</th><td>{_esc(conf.get('kwic_chars',''))}</td></tr>
        </table>
      </div>
    </section>

    <section class="grid">
      {_render_terms(tbc)}
    </section>

    <section class="card">
      <h2>Erweiterte Familien (dedupliziert)</h2>
      <mono><small>{_esc(json.dumps(expanded, ensure_ascii=False, indent=2) if expanded else '–')}</small></mono>
    </section>

    <section class="grid">
      <div class="card">
        <h2>Termverteilung gesamt</h2>
        {_render_totals_by_root(scan)}
      </div>
      <div class="card">
        <h2>Top-Dateien</h2>
        {_render_top_files(scan)}
      </div>
    </section>

    <section class="grid">
      <div class="card">
        <h2>Per Autor (Heuristik)</h2>
        {_render_authors(scan)}
      </div>
      <div class="card">
        <h2>Ko-Auftreten der Begriffe (Dokumentebene)</h2>
        {_render_cooccurrence(scan)}
      </div>
    </section>

    <section class="card">
      <h2>Datei × Begriff (Top 8 Begriffe als Spalten)</h2>
      {_render_file_by_root_table(scan, cols=8, limit_rows=300)}
      <small>Hinweis: „Sonstige“ aggregiert alle weiteren Begriffe außerhalb der Top-8.</small>
    </section>

    <section class="grid">
      {_render_kwic_by_root(scan, limit_roots=10)}
    </section>

    <footer class="meta" style="margin-top:12px;">
      © Report – JSON-basiert; CSS getrennt. SCSS-Quelle: report.scss
    </footer>
  </div>
</body>
</html>
"""
    html_path.write_text(html_doc, encoding="utf-8")
    return {"html": str(html_path), "css": str(css_path), "scss": str(scss_path)}


def main() -> None:
    print("[DEBUG] html_report.py Demo → ./_demo_report")
    demo_scan = {
        "summary": {
            "files_scanned": 3, "files_with_hits": 3, "total_hits": 12,
            "totals_by_root": [{"root": "Frieden", "hits": 7}, {"root": "Waffenstillstand", "hits": 5}],
            "authors_top": [{"author": "Muster, A.", "hits": 6}, {"author": "Beispiel B", "hits": 4}],
            "cooccurrence_top": [{"root1": "Frieden", "root2": "Waffenstillstand", "docs": 2}],
            "top_files": [{"path": "/a/b/c1.pdf", "hits": 5}, {"path": "/a/b/c2.pdf", "hits": 4}],
        },
        "files": [
            {"path": "/a/b/c1.pdf","hits_total":5,"author_guess":"Muster, A.","by_root":{"Frieden":3,"Waffenstillstand":2},
             "samples_by_root":{"Frieden":[{"page":1,"kwic":"... [[Frieden]] ..."}]}},
            {"path": "/a/b/c2.pdf","hits_total":4,"author_guess":"Beispiel B","by_root":{"Frieden":2,"Waffenstillstand":2},"samples_by_root":{}},
            {"path": "/a/b/c3.pdf","hits_total":3,"author_guess":"C Autor","by_root":{"Frieden":2,"Waffenstillstand":1},"samples_by_root":{}},
        ],
        "detailed": True
    }
    demo = {
        "session": {"name": "session_demo_00", "created_at": "2025-11-08T21:00:00Z"},
        "seed": "Frieden",
        "config": {"language": "de", "model": "gpt-4.1-mini", "kwic_chars": 80, "library_dir": "/path/to/lib"},
        "generated_raw": {"terms_by_category": {"core": ["Frieden"], "truce": ["Waffenstillstand"]}},
        "expanded_terms": {"Frieden": ["Frieden","Friede"], "Waffenstillstand": ["Waffenstillstand"]},
        "master_regex": r"\\b(?:Frieden|Friede|Waffenstillstand)\\b",
        "scan_result": demo_scan
    }
    out = Path("./_demo_report")
    res = render_html_report(demo, out)
    print("[DEBUG] HTML:", res.get("html"))
    print("[DEBUG] CSS :", res.get("css"))
    print("[DEBUG] SCSS:", res.get("scss"))


if __name__ == "__main__":
    main()
