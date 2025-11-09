"""
chronik_finder/output.py – Ausgabe: Session-Ordner, CSV, HTML (Template-basiert mit Auto-Einbettung von CSS/JS), GEXF, Metadaten.

Nutzung:
  - Wird vom Orchestrator verwendet:
      from .output import ensure_session_dir, write_outputs, write_html, write_meta, maybe_write_gexf
  - Testmodus:
      python -m chronik_finder.output

Eigenschaften:
  - Lädt Templates aus <PROJECT_ROOT>/config/:
      report_template.html, report_style.css, report_ui.js
  - Setzt Platzhalter im HTML-Template ein. Falls einzelne Platzhalter fehlen,
    werden CSS/JS/JSON-Variablen robust in den <head>/<body> injiziert.
  - Data-Verzeichnis:
      ENV CHRONIK_DATA_DIR → <ROOT>/../data → <ROOT>/data
  - CSV-Dateien und Filenamen bleiben identisch.
"""
from __future__ import annotations

import html
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .env import HAVE_NX, HAVE_PANDAS, nx, pd  # type: ignore
from .constants import TEXT_MIN_LEN
from .paths import project_root, data_base_dir


# ------------------------- Session / CSV -------------------------

def ensure_session_dir(root: Path, pdfs: List[Path]) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    h = hashlib.sha1("|".join(sorted(p.name for p in pdfs)).encode("utf-8")).hexdigest()[:8]
    base = data_base_dir(root)
    out = base / "chronik_data" / f"session_{ts}_{h}"
    out.mkdir(parents=True, exist_ok=True)
    return out


def write_outputs(out_dir: Path, df, agg) -> Tuple[Path, Path]:
    det = out_dir / "chroniken_mentions.csv"
    smy = out_dir / "chroniken_summary.csv"
    if df is None:
        raise RuntimeError("interner Fehler: df ist None")
    if agg is None:
        raise RuntimeError("interner Fehler: agg ist None")
    df.to_csv(det, index=False, sep=";")
    agg.sort_values(["group", "weighted_mentions"], ascending=[True, False]).to_csv(smy, index=False, sep=";")
    print(f"[INFO] CSV geschrieben: {det}")
    print(f"[INFO] CSV geschrieben: {smy}")
    return det, smy


# ------------------------- HTML Report -------------------------

def _esc(x: object) -> str:
    return html.escape("" if x is None else str(x), quote=True)


def _mark_once(text: str, pattern: str) -> str:
    """Markiert genau ein Vorkommen von pattern in text. Bei Fehlern: unmarkiert."""
    esc = html.escape(text, quote=True)
    try:
        import re
        rgx = re.compile(pattern, re.IGNORECASE)
        return rgx.sub(lambda m: f"<mark>{html.escape(m.group(0))}</mark>", esc, count=1)
    except Exception:
        return esc


def _safe_read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _load_templates() -> Tuple[str, str, str, bool]:
    """
    Liefert (html_tpl, css_tpl, js_tpl, all_present).
    Fallback auf Minimal-Template, wenn Dateien fehlen.
    """
    cfg_dir = project_root() / "config"
    tpl_html = cfg_dir / "report_template.html"
    tpl_css = cfg_dir / "report_style.css"
    tpl_js = cfg_dir / "report_ui.js"

    html_txt = _safe_read(tpl_html)
    css_txt = _safe_read(tpl_css)
    js_txt = _safe_read(tpl_js)

    if html_txt and css_txt and js_txt:
        return html_txt, css_txt, js_txt, True

    # Minimaler Fallback
    fb_html = """<!doctype html><html><head><meta charset="utf-8"><title>{{TITLE}}</title>{{CSS_INJECT}}</head><body>
<h1>{{TITLE}}</h1>
<p class="small">Stand: {{STAMP}} · Treffer: <b>{{TOTAL_HITS}}</b> · Labels: <b>{{TOTAL_LABELS}}</b> · Dateien: <b>{{TOTAL_FILES}}</b></p>
<div class="toolbar"><input id="flt" class="filter" placeholder="Filter…"><button id="dark" class="toggle">Dark</button></div>
<section class="chips" id="labelChips">{{LABEL_CHIPS}}<button class="chip clear" id="clearLabels">Alle</button></section>
<table id="agg"><thead><tr><th>Gruppe</th><th>Label</th><th>Mentions</th><th>Dokumente</th><th>Weighted</th></tr></thead><tbody>{{AGG_ROWS}}</tbody></table>
<table id="det"><thead><tr><th>Datei</th><th>Seite</th><th>Gruppe</th><th>Label</th><th>Muster</th><th>Kontext</th></tr></thead><tbody>{{DETAIL_ROWS}}</tbody></table>
{{JSON_INJECT}}{{JS_INJECT}}</body></html>"""
    fb_css = "body{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:24px}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ddd;padding:6px}.chip{display:inline-block;border:1px solid #ccc;padding:2px 8px;border-radius:10px;margin:2px}.hidden{display:none}"
    fb_js = "(()=>{const $=s=>document.querySelector(s),$$=s=>Array.from(document.querySelectorAll(s));let t='';const i=$('#flt');i&&(i.oninput=()=>{$$('#agg tbody tr,#det tbody tr').forEach(r=>r.style.display=(r.innerText.toLowerCase().includes(i.value.toLowerCase()))?'':'none')});document.getElementById('dark')?.addEventListener('click',()=>document.documentElement.classList.toggle('dark'));})();"
    return fb_html, fb_css, fb_js, False


def _inject_assets(html_tpl: str, css: str, js: str, json_script: str) -> str:
    """
    Fügt CSS/JS/JSON in das Template ein.
    - Bevorzugt Platzhalter: {{INLINE_CSS}}, {{INLINE_JS}} und JSON_*
    - Falls Platzhalter fehlen: CSS vor </head>, JSON + JS vor </body> einfügen.
    """
    out = html_tpl

    # CSS
    if "{{INLINE_CSS}}" in out:
        out = out.replace("{{INLINE_CSS}}", css)
    else:
        head_tag = "</head>"
        css_block = f"<style>{css}</style>"
        if head_tag in out:
            out = out.replace(head_tag, css_block + head_tag)
        else:
            out = css_block + out

    # JSON Variablen
    json_block = f"<script>{json_script}</script>"
    json_placeholders = [
        "{{JSON_LABEL_STATS}}",
        "{{JSON_LABEL_EXAMPLES}}",
        "{{JSON_GROUPS}}",
        "{{JSON_LABELS_SORTED}}",
    ]
    has_all_json_ph = all(p in out for p in json_placeholders)
    if has_all_json_ph:
        # Wird höher in write_html via replace() erledigt. Hier kein Eingriff.
        pass
    else:
        # Komplett-Injektion mit zusammengebautem JSON-Skript
        if "{{JSON_INJECT}}" in out:
            out = out.replace("{{JSON_INJECT}}", json_block)
        else:
            if "</body>" in out:
                out = out.replace("</body>", json_block + "</body>")
            else:
                out += json_block

    # JS
    if "{{INLINE_JS}}" in out:
        out = out.replace("{{INLINE_JS}}", js)
    else:
        js_block = f"<script>{js}</script>"
        if "{{JS_INJECT}}" in out:
            out = out.replace("{{JS_INJECT}}", js_block)
        else:
            if "</body>" in out:
                out = out.replace("</body>", js_block + "</body>")
            else:
                out += js_block

    return out


def _build_label_maps(df, agg, max_examples: int = 12) -> Tuple[Dict[str, dict], Dict[str, List[dict]]]:
    label_stats: Dict[str, dict] = {}
    label_examples: Dict[str, List[dict]] = {}

    if agg is not None and len(agg) > 0:
        for _, r in agg.iterrows():
            label_stats[str(r["label"])] = {
                "group": str(r["group"]),
                "mentions": int(r["mentions"]),
                "docs": int(r["docs"]),
                "weighted": float(r["weighted_mentions"]),
            }

    if df is not None and len(df) > 0:
        for label, sub in df.groupby("label"):
            sub_sorted = sub.sort_values(["pdf_file", "page"]).head(max_examples)
            label_examples[str(label)] = [{
                "file": Path(row["pdf_file"]).name,
                "page": int(row["page"]),
                "pattern": str(row["pattern"]),
                "context": str(row["context"]),
                "group": str(row["group"]),
                "label": str(row["label"]),
            } for _, row in sub_sorted.iterrows()]
    return label_stats, label_examples


def write_html(out_dir: Path, df, agg) -> Path:
    """
    Baut den HTML-Report mithilfe der Templates.
    Platzhalter im Template:
      {{TITLE}}, {{STAMP}}, {{TOTAL_*}}, {{GROUP_BUTTONS}}, {{LABEL_CHIPS}},
      {{AGG_ROWS}}, {{DETAIL_ROWS}}, {{INLINE_CSS}}, {{INLINE_JS}},
      {{JSON_LABEL_STATS}}, {{JSON_LABEL_EXAMPLES}}, {{JSON_GROUPS}},
      {{JSON_LABELS_SORTED}}, {{DARK_CLASS}}
    Falls einzelne fehlen, werden CSS/JS/JSON robust injiziert.
    """
    if not HAVE_PANDAS:
        raise RuntimeError("pandas ist erforderlich, um den Report zu erstellen.")
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "chroniken_report.html"

    # Daten
    total_hits = 0 if df is None else int(len(df))
    total_labels = 0 if agg is None else int(len(set(agg["label"])))
    total_files = 0 if df is None else int(len(set(df["pdf_file"])))
    groups = sorted(set(agg["group"].astype(str).tolist())) if agg is not None and len(agg) > 0 else ["work", "series", "generic"]

    if agg is not None and len(agg) > 0:
        labels_sorted = list(agg.sort_values("weighted_mentions", ascending=False)["label"].astype(str).tolist())
    else:
        labels_sorted = sorted({str(x) for x in (df["label"].tolist() if df is not None and len(df) > 0 else [])})

    label_stats, label_examples = _build_label_maps(df, agg, max_examples=12)

    # Tabellen
    agg_rows: List[str] = []
    if agg is not None and len(agg) > 0:
        for _, r in agg.sort_values(["group", "weighted_mentions"], ascending=[True, False]).iterrows():
            bar = f"<div class='bar' style='width:{min(100, 5*float(r['weighted_mentions']))}px'></div>"
            agg_rows.append(
                "<tr"
                f" data-group='{_esc(r['group'])}'"
                f" data-label='{_esc(r['label'])}'>"
                f"<td>{_esc(r['group'])}</td>"
                f"<td><button class='lbl' data-label='{_esc(r['label'])}'>{_esc(r['label'])}</button></td>"
                f"<td class='n'>{int(r['mentions'])}</td>"
                f"<td class='n'>{int(r['docs'])}</td>"
                f"<td class='n'>{_esc(r['weighted_mentions'])}</td>"
                f"<td>{bar}</td>"
                "</tr>"
            )

    detail_rows: List[str] = []
    if df is not None and len(df) > 0:
        cap = min(50000, len(df))
        view = df.head(cap)
        for _, r in view.iterrows():
            detail_rows.append(
                "<tr"
                f" data-group='{_esc(r['group'])}'"
                f" data-label='{_esc(r['label'])}'>"
                f"<td>{_esc(Path(r['pdf_file']).name)}</td>"
                f"<td class='n'>{int(r['page'])}</td>"
                f"<td>{_esc(r['group'])}</td>"
                f"<td><button class='lbl' data-label='{_esc(r['label'])}'>{_esc(r['label'])}</button></td>"
                f"<td><code>{_esc(r['pattern'])}</code></td>"
                f"<td>{_mark_once(str(r['context']), str(r['pattern']))}</td>"
                "</tr>"
            )

    # Chips/Buttons
    group_buttons = "".join([f"<button class='chip group' data-group='{_esc(g)}'>{_esc(g)}</button>" for g in groups])
    label_chips = "".join([f"<button class='chip label' data-label='{_esc(lbl)}'>{_esc(lbl)}</button>" for lbl in labels_sorted])

    # Templates laden
    tpl_html, tpl_css, tpl_js, all_present = _load_templates()

    # JSON-Variablen als Skript
    json_script = (
        "window.LABEL_STATS = " + json.dumps(label_stats, ensure_ascii=False) + ";\n"
        "window.LABEL_EXAMPLES = " + json.dumps(label_examples, ensure_ascii=False) + ";\n"
        "window.GROUPS = " + json.dumps(groups, ensure_ascii=False) + ";\n"
        "window.LABELS_SORTED = " + json.dumps(labels_sorted, ensure_ascii=False) + ";\n"
    )

    # Primäre Platzhalter-Substitution
    out = (
        tpl_html
        .replace("{{TITLE}}", "chroniken_library – Volltext-Erwähnungen")
        .replace("{{STAMP}}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        .replace("{{TOTAL_HITS}}", str(total_hits))
        .replace("{{TOTAL_LABELS}}", str(total_labels))
        .replace("{{TOTAL_FILES}}", str(total_files))
        .replace("{{GROUP_BUTTONS}}", group_buttons if "{{GROUP_BUTTONS}}" in tpl_html else "{{GROUP_BUTTONS}}")
        .replace("{{LABEL_CHIPS}}", label_chips if "{{LABEL_CHIPS}}" in tpl_html else "{{LABEL_CHIPS}}")
        .replace("{{AGG_ROWS}}", "\n".join(agg_rows) if "{{AGG_ROWS}}" in tpl_html else "{{AGG_ROWS}}")
        .replace("{{DETAIL_ROWS}}", "\n".join(detail_rows) if "{{DETAIL_ROWS}}" in tpl_html else "{{DETAIL_ROWS}}")
        .replace("{{INLINE_CSS}}", "{{INLINE_CSS}}")  # erst mit _inject_assets füllen
        .replace("{{INLINE_JS}}", "{{INLINE_JS}}")
        .replace("{{JSON_LABEL_STATS}}", json.dumps(label_stats, ensure_ascii=False))
        .replace("{{JSON_LABEL_EXAMPLES}}", json.dumps(label_examples, ensure_ascii=False))
        .replace("{{JSON_GROUPS}}", json.dumps(groups, ensure_ascii=False))
        .replace("{{JSON_LABELS_SORTED}}", json.dumps(labels_sorted, ensure_ascii=False))
        .replace("{{DARK_CLASS}}", "")
    )

    # Fehlende Kern-Platzhalter ({GROUP_BUTTONS, LABEL_CHIPS, AGG_ROWS, DETAIL_ROWS}) im Notfall ersetzen
    if "{{GROUP_BUTTONS}}" in out:
        out = out.replace("{{GROUP_BUTTONS}}", group_buttons)
    if "{{LABEL_CHIPS}}" in out:
        out = out.replace("{{LABEL_CHIPS}}", label_chips)
    if "{{AGG_ROWS}}" in out:
        out = out.replace("{{AGG_ROWS}}", "\n".join(agg_rows))
    if "{{DETAIL_ROWS}}" in out:
        out = out.replace("{{DETAIL_ROWS}}", "\n".join(detail_rows))

    # CSS/JS/JSON sicher injizieren
    out = _inject_assets(out, tpl_css, tpl_js, json_script)

    with open(path, "w", encoding="utf-8") as f:
        f.write(out)

    print(f"[INFO] HTML geschrieben: {path}")
    return path


# ------------------------- Metadaten / Netzwerk -------------------------

def write_meta(out_dir: Path, root: Path, pdf_dir: Path, pdfs: List[Path], cfg_file: Path, weights: Dict[str, float]) -> Path:
    meta = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(root),
        "pdf_dir": str(pdf_dir),
        "num_pdfs": len(pdfs),
        "pdfs": [str(p) for p in pdfs],
        "config_file": str(cfg_file),
        "weights": {str(k): float(v) for k, v in weights.items()},
        "env": {
            "pymupdf": True,
            "pandas": HAVE_PANDAS,
            "pytesseract": True,
            "networkx": HAVE_NX,
            "ocr_threshold_chars": TEXT_MIN_LEN
        }
    }
    out = out_dir / "session_meta.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    print(f"[INFO] Session-Metadaten geschrieben: {out}")
    return out


def maybe_write_gexf(out_dir: Path, df) -> None:
    if not HAVE_NX or df is None or len(df) == 0:
        return
    G = None
    try:
        from .aggregate import build_co_mention_network
        G = build_co_mention_network(df)
    except Exception as e:
        print(f"[WARN] Netzwerkaufbau fehlgeschlagen: {e}")
        return
    if G is not None:
        path = out_dir / "chroniken_network.gexf"
        try:
            nx.write_gexf(G, str(path))
            print(f"[INFO] GEXF geschrieben: {path}")
        except Exception as e:
            print(f"[WARN] Konnte GEXF nicht schreiben: {e}")


# ------------------------- Testmodus -------------------------

def _find_latest_session(base: Path) -> Optional[Path]:
    c = base / "chronik_data"
    if not c.exists():
        return None
    dirs = [p for p in c.iterdir() if p.is_dir() and p.name.startswith("session_")]
    if not dirs:
        return None
    return max(dirs, key=lambda p: p.stat().st_mtime)


def main() -> None:
    """Testlauf: jüngste Session suchen und Report via Templates neu schreiben."""
    if not HAVE_PANDAS:
        print("[ERROR] pandas fehlt. Installiere mit: pip install pandas")
        return
    root = project_root()
    data_base = data_base_dir(root)
    sess = _find_latest_session(data_base)
    if not sess:
        print(f"[WARN] Kein Session-Ordner unter {data_base/'chronik_data'} gefunden.")
        return
    det = sess / "chroniken_mentions.csv"
    smy = sess / "chroniken_summary.csv"
    if not det.exists() or not smy.exists():
        print(f"[WARN] CSVs fehlen im Session-Ordner: {sess}")
        return
    try:
        df = pd.read_csv(det, sep=";")
        agg = pd.read_csv(smy, sep=";")
    except Exception as e:
        print(f"[ERROR] Konnte CSVs nicht lesen: {e}")
        return
    try:
        write_html(sess, df, agg)
    except Exception as e:
        print(f"[ERROR] Report-Erstellung fehlgeschlagen: {e}")
        return
    print(f"[INFO] Report aktualisiert: {sess/'chroniken_report.html'}")


if __name__ == "__main__":
    main()