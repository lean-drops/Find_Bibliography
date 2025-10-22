#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
get_that_chapters_separated.py
==============================
E-Periodica: Kapitel/Beiträge automatisch erkennen (IIIF Manifest) und
jeweils als eigenes PDF speichern. Bibliographien werden zuverlässig mit
eingeschlossen (Bounding-Range von erster bis letzter Canvas-Seite je Kapitel).

NEU:
- Dateiname: "INDEX - AUTOR1; AUTOR2 - TITEL (Sx-Sy).pdf"
- Autoren-Erkennung aus IIIF-Range-Metadaten (v2/v3) + sanfte Label-Heuristiken.
- Manifest-Auflösung via iiifPresentationBaseUrl + docPid (robust für E-Periodica).

Leistungsmerkmale
-----------------
- Tkinter GUI: fragt 1 Link (digbib/view?pid=… oder cntmng?pid=…) + Zielordner.
- Kapitel-Erkennung über IIIF-Presentation-Manifest (v2/v3), robust & rekursiv.
- Pro Kapitel: bevorzugt Zuschneiden eines vorhandenen Gesamt-PDFs (/cntmng).
- Fallback: IIIF-Seiten (JPG/PNG) parallel laden und zu PDF bauen.
- Auto-HTTP/2: nutzt httpx mit http2, fällt sonst auf HTTP/1.1 (mit Hinweis) zurück.
- Sehr ausführliche Debug-Prints. Kein Konsolen-Input im main (nur GUI).

Install
-------
    pip install httpx[http2] beautifulsoup4 img2pdf pillow pypdf
"""

import asyncio
import io
import json
import re
import sys
import time
import random
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Set
from urllib.parse import urlparse, parse_qs, unquote

import img2pdf
from bs4 import BeautifulSoup
from PIL import Image  # nur für Debug der ersten Seite
import httpx
from pypdf import PdfReader, PdfWriter

# ---------- GUI ----------
try:
    import tkinter as tk
    from tkinter import simpledialog, filedialog, messagebox
    TK_OK = True
except Exception:
    TK_OK = False

# ---------- Auto-HTTP/2-Erkennung ----------
try:
    import h2  # noqa: F401
    HTTP2_AVAILABLE = True
except Exception:
    HTTP2_AVAILABLE = False

# ---------- Konfiguration ----------
BASE = "https://www.e-periodica.ch"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Safari/537.36 "
      "(E-Periodica chapter downloader; non-commercial, educational)")
CONNECT_TIMEOUT = 12.0
READ_TIMEOUT = 90.0
RETRIES = 4
BACKOFF_BASE = 1.8
CONCURRENCY = 14  # Parallelität (12–24 sind i.d.R. ok)
MIN_FULLPDF_BYTES = 150_000  # Heuristik gegen 1-Seiten-PDFs
MAX_EXPECTED_CHAPTERS = 1200  # Sicherheitsgrenze

# ---------- Utils ----------
def dprint(msg: str) -> None:
    print(msg, flush=True)

def sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w\-. ()\u00C0-\u017F]+", "_", name, flags=re.UNICODE)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:200] or "eperiodica_document"

def shorten_for_filename(text: str, max_len: int = 120) -> str:
    text = text.strip()
    return text if len(text) <= max_len else (text[: max_len - 1].rstrip() + "…")

def strip_html(text: str) -> str:
    try:
        return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    except Exception:
        return re.sub(r"<[^>]+>", " ", text or "").strip()

def extract_pid(url: str) -> Optional[str]:
    try:
        q = parse_qs(urlparse(url).query)
        pid = q.get("pid", [None])[0]
        if pid:
            pid = unquote(pid)
            dprint(f"[DEBUG] PID = {pid}")
            return pid
    except Exception as e:
        dprint(f"[WARN] PID konnte nicht extrahiert werden: {e}")
    return None

def parse_title(soup: BeautifulSoup) -> str:
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        t = og["content"].strip()
        dprint(f"[DEBUG] Titel (og:title): {t}")
        return t
    h1 = soup.find(["h1", "h2"])
    if h1 and h1.get_text(strip=True):
        t = h1.get_text(" ", strip=True)
        dprint(f"[DEBUG] Titel (h1/h2): {t}")
        return t
    if soup.title and soup.title.get_text(strip=True):
        t = soup.title.get_text(" ", strip=True)
        dprint(f"[DEBUG] Titel (<title>): {t}")
        return t
    dprint("[WARN] Kein Titel gefunden – Fallback")
    return "E-Periodica Dokument"

def backoff_sleep(attempt: int) -> None:
    import time as _t, random as _r
    wait = (BACKOFF_BASE ** attempt) + _r.uniform(0, 0.6)
    dprint(f"[DEBUG] Backoff {wait:.2f}s …")
    _t.sleep(wait)

# ---------- HTTP (async) ----------
def httpx_limits():
    return httpx.Limits(max_keepalive_connections=CONCURRENCY * 2,
                        max_connections=CONCURRENCY * 2)

def httpx_timeout():
    return httpx.Timeout(CONNECT_TIMEOUT, read=READ_TIMEOUT)

async def fetch_bytes(client: httpx.AsyncClient, url: str) -> bytes:
    for i in range(1, RETRIES + 1):
        try:
            dprint(f"[DEBUG] GET {i}/{RETRIES}: {url}")
            r = await client.get(url, follow_redirects=True)
            if r.status_code in (200, 206):
                return r.content
            if r.status_code in (429, 500, 502, 503, 504):
                dprint(f"[WARN] Status {r.status_code} – retry …")
                backoff_sleep(i)
                continue
            dprint(f"[ERROR] Unerwarteter Status {r.status_code} für {url}")
            return b""
        except httpx.RequestError as e:
            dprint(f"[ERROR] Netzfehler: {e}")
            backoff_sleep(i)
    return b""

async def fetch_json(client: httpx.AsyncClient, url: str) -> Optional[dict]:
    raw = await fetch_bytes(client, url)
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8", errors="ignore"))
    except Exception as e:
        dprint(f"[ERROR] JSON parse failed for {url}: {e}")
        return None

async def head_is_pdf(client: httpx.AsyncClient, url: str) -> bool:
    try:
        dprint(f"[DEBUG] HEAD: {url}")
        r = await client.head(url, follow_redirects=True)
        ctype = (r.headers.get("Content-Type") or "").lower()
        disp  = (r.headers.get("Content-Disposition") or "").lower()
        return r.status_code == 200 and ("application/pdf" in ctype or ".pdf" in disp)
    except httpx.RequestError:
        return False

async def try_download_full_pdf(client: httpx.AsyncClient, pid: str) -> bytes:
    url = f"{BASE}/cntmng?pid={pid}"
    if await head_is_pdf(client, url):
        dprint("[INFO] Gesamt-PDF via /cntmng bestätigt (HEAD) – lade …")
        return await fetch_bytes(client, url)
    dprint("[INFO] Versuche Direkt-GET auf /cntmng …")
    return await fetch_bytes(client, url)

# ---------- E-Periodica spezifische Extraktion ----------
def extract_epdata_values(html_text: str):
    """
    Extrahiert wichtige epdata-Felder aus dem HTML.
    Rückgabe:
      (iiif_system, iiif_endpoint_image, pages_ordered, iiif_manifest_explicit,
       iiif_presentation_base, doc_pid)
    """
    m_sys = re.search(r"window\.epdata\.iiifSystemId\s*=\s*'([^']+)'", html_text)
    iiif_system = m_sys.group(1) if m_sys else None
    dprint(f"[DEBUG] iiifSystemId: {iiif_system}")

    m_ep = re.search(r"window\.epdata\.iiifEndpointUri\s*=\s*'([^']+)'", html_text)
    iiif_endpoint = m_ep.group(1) if m_ep else None
    dprint(f"[DEBUG] iiifEndpointUri (image): {iiif_endpoint}")

    m_mu = re.search(r"window\.epdata\.iiifManifestUri\s*=\s*'([^']+)'", html_text)
    iiif_manifest = m_mu.group(1) if m_mu else None
    dprint(f"[DEBUG] iiifManifestUri: {iiif_manifest}")

    m_pb = re.search(r"window\.epdata\.iiifPresentationBaseUrl\s*=\s*'([^']+)'", html_text)
    iiif_presentation_base = m_pb.group(1) if m_pb else None
    dprint(f"[DEBUG] iiifPresentationBaseUrl: {iiif_presentation_base}")

    m_dp = re.search(r"window\.epdata\.docPid\s*=\s*'([^']+)'", html_text)
    doc_pid = m_dp.group(1) if m_dp else None
    dprint(f"[DEBUG] docPid: {doc_pid}")

    pages = []
    block = re.search(r"pagesMinified\s*=\s*\[(.*?)\]\s*;", html_text, flags=re.S | re.I)
    if block:
        data = block.group(1)
        for m in re.finditer(r"\[\s*'([^']+?\.jpg)'\s*,", data):
            pages.append(m.group(1))
    else:
        dprint("[WARN] pagesMinified nicht gefunden – JPG-Fallback (alle '.jpg' im HTML).")
        for m in re.finditer(r"'([^']+?\.jpg)'", html_text):
            pages.append(m.group(1))

    seen, ordered = set(), []
    for p in pages:
        if p not in seen:
            ordered.append(p); seen.add(p)
    dprint(f"[DEBUG] Seiten gefunden: {len(ordered)}")

    return iiif_system, iiif_endpoint, ordered, iiif_manifest, iiif_presentation_base, doc_pid

def iiif_image_url(iiif_endpoint: str, iiif_system: str, page_jpg_id: str) -> str:
    ident = f"{iiif_system}!{page_jpg_id}"
    return f"{iiif_endpoint.rstrip('/')}/{ident}/full/full/0/default.jpg"

def guess_manifest_urls_from_presentation(base: Optional[str], pid: Optional[str]) -> List[str]:
    urls: List[str] = []
    if base and pid:
        b = base.rstrip('/')
        urls.append(f"{b}/manifest?pid={pid}")
        urls.append(f"{b}/{pid}/manifest")
        urls.append(f"{b}/manifest/{pid}")
        urls.append(f"{b}/presentation/{pid}/manifest")
        urls.append(f"{b}/presentation/{pid}/manifest.json")
    return list(dict.fromkeys(urls))

def guess_manifest_urls_from_html(html: str) -> List[str]:
    urls: List[str] = []
    urls += re.findall(r"https?://[^\"']+/iiif/manifest\?pid=[^\"']+", html, flags=re.I)
    urls += re.findall(r"https?://[^\"']+/iiif/[^\"']*/manifest[^\"']*", html, flags=re.I)
    return list(dict.fromkeys(urls))

# ---------- IIIF: Label/Value Normalisierung & Autorenerkennung ----------
def _textify(obj: Any) -> str:
    if obj is None:
        return ""
    if isinstance(obj, str):
        return strip_html(obj)
    if isinstance(obj, dict):
        for key in ("de", "en", "fr", "it", "none", "@value", "value"):
            if key in obj:
                return _textify(obj[key])
        # Fallback: alle Values zusammenziehen
        parts = [_textify(v) for v in obj.values()]
        parts = [p for p in parts if p]
        return "; ".join(parts)
    if isinstance(obj, list):
        parts = [_textify(x) for x in obj]
        parts = [p for p in parts if p]
        return "; ".join(parts)
    return str(obj)

def _label_to_text(label: Any) -> str:
    # nutzt _textify, aber hält Reihenfolge-Präferenz
    return _textify(label) or "Unbenannt"

AUTHOR_KEYS = {
    "autor", "autoren", "author", "authors", "creator", "creators",
    "verfasser", "verfasserin", "verfasserinnen", "herausgeber", "contributors"
}

def _split_authors(author_text: str) -> List[str]:
    """Konservative Zerlegung: NICHT an Komma splitten, um 'Nachname, Vorname' zu erhalten."""
    if not author_text:
        return []
    # Erst an offensichtlichen Trennern:
    parts = re.split(r"\s*;\s*|\s*/\s*|\s*\|\s*|<br\s*/?>|\s*·\s*|\s*•\s*", author_text, flags=re.I)
    out: List[str] = []
    for p in parts:
        p = strip_html(p).strip(" ;|/·•-—–").strip()
        if not p:
            continue
        # Falls ' und ' / ' and ' vorkommt und KEIN typisches Nachname, Vorname-Komma enthält:
        if (" und " in p or " and " in p) and not re.search(r",\s*\w", p):
            sub = re.split(r"\s+und\s+|\s+and\s+", p)
            for s in sub:
                s = s.strip(" ;|/·•-—–").strip()
                if s:
                    out.append(s)
        else:
            out.append(p)
    # Deduplizieren, Reihenfolge wahren
    seen = set(); ordered = []
    for a in out:
        if a not in seen:
            ordered.append(a); seen.add(a)
    return ordered

def extract_authors_from_iiif_node(node: dict) -> List[str]:
    """Sucht in Range-Metadaten nach Autoren."""
    authors: List[str] = []
    meta = node.get("metadata") or []
    if isinstance(meta, list):
        for m in meta:
            if not isinstance(m, dict):
                continue
            label = _textify(m.get("label"))
            value = _textify(m.get("value"))
            if not label or not value:
                continue
            if any(k in label.lower() for k in AUTHOR_KEYS):
                authors.extend(_split_authors(value))
    # Dedupe
    seen = set(); ordered = []
    for a in authors:
        if a and a not in seen:
            ordered.append(a); seen.add(a)
    return ordered

def parse_title_and_authors_from_label(label: str) -> Tuple[str, List[str]]:
    """
    Versucht, aus dem Range-Label 'Autor – Titel' oder 'Titel / Autor' zu erkennen.
    Very gentle: Wir verändern den Titel nur, wenn das Muster klar ist.
    """
    txt = label.strip()
    # Muster 1: "Autor – Titel" / "Autor — Titel"
    m = re.match(r"^\s*(.+?)\s*[–—]\s*(.+?)\s*$", txt)
    if m:
        left, right = m.group(1).strip(), m.group(2).strip()
        # Heuristik: wenn rechter Teil länger aussieht (typisch Titel), dann links=Autor(en)
        if len(right) >= len(left):
            return right, _split_authors(left)
    # Muster 2: "Titel / Autor"
    m = re.match(r"^\s*(.+?)\s*/\s*(.+?)\s*$", txt)
    if m:
        left, right = m.group(1).strip(), m.group(2).strip()
        # Hier ist links meist der Titel
        return left, _split_authors(right)
    # Muster 3: "Autor: Titel"
    m = re.match(r"^\s*(.+?)\s*:\s*(.+?)\s*$", txt)
    if m:
        left, right = m.group(1).strip(), m.group(2).strip()
        if len(right) >= len(left):
            return right, _split_authors(left)
    return label, []

# ---------- IIIF Manifest Parsing ----------
def _collect_canvases_v2(manifest: dict) -> List[str]:
    seqs = manifest.get("sequences", []) or []
    if not seqs:
        return []
    canvases = seqs[0].get("canvases", []) or []
    return [c.get("@id") or "" for c in canvases if isinstance(c, dict)]

def _collect_canvases_v3(manifest: dict) -> List[str]:
    items = manifest.get("items", []) or []
    return [c.get("id") or "" for c in items if isinstance(c, dict)]

def _range_items_to_canvas_ids_v2(rng: dict) -> List[str]:
    out: List[str] = []
    for c in rng.get("canvases", []) or []:
        if isinstance(c, str):
            out.append(c)
    return out

def _range_items_to_canvas_ids_v3(rng: dict) -> List[str]:
    out: List[str] = []
    for it in rng.get("items", []) or []:
        if isinstance(it, dict):
            cid = it.get("id")
            if isinstance(cid, str):
                out.append(cid)
    return out

def _flatten_ranges_with_node(manifest: dict) -> List[dict]:
    """Gibt alle leaf-Ranges zurück, inkl. Range-Node (für Metadatenzugriff)."""
    is_v3 = "items" in manifest and isinstance(manifest.get("items"), list)
    structures = manifest.get("structures", []) or []
    leaves: List[dict] = []

    def rec(r: dict):
        child_keys = ("ranges", "items") if is_v3 else ("ranges",)
        children = []
        for k in child_keys:
            v = r.get(k)
            if isinstance(v, list):
                children.extend([c for c in v if isinstance(c, dict)])
        for c in children:
            rec(c)
        canv = _range_items_to_canvas_ids_v3(r) if is_v3 else _range_items_to_canvas_ids_v2(r)
        if canv:
            leaves.append({
                "label": _label_to_text(r.get("label")),
                "canvases": canv,
                "node": r
            })

    for top in structures:
        if isinstance(top, dict):
            rec(top)
    return leaves

def extract_chapters_with_authors(manifest: dict,
                                  pages_all: List[str]) -> List[dict]:
    """
    Liefert pro Kapitel ein Dict:
      {
        "title": <Kapitel-Titel>,
        "authors": [..],
        "sp": <start 1-basiert>,
        "ep": <ende 1-basiert>,
        "idxs": [alle Seitenindices der Bounding-Range]
      }
    """
    if "items" in manifest:
        canvas_ids = _collect_canvases_v3(manifest)
    else:
        canvas_ids = _collect_canvases_v2(manifest)
    canvas_ids = [c for c in canvas_ids if isinstance(c, str) and c]

    if not canvas_ids:
        dprint("[WARN] Manifest enthält keine Canvas-Liste – Kapitel-Erkennung unmöglich.")
        return []

    id_to_idx: Dict[str, int] = {cid: i+1 for i, cid in enumerate(canvas_ids)}  # 1-basiert
    dprint(f"[DEBUG] Canvases im Manifest: {len(canvas_ids)}")

    leaves = _flatten_ranges_with_node(manifest)
    dprint(f"[INFO] Ranges (Blätter) mit Canvas-Referenzen: {len(leaves)}")
    if len(leaves) > MAX_EXPECTED_CHAPTERS:
        dprint(f"[ERROR] Extrem viele Kapitel erkannt ({len(leaves)} > {MAX_EXPECTED_CHAPTERS}). Abbruch zur Sicherheit.")
        return []

    results: List[dict] = []
    for leaf in leaves:
        base_label = leaf.get("label") or "Unbenannt"
        node = leaf.get("node") or {}
        canvases = leaf.get("canvases") or []

        # Seitenindices
        idxs: List[int] = []
        for cid in canvases:
            if cid in id_to_idx:
                idxs.append(id_to_idx[cid])
        if not idxs:
            continue
        idxs_sorted = sorted(set(idxs))
        sp, ep = min(idxs_sorted), max(idxs_sorted)
        full_span_indices = list(range(sp, ep + 1))

        # Autoren aus Metadaten + Label-Heuristik
        authors = extract_authors_from_iiif_node(node)
        derived_title, label_authors = parse_title_and_authors_from_label(base_label)
        if not authors and label_authors:
            authors = label_authors
        title = derived_title  # Titel ggf. aus Label-Parsing korrigiert

        results.append({
            "title": title,
            "authors": authors,
            "sp": sp,
            "ep": ep,
            "idxs": full_span_indices
        })

    dprint(f"[INFO] Kapitel nach Bereinigung: {len(results)}")
    return results

# ---------- Parallel-Download & PDF-Bau ----------
async def download_all_images(iiif_endpoint: str,
                              iiif_system: str,
                              pages: List[str],
                              client: httpx.AsyncClient,
                              temp_dir: Path) -> List[Path]:
    sem = asyncio.Semaphore(CONCURRENCY)
    out_paths: List[Path] = [temp_dir / f"page_{i:05d}.bin" for i in range(1, len(pages) + 1)]

    async def worker(idx: int, page_id: str):
        url = iiif_image_url(iiif_endpoint, iiif_system, page_id)
        async with sem:
            dprint(f"[DEBUG] ({idx+1}/{len(pages)}) IIIF: {url}")
            blob = await fetch_bytes(client, url)
            if not blob:
                dprint(f"[ERROR] Seite {idx+1}: leer/Fehler – finaler Wiederholungsversuch …")
                blob = await fetch_bytes(client, url)
            if not blob:
                dprint(f"[ERROR] Seite {idx+1}: dauerhaft fehlgeschlagen.")
                return
            if blob[:2] not in (b"\xff\xd8", b"\x89P"):  # JPEG/PNG
                dprint(f"[WARN] Seite {idx+1}: kein reines JPEG/PNG, Größe {len(blob)} Bytes.")
            out_paths[idx].write_bytes(blob)

    await asyncio.gather(*(worker(i, pid) for i, pid in enumerate(pages)))
    done_paths = [p for p in out_paths if p.exists() and p.stat().st_size > 0]
    if len(done_paths) != len(pages):
        dprint(f"[WARN] {len(pages) - len(done_paths)} Seiten konnten nicht geladen werden.")
    return done_paths

def build_pdf_with_img2pdf(image_paths: List[Path], out_file: Path) -> None:
    if not image_paths:
        raise RuntimeError("Keine Bilddateien für den PDF-Bau vorhanden.")
    dprint("[INFO] Erzeuge End-PDF mit img2pdf …")
    try:
        with Image.open(image_paths[0]) as im0:
            dprint(f"[DEBUG] Erste Seite: {im0.format} {im0.size[0]}x{im0.size[1]} px")
    except Exception:
        pass
    with open(out_file, "wb") as f:
        f.write(img2pdf.convert([str(p) for p in image_paths]))
    dprint(f"[INFO] PDF gespeichert: {out_file} ({out_file.stat().st_size} Bytes)")

def slice_pdf_bytes(full_pdf: bytes, start_page_1: int, end_page_1: int) -> bytes:
    reader = PdfReader(io.BytesIO(full_pdf))
    total = len(reader.pages)
    dprint(f"[DEBUG] Voll-PDF Seiten gesamt: {total}")
    s = max(1, min(start_page_1 or 1, total))
    e = max(1, min(end_page_1 or total, total))
    if s > e:
        s, e = e, s
    writer = PdfWriter()
    for i in range(s-1, e):
        writer.add_page(reader.pages[i])
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()

# ---------- Manifest-Ermittlung ----------
async def resolve_manifest_url(html: str,
                               iiif_endpoint: Optional[str],
                               iiif_system: Optional[str],
                               iiif_presentation_base: Optional[str],
                               doc_pid: Optional[str],
                               client: httpx.AsyncClient) -> Optional[str]:
    m = re.search(r"window\.epdata\.iiifManifestUri\s*=\s*'([^']+)'", html)
    if m:
        cand = m.group(1).strip()
        dprint(f"[INFO] Manifest-URL (epdata): {cand}")
        test = await fetch_json(client, cand)
        if test and isinstance(test, dict):
            return cand
        dprint("[WARN] epdata-Manifest-URL antwortet nicht als JSON – versuche weitere Kandidaten …")

    for cand in guess_manifest_urls_from_presentation(iiif_presentation_base, doc_pid):
        dprint(f"[DEBUG] Probiere Manifest (presentation+pid): {cand}")
        test = await fetch_json(client, cand)
        if test and isinstance(test, dict) and ("sequences" in test or "items" in test):
            dprint("[INFO] Manifest gefunden (presentation base + pid).")
            return cand

    for cand in guess_manifest_urls_from_html(html):
        dprint(f"[DEBUG] Kandidat aus HTML mit 'manifest': {cand}")
        test = await fetch_json(client, cand)
        if test and isinstance(test, dict) and ("sequences" in test or "items" in test):
            dprint("[INFO] Manifest gefunden (HTML-Hinweis).")
            return cand

    if doc_pid:
        generic_bases = [f"{BASE}/iiif", "https://iiif.library.ethz.ch/iiif"]
        for g in generic_bases:
            for cand in guess_manifest_urls_from_presentation(g, doc_pid):
                dprint(f"[DEBUG] Letzter Versuch (generische base): {cand}")
                test = await fetch_json(client, cand)
                if test and isinstance(test, dict) and ("sequences" in test or "items" in test):
                    dprint("[INFO] Manifest gefunden (generische base + pid).")
                    return cand

    dprint("[ERROR] Konnte keine IIIF-Manifest-URL ermitteln.")
    return None

# ---------- GUI ----------
def gui_get_url_and_folder() -> Tuple[str, Path]:
    if not TK_OK:
        raise RuntimeError("Tkinter ist nicht verfügbar – bitte Python mit Tk-Unterstützung nutzen.")
    root = tk.Tk(); root.withdraw()
    messagebox.showinfo(
        "E-Periodica Kapitel-Downloader",
        ("Bitte E-Periodica-Link (digbib/view?pid=… oder cntmng?pid=…) einfügen.\n"
         "Es werden automatisch alle Kapitel/Beiträge erkannt und als PDFs gespeichert.\n"
         "Dateiname: 'INDEX - AUTOR1; AUTOR2 - TITEL (Sx-Sy).pdf'")
    )
    url = simpledialog.askstring("E-Periodica URL", "Link hier einfügen:")
    if not url:
        raise RuntimeError("Keine URL angegeben.")
    outdir = filedialog.askdirectory(title="Zielordner wählen")
    if not outdir:
        raise RuntimeError("Kein Zielordner gewählt.")
    return url.strip(), Path(outdir)

# ---------- Hauptlogik ----------
async def process_url_split_chapters(url: str, out_dir: Path) -> None:
    http2_flag = HTTP2_AVAILABLE
    dprint(f"[INFO] HTTP/2 verfügbar: {http2_flag} "
           f"({'nutze HTTP/2' if http2_flag else 'Fallback auf HTTP/1.1 – pip install httpx[http2] für mehr Speed'})")

    async with httpx.AsyncClient(http2=http2_flag,
                                 headers={"User-Agent": UA},
                                 timeout=httpx_timeout(),
                                 limits=httpx_limits()) as client:
        dprint("[INFO] Lade HTML und extrahiere Metadaten …")
        html_bytes = await fetch_bytes(client, url)
        if not html_bytes:
            raise RuntimeError("HTML nicht ladbar.")
        html = html_bytes.decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html, "html.parser")
        global_title = sanitize_filename(parse_title(soup))
        pid_from_url = extract_pid(url)

        (iiif_system,
         iiif_endpoint,
         pages_all,
         iiif_manifest,
         iiif_presentation_base,
         doc_pid) = extract_epdata_values(html)

        manifest_url = iiif_manifest or await resolve_manifest_url(
            html, iiif_endpoint, iiif_system, iiif_presentation_base, (doc_pid or pid_from_url), client
        )
        if not manifest_url:
            raise RuntimeError("IIIF-Manifest nicht auffindbar – Kapitel-Erkennung nicht möglich.")
        dprint(f"[INFO] Verwende Manifest: {manifest_url}")

        manifest = await fetch_json(client, manifest_url)
        if not manifest:
            raise RuntimeError("Manifest konnte nicht geladen/geparst werden.")

        # Kapitel + Autoren
        chapters = extract_chapters_with_authors(manifest, pages_all)
        if not chapters:
            raise RuntimeError("Keine Kapitel/Beiträge im Manifest gefunden.")

        # Gesamt-PDF als Cut-Quelle?
        full_pdf: Optional[bytes] = None
        final_pid = doc_pid or pid_from_url
        if final_pid:
            dprint("[INFO] Prüfe, ob Gesamt-PDF verfügbar ist …")
            blob = await try_download_full_pdf(client, final_pid)
            if blob and len(blob) >= MIN_FULLPDF_BYTES and blob[:4] == b"%PDF":
                full_pdf = blob
                dprint(f"[INFO] Gesamt-PDF verfügbar (Größe: {len(blob)} Bytes).")
            else:
                dprint("[INFO] Kein nutzbares Gesamt-PDF – werde IIIF-Bilder pro Kapitel verwenden.")
        else:
            dprint("[WARN] Keine PID für /cntmng ermittelbar – direkt IIIF-Bilder verwenden.")

        if not pages_all:
            dprint("[WARN] Konnte pagesMinified nicht bestimmen; IIIF-Download basiert nur auf Spannen-Indices.")

        dprint(f"[INFO] Starte Kapitel-Export: {len(chapters)} Kapitel erkannt.")
        chapters.sort(key=lambda c: (c["sp"], c["ep"], (c["title"] or "").lower()))

        summary = []

        for idx, ch in enumerate(chapters, start=1):
            title = ch["title"] or "Unbenannt"
            authors = ch.get("authors") or []
            sp, ep, idxs = ch["sp"], ch["ep"], ch["idxs"]

            author_str = "; ".join(authors)
            safe_title = sanitize_filename(shorten_for_filename(title, 160))
            safe_authors = sanitize_filename(shorten_for_filename(author_str, 160)) if author_str else ""

            prefix = f"{idx:03d}"
            suffix = f"S{sp}-S{ep}"
            if safe_authors:
                out_name = f"{prefix} - {safe_authors} - {safe_title} ({suffix}).pdf"
            else:
                out_name = f"{prefix} - {safe_title} ({suffix}).pdf"
            out_file = out_dir / out_name

            dprint(f"[INFO] === Kapitel {idx}/{len(chapters)} ===")
            dprint(f"[INFO] Titel: {title}")
            dprint(f"[INFO] Autoren: {author_str if author_str else '(keine Angabe)'}")
            dprint(f"[INFO] Seiten: {sp}–{ep} (inkl. Bibliographie via Bounding-Range)")
            dprint(f"[INFO] Ziel-Datei: {out_file}")

            if out_file.exists() and out_file.stat().st_size > 0:
                dprint("[WARN] Ziel existiert bereits – überspringe (bereits vorhanden).")
                summary.append({
                    "index": idx, "title": title, "authors": authors,
                    "file": str(out_file), "span": [sp, ep], "skipped_existing": True
                })
                continue

            try:
                if full_pdf is not None:
                    dprint("[INFO] Zuschneiden aus Gesamt-PDF …")
                    try:
                        sliced = slice_pdf_bytes(full_pdf, sp, ep)
                        out_file.write_bytes(sliced)
                        dprint(f"[INFO] Kapitel-PDF gespeichert (Cut): {out_file}")
                        summary.append({
                            "index": idx, "title": title, "authors": authors,
                            "file": str(out_file), "span": [sp, ep], "method": "cut"
                        })
                        continue
                    except Exception as e:
                        dprint(f"[WARN] Zuschneiden fehlgeschlagen ({e}) – IIIF-Fallback für dieses Kapitel.")

                if not iiif_system or not iiif_endpoint:
                    raise RuntimeError("Fehlende IIIF-Daten für Bild-Download.")
                if not pages_all:
                    raise RuntimeError("Seitenliste (pagesMinified) leer – kann IIIF-Seiten nicht mappen.")

                subset = [pages_all[i-1] for i in idxs if 1 <= i <= len(pages_all)]
                dprint(f"[INFO] Lade {len(subset)} Seiten parallel (Concurrency={CONCURRENCY}) …")
                with tempfile.TemporaryDirectory(prefix="eperi_chapter_") as tdir:
                    tdir_path = Path(tdir)
                    image_paths = await download_all_images(iiif_endpoint, iiif_system, subset, client, tdir_path)
                    if not image_paths:
                        raise RuntimeError("Kein Seitenbild geladen – Abbruch für dieses Kapitel.")
                    build_pdf_with_img2pdf(image_paths, out_file)

                dprint(f"[INFO] Kapitel-PDF gespeichert (IIIF): {out_file}")
                summary.append({
                    "index": idx, "title": title, "authors": authors,
                    "file": str(out_file), "span": [sp, ep], "method": "iiif"
                })
            except Exception as e:
                dprint(f"[ERROR] Kapitel {idx} fehlgeschlagen: {e}")
                summary.append({
                    "index": idx, "title": title, "authors": authors,
                    "error": str(e), "span": [sp, ep]
                })

        summary_file = out_dir / f"{global_title} - chapters_summary.json"
        try:
            summary_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            dprint(f"[INFO] Zusammenfassung geschrieben: {summary_file}")
        except Exception as e:
            dprint(f"[WARN] Konnte Zusammenfassung nicht schreiben: {e}")

        dprint("[INFO] Fertig – alle Kapitel verarbeitet.")

def main():
    dprint("[INFO] E-Periodica Kapitel-Downloader (IIIF + Auto-HTTP/2) gestartet")
    url, out_dir = gui_get_url_and_folder()
    out_dir.mkdir(parents=True, exist_ok=True)
    dprint(f"[INFO] Zielordner: {out_dir}")
    dprint(f"[INFO] Eingabe-URL: {url}")
    asyncio.run(process_url_split_chapters(url, out_dir))

if __name__ == "__main__":
    # keine Konsolen-Eingaben – sofort loslegen
    main()