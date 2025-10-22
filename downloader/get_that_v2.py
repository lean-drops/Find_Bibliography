#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
E-Periodica Turbo-Downloader (Auto-HTTP/2 Fallback) + Seitenbereich
-------------------------------------------------------------------
- Tkinter fragt URL + Start-/Endseite (1-basiert).
- Erst Versuch: /cntmng?pid=… (Gesamt-PDF). Bei Bereich → PDF zuschneiden.
- Fallback: IIIF-Seiten (parallel, schnell) laden und zu 1 PDF bauen.
- Auto-HTTP/2: Wenn 'h2' fehlt, wird HTTP/1.1 verwendet (mit Debug-Hinweis).
- Sehr ausführliche Debug-Prints. Keine Konsolen-Eingaben im main.

Install (empfohlen, für HTTP/2-Speed):
    pip install httpx[http2] beautifulsoup4 img2pdf pillow pypdf
"""

import asyncio
import io
import re
import sys
import time
import random
import tempfile
from pathlib import Path
from typing import List, Tuple, Optional
from urllib.parse import urlparse, parse_qs, unquote

import img2pdf
from bs4 import BeautifulSoup
from PIL import Image  # nur Debug (erste Seite)
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
      "(E-Periodica turbo downloader; non-commercial, educational)")
CONNECT_TIMEOUT = 10.0
READ_TIMEOUT = 60.0
RETRIES = 4
BACKOFF_BASE = 1.8
CONCURRENCY = 14  # Parallelität (12–24 sind i.d.R. ok)

MIN_FULLPDF_BYTES = 150_000  # Heuristik gegen 1-Seiten-PDFs

# ---------- Utils ----------
def dprint(msg: str) -> None:
    print(msg, flush=True)

def sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w\-. ()\u00C0-\u017F]+", "_", name, flags=re.UNICODE)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:200] or "eperiodica_document"

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

def extract_epdata_values(html_text: str):
    # iiifSystemId
    m_sys = re.search(r"window\.epdata\.iiifSystemId\s*=\s*'([^']+)'", html_text)
    iiif_system = m_sys.group(1) if m_sys else None
    dprint(f"[DEBUG] iiifSystemId: {iiif_system}")
    # iiifEndpointUri
    m_ep = re.search(r"window\.epdata\.iiifEndpointUri\s*=\s*'([^']+)'", html_text)
    iiif_endpoint = m_ep.group(1) if m_ep else None
    dprint(f"[DEBUG] iiifEndpointUri: {iiif_endpoint}")
    # pagesMinified
    pages = []
    block = re.search(r"pagesMinified\s*=\s*\[(.*?)\]\s*;", html_text, flags=re.S | re.I)
    if block:
        data = block.group(1)
        for m in re.finditer(r"\[\s*'([^']+?\.jpg)'\s*,", data):
            pages.append(m.group(1))
    else:
        dprint("[WARN] pagesMinified nicht gefunden – JPG-Fallback.")
        for m in re.finditer(r"'([^']+?\.jpg)'", html_text):
            pages.append(m.group(1))
    # dedup, order preserved
    seen, ordered = set(), []
    for p in pages:
        if p not in seen:
            ordered.append(p); seen.add(p)
    dprint(f"[DEBUG] Seiten gefunden: {len(ordered)}")
    return iiif_system, iiif_endpoint, ordered

def iiif_image_url(iiif_endpoint: str, iiif_system: str, page_jpg_id: str) -> str:
    ident = f"{iiif_system}!{page_jpg_id}"
    return f"{iiif_endpoint.rstrip('/')}/{ident}/full/full/0/default.jpg"

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
            if blob[:2] not in (b"\xff\xd8", b"\x89P"):
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

# ---------- GUI ----------
def gui_get_url_folder_and_range() -> Tuple[str, Path, int | None, int | None]:
    if not TK_OK:
        raise RuntimeError("Tkinter ist nicht verfügbar – bitte Python mit Tk-Unterstützung nutzen.")
    root = tk.Tk(); root.withdraw()
    messagebox.showinfo(
        "E-Periodica Turbo-Downloader",
        ("Bitte E-Periodica-Link (digbib/view?pid=… oder cntmng?pid=…) einfügen.\n"
         "Danach Start- und Endseite angeben (1-basiert). Leerlassen = ganze Publikation.")
    )
    url = simpledialog.askstring("E-Periodica URL", "Link hier einfügen:")
    if not url:
        raise RuntimeError("Keine URL angegeben.")
    outdir = filedialog.askdirectory(title="Zielordner wählen")
    if not outdir:
        raise RuntimeError("Kein Zielordner gewählt.")
    s = simpledialog.askstring("Startseite", "Ab welcher Seite? (1-basiert; leer = 1)")
    e = simpledialog.askstring("Endseite",   "Bis welche Seite? (1-basiert; leer = letzte)")
    def to_int(x):
        try: return int(x) if x and x.strip() else None
        except: return None
    return url.strip(), Path(outdir), to_int(s), to_int(e)

# ---------- Hauptablauf ----------
async def process_url_to_pdf(url: str, out_dir: Path, start_page_req: int | None, end_page_req: int | None) -> None:
    # HTTP/2-Entscheid:
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
        title = sanitize_filename(parse_title(soup))
        pid = extract_pid(url)
        iiif_system, iiif_endpoint, pages_all = extract_epdata_values(html)

        total_pages = len(pages_all) if pages_all else None
        if total_pages:
            sp = start_page_req if (start_page_req and start_page_req >= 1) else 1
            ep = end_page_req if (end_page_req and end_page_req >= 1) else total_pages
            sp = max(1, min(sp, total_pages))
            ep = max(1, min(ep, total_pages))
            if sp > ep:
                sp, ep = ep, sp
            dprint(f"[INFO] Gewählter Seitenbereich: {sp}–{ep} von {total_pages} Seiten")
        else:
            sp, ep = 1, None
            dprint("[WARN] Konnte Seitenliste nicht bestimmen – behandle Bereich als 'alles'.")

        # 1) Versuch: Gesamt-PDF
        if pid:
            full_pdf = await try_download_full_pdf(client, pid)
            if full_pdf and len(full_pdf) >= MIN_FULLPDF_BYTES and full_pdf[:4] == b"%PDF":
                if total_pages and (sp != 1 or ep != total_pages):
                    dprint("[INFO] Zuschneiden des Gesamt-PDF auf gewählten Bereich …")
                    try:
                        sliced = slice_pdf_bytes(full_pdf, sp, ep)
                        out_file = out_dir / f"{title}_S{sp}-S{ep}.pdf"
                        out_file.write_bytes(sliced)
                        dprint(f"[INFO] Bereichs-PDF gespeichert: {out_file}")
                        return
                    except Exception as e:
                        dprint(f"[WARN] Zuschneiden fehlgeschlagen ({e}) – IIIF-Fallback wird genutzt.")
                else:
                    out_file = out_dir / f"{title}.pdf"
                    out_file.write_bytes(full_pdf)
                    dprint(f"[INFO] Gesamt-PDF gespeichert: {out_file}")
                    return
            else:
                dprint("[INFO] Kein nutzbares Gesamt-PDF – IIIF-Fallback wird genutzt.")

        # 2) IIIF-Fallback (nur gewählten Bereich laden)
        if not iiif_system or not iiif_endpoint or not pages_all:
            raise RuntimeError("Fehlende IIIF-Daten oder Seitenliste – Abbruch.")

        subset = pages_all[(sp-1):ep] if ep is not None else pages_all[(sp-1):]
        dprint(f"[INFO] Lade {len(subset)} Seiten parallel (Concurrency={CONCURRENCY}) …")
        with tempfile.TemporaryDirectory(prefix="eperiodica_") as tdir:
            tdir_path = Path(tdir)
            image_paths = await download_all_images(iiif_endpoint, iiif_system, subset, client, tdir_path)
            if not image_paths:
                raise RuntimeError("Kein Seitenbild geladen – Abbruch.")
            suffix = "" if (sp == 1 and (ep is None or ep == len(pages_all))) else f"_S{sp}-S{ep}"
            out_file = out_dir / f"{title}{suffix}.pdf"
            build_pdf_with_img2pdf(image_paths, out_file)
        dprint("[INFO] Fertig.")

def main():
    dprint("[INFO] E-Periodica 1-Link→1-PDF (Turbo + Range, Auto-HTTP/2) gestartet")
    url, out_dir, s_req, e_req = gui_get_url_folder_and_range()
    out_dir.mkdir(parents=True, exist_ok=True)
    dprint(f"[INFO] Zielordner: {out_dir}")
    dprint(f"[INFO] Eingabe-URL: {url}")
    asyncio.run(process_url_to_pdf(url, out_dir, s_req, e_req))

if __name__ == "__main__":
    # keine Konsolen-Eingaben – sofort loslegen
    main()
