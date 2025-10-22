#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
E-Periodica: 1-Link -> 1 PDF (HTML- oder Direkt-Download)
---------------------------------------------------------
Funktion:
- Tkinter-Dialog fragt 1 E-Periodica-URL (z.B. https://www.e-periodica.ch/digbib/view?pid=szg-006:1959:9)
- Script lädt die HTML-Seite, extrahiert automatisch:
    * Titel (og:title, <h1>, <title>)
    * PID (query param 'pid')
    * IIIF System-ID & Endpoint (window.epdata.iiifSystemId / .iiifEndpointUri)
    * Alle Seiten aus window.epdata.pagesMinified
- Strategie:
    1) Versuche Gesamt-PDF via https://www.e-periodica.ch/cntmng?pid=<PID>
    2) Falls nicht verfügbar: lade alle Seiten als IIIF-Bilder in korrekter Reihenfolge
       (Identifier = <iiifSystemId>!<pageName>), Pfad: <iiifEndpoint>/IDENTIFIER/full/full/0/default.jpg
    3) Baue daraus *ein* PDF, Dateiname = bereinigter Titel
- Keine Konsolen-Inputs. Enthält ausführliche Debug-Prints.

Voraussetzungen:
    pip install requests beautifulsoup4 pypdf pillow
Getestet mit Python 3.10+
"""

import io
import os
import re
import sys
import time
import random
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote

import requests
from bs4 import BeautifulSoup
from PIL import Image
from pypdf import PdfWriter, PdfReader

# ------------------ GUI (keine Konsolen-Inputs) ------------------
try:
    import tkinter as tk
    from tkinter import simpledialog, filedialog, messagebox
    TK_OK = True
except Exception:
    TK_OK = False

# ------------------ Konfiguration ------------------
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36 "
    "(E-Periodica one-shot PDF builder; non-commercial, educational)"
)
BASE = "https://www.e-periodica.ch"
REQUEST_TIMEOUT = (10, 60)    # (connect, read) Sekunden
MAX_RETRIES = 4
BACKOFF_BASE = 1.7

# ------------------ Hilfsfunktionen ------------------
def dbg_sleep(attempt:int):
    w = (BACKOFF_BASE ** attempt) + random.uniform(0, 0.6)
    print(f"[DEBUG] Backoff {w:.2f}s …")
    time.sleep(w)

def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    return s

def http_get(s:requests.Session, url:str, stream=False) -> requests.Response:
    for i in range(1, MAX_RETRIES+1):
        try:
            print(f"[DEBUG] GET {i}/{MAX_RETRIES}: {url}")
            r = s.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True, stream=stream)
            if r.status_code in (200, 206):
                return r
            if r.status_code in (429, 500, 502, 503, 504):
                print(f"[WARN] Server-Status {r.status_code} – erneuter Versuch …")
                dbg_sleep(i)
                continue
            print(f"[ERROR] Unerwarteter Status {r.status_code} für {url}")
            return r
        except requests.RequestException as e:
            print(f"[ERROR] Netzfehler: {e}")
            dbg_sleep(i)
    raise RuntimeError(f"Konnte {url} nach {MAX_RETRIES} Versuchen nicht laden.")

def http_head(s:requests.Session, url:str) -> requests.Response | None:
    try:
        print(f"[DEBUG] HEAD: {url}")
        return s.head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    except requests.RequestException:
        return None

def is_pdf_response(resp:requests.Response) -> bool:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    disp  = (resp.headers.get("Content-Disposition") or "").lower()
    return ("application/pdf" in ctype) or (".pdf" in disp)

def sanitize_filename(name:str) -> str:
    name = re.sub(r"[^\w\-. ()\u00C0-\u017F]+", "_", name, flags=re.UNICODE)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:200] or "eperiodica_document"

def extract_pid(url:str) -> str | None:
    try:
        q = parse_qs(urlparse(url).query)
        pid = q.get("pid", [None])[0]
        if pid:
            pid = unquote(pid)
            print(f"[DEBUG] PID = {pid}")
            return pid
    except Exception as e:
        print(f"[WARN] PID konnte nicht extrahiert werden: {e}")
    return None

def pick_url_and_folder() -> tuple[str, Path]:
    if not TK_OK:
        raise RuntimeError("Tkinter nicht verfügbar – bitte Python mit Tk-Unterstützung verwenden.")
    root = tk.Tk(); root.withdraw()
    messagebox.showinfo("E-Periodica → PDF", "Bitte die E-Periodica-URL einfügen (digbib/view?pid=… oder cntmng?pid=…).")
    url = simpledialog.askstring("E-Periodica URL", "Link hier einfügen:")
    if not url:
        raise RuntimeError("Keine URL angegeben.")
    outdir = filedialog.askdirectory(title="Zielordner wählen")
    if not outdir:
        raise RuntimeError("Kein Zielordner gewählt.")
    return url.strip(), Path(outdir)

# ------------------ HTML-Parsing ------------------
def load_html(s:requests.Session, url:str) -> BeautifulSoup | None:
    r = http_get(s, url, stream=False)
    if r.status_code != 200 or "text/html" not in (r.headers.get("Content-Type") or ""):
        print(f"[WARN] Keine HTML-Antwort (Status {r.status_code})")
        return None
    return BeautifulSoup(r.text, "html.parser")

def extract_title(soup:BeautifulSoup) -> str:
    og = soup.find("meta", attrs={"property":"og:title"})
    if og and og.get("content"):
        t = og["content"].strip()
        print(f"[DEBUG] Titel (og:title): {t}")
        return t
    h1 = soup.find(["h1","h2"])
    if h1 and h1.get_text(strip=True):
        t = h1.get_text(" ", strip=True)
        print(f"[DEBUG] Titel (h1/h2): {t}")
        return t
    if soup.title and soup.title.get_text(strip=True):
        t = soup.title.get_text(" ", strip=True)
        print(f"[DEBUG] Titel (<title>): {t}")
        return t
    print("[WARN] Kein Titel gefunden – nutze 'E-Periodica Dokument'")
    return "E-Periodica Dokument"

def extract_epdata_values(html_text:str) -> tuple[str | None, str | None, list[str]]:
    """
    Holt iiifSystemId, iiifEndpointUri und die List der Seiten-JPG-IDs aus window.epdata.pagesMinified.
    Wir parsen robust via Regex, ohne auf JS-JSON-Syntax angewiesen zu sein.
    """
    # iiifSystemId
    m_sys = re.search(r"window\.epdata\.iiifSystemId\s*=\s*'([^']+)'", html_text)
    iiif_system = m_sys.group(1) if m_sys else None
    print(f"[DEBUG] iiifSystemId: {iiif_system}")

    # iiifEndpointUri
    m_ep = re.search(r"window\.epdata\.iiifEndpointUri\s*=\s*'([^']+)'", html_text)
    iiif_endpoint = m_ep.group(1) if m_ep else None
    print(f"[DEBUG] iiifEndpointUri: {iiif_endpoint}")

    # pagesMinified: alle '...jpg' in der initialisierten Array einsammeln (Reihenfolge beibehalten)
    pages = []
    # Bereich der pagesMinified isolieren, um false-positive zu minimieren
    block = re.search(r"pagesMinified\s*=\s*\[(.*?)\]\s*;", html_text, flags=re.S|re.I)
    if block:
        data = block.group(1)
        for m in re.finditer(r"\[\s*'([^']+?\.jpg)'\s*,", data):
            pages.append(m.group(1))
    else:
        print("[WARN] pagesMinified nicht gefunden – versuche generisches JPG-Muster (Fallback).")
        for m in re.finditer(r"'([^']+?\.jpg)'", html_text):
            pages.append(m.group(1))
    # Deduplizieren, Reihenfolge bewahren
    seen = set(); ordered = []
    for x in pages:
        if x not in seen:
            ordered.append(x); seen.add(x)
    print(f"[DEBUG] Seiten gefunden: {len(ordered)}")
    return iiif_system, iiif_endpoint, ordered

# ------------------ Download & PDF-Bau ------------------
def try_download_full_pdf(s:requests.Session, pid:str) -> bytes | None:
    cnt = f"{BASE}/cntmng?pid={requests.utils.quote(pid, safe=':/')}"
    # HEAD
    h = http_head(s, cnt)
    if h is not None and h.status_code == 200 and ("application/pdf" in (h.headers.get("Content-Type","").lower())):
        print("[INFO] Gesamt-PDF via cntmng (HEAD bestätigt) – lade …")
        return stream_to_bytes(s, cnt)
    # direkter GET-Fallback
    r = http_get(s, cnt, stream=True)
    if is_pdf_response(r):
        print("[INFO] Gesamt-PDF via cntmng (GET bestätigt) – lade …")
        return stream_iter_to_bytes(r)
    print("[INFO] Kein Gesamt-PDF unter /cntmng – wechsle auf IIIF-Seitenmodus.")
    return None

def stream_to_bytes(s:requests.Session, url:str) -> bytes:
    r = http_get(s, url, stream=True)
    return stream_iter_to_bytes(r)

def stream_iter_to_bytes(resp:requests.Response) -> bytes:
    out = io.BytesIO()
    size = 0
    for chunk in resp.iter_content(chunk_size=1024*256):
        if chunk:
            out.write(chunk)
            size += len(chunk)
            sys.stdout.write(f"\r[DEBUG] lade … {size/1024:.1f} KiB")
            sys.stdout.flush()
    sys.stdout.write("\n")
    return out.getvalue()

def iiif_image_url(iiif_endpoint:str, iiif_system:str, page_jpg_id:str) -> str:
    """
    Baut eine Vollauflösungs-IIIF-URL:
      <endpoint>/<system>!<page>.jpg/full/full/0/default.jpg
    Beispiel aus og:image:
      https://www.e-periodica.ch/iiif/2/e-periodica!szg!1959_009!szg-006_1959_009_0001.jpg/full/!1200,1200/0/default.jpg
    Wir nehmen 'full/full' für maximale Größe.
    """
    ident = f"{iiif_system}!{page_jpg_id}"
    # Wichtig: keine doppelte Slash-Probleme
    return f"{iiif_endpoint.rstrip('/')}/{ident}/full/full/0/default.jpg"

def image_bytes_to_pdf_page(img_bytes: bytes) -> bytes:
    im = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    buf = io.BytesIO()
    im.save(buf, format="PDF", resolution=300.0)
    return buf.getvalue()

def merge_pdfs(pages:list[bytes]) -> bytes:
    writer = PdfWriter()
    for idx, blob in enumerate(pages, start=1):
        print(f"[DEBUG] füge Seite {idx} hinzu …")
        reader = PdfReader(io.BytesIO(blob))
        for i in range(len(reader.pages)):
            writer.add_page(reader.pages[i])
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()

# ------------------ Hauptprozess ------------------
def build_pdf_from_html(s:requests.Session, url:str, out_dir:Path):
    print("[INFO] Lade HTML und extrahiere Metadaten …")
    r = http_get(s, url, stream=False)
    if r.status_code != 200 or "text/html" not in (r.headers.get("Content-Type") or ""):
        raise RuntimeError(f"HTML nicht ladbar (Status {r.status_code})")

    soup = BeautifulSoup(r.text, "html.parser")
    title = sanitize_filename(extract_title(soup))
    pid = extract_pid(url)
    iiif_system, iiif_endpoint, pages = extract_epdata_values(r.text)

    # 1) Gesamt-PDF versuchen
    full_pdf = None
    if pid:
        try:
            full_pdf = try_download_full_pdf(s, pid)
        except Exception as e:
            print(f"[WARN] cntmng-Download fehlgeschlagen: {e}")

    if full_pdf and len(full_pdf) > 150_000:
        out_file = (out_dir / f"{title}.pdf")
        out_file.write_bytes(full_pdf)
        print(f"[INFO] Gesamt-PDF gespeichert: {out_file}")
        return

    # 2) IIIF-Bilder laden
    if not iiif_system or not iiif_endpoint or not pages:
        raise RuntimeError("Fehlende IIIF-Daten oder Seitenliste – Abbruch.")

    print(f"[INFO] Lade {len(pages)} Seitenbilder über IIIF und baue PDF …")
    page_pdf_blobs: list[bytes] = []
    for idx, page_id in enumerate(pages, start=1):
        page_url = iiif_image_url(iiif_endpoint, iiif_system, page_id)
        print(f"[DEBUG] ({idx}/{len(pages)}) IIIF: {page_url}")
        try:
            img_bytes = stream_to_bytes(s, page_url)
            # Validierung grob
            if not img_bytes or img_bytes[:2] not in (b"\xff\xd8", b"\x89P"):  # JPEG/PNG
                print(f"[WARN] Unerwarteter Bildtyp/leer – konvertiere trotzdem.")
            pdf_page = image_bytes_to_pdf_page(img_bytes)
            page_pdf_blobs.append(pdf_page)
        except Exception as e:
            print(f"[ERROR] Seite {idx}: {e} – überspringe.")
        # höfliche Pause
        time.sleep(0.4 + random.uniform(0.1, 0.4))

    if not page_pdf_blobs:
        raise RuntimeError("Keine Seiten verarbeitet – PDF kann nicht gebaut werden.")

    merged = merge_pdfs(page_pdf_blobs)
    out_file = (out_dir / f"{title}.pdf")
    out_file.write_bytes(merged)
    print(f"[INFO] Zusammengeführtes PDF gespeichert: {out_file}")

def main():
    print("[INFO] E-Periodica 1-Link→1-PDF gestartet")
    s = session()
    url, out_dir = pick_url_and_folder()
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Zielordner: {out_dir}")
    print(f"[INFO] Eingabe-URL: {url}")
    build_pdf_from_html(s, url, out_dir)
    print("[INFO] Fertig. Viel Spass!")

if __name__ == "__main__":
    # keine Konsolen-Eingaben – sofort starten
    main()
