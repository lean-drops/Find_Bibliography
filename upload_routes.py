#!/usr/bin/env python3
"""
Blueprint  /        –  PDF-Upload + Lookup
──────────────────────────────────────────
•  GET  /                  Upload-Formular  (index.html)
•  POST /                  Batch-Analyse aller hochgeladenen PDFs
•  GET  /lookup            JSON-Lookup per Hash / Titel / Autor
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from dotenv import load_dotenv
from flask import (
    Blueprint,
    abort,
    current_app,
    jsonify,
    render_template,
    request,
)
from werkzeug.datastructures import FileStorage
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.utils import secure_filename, safe_join

from services.ingesting_service import ingest_single      # 1-PDF-Pipeline
from services.read             import get_stats           # /lookup-Helper

# ──────────────────────────  Konfiguration  ──────────────────────────────
load_dotenv()

LOG = logging.getLogger("upload")
LOG.setLevel(os.getenv("UPLOAD_LOG", "INFO").upper())

UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", tempfile.gettempdir())) / "bib_finder"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAX_WORKERS  = int(os.getenv("WEB_WORKERS", os.cpu_count() or 4))
MAX_SIZE_MB  = int(os.getenv("MAX_PDF_MB", "50"))
ALLOWED_MIME = {"application/pdf"}

# ──────────────────────────  Blueprint  ──────────────────────────────────
upload_bp = Blueprint(
    "upload",
    __name__,
    template_folder="templates",
    static_folder="static",
)

# ────────────────────────  Hilfsfunktionen  ─────────────────────────────
def _error(msg: str, code: int = 400):
    """XHR → JSON, sonst HTML-Error."""
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify(error=msg), code
    abort(code, msg)


def _validate_file(f: FileStorage) -> None:
    """MIME- & Größen-Check; wirft Exception bei Fehler."""
    if f.mimetype not in ALLOWED_MIME:
        raise ValueError(f"Nur PDF erlaubt, nicht “{f.mimetype}”")

    f.seek(0, os.SEEK_END)
    size_mb = f.tell() / (1024 * 1024)
    f.seek(0)
    if size_mb > MAX_SIZE_MB:
        raise RequestEntityTooLarge(f"Datei > {MAX_SIZE_MB} MB")


def _safe_path(fname: str) -> Path:
    """Verhindert Path-Traversal & erzeugt Ziel-Pfad im Upload-Ordner."""
    safe = secure_filename(fname) or "upload.pdf"
    return Path(safe_join(UPLOAD_DIR, safe))  # type: ignore[arg-type]


def _save_file(f: FileStorage) -> Path:
    """Speichert FileStorage im UPLOAD_DIR und gibt Pfad zurück."""
    _validate_file(f)

    stem      = Path(f.filename or "upload").stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst       = _safe_path(f"{stem}_{timestamp}.pdf")

    f.save(dst)
    LOG.debug("saved %s (%s bytes)", dst.name, dst.stat().st_size)
    return dst


def _run_pipeline(pdf_path: Path) -> Tuple[str, Tuple[int, int] | None]:
    """
    1 · ingest_single() ⇒ legt alles in der DB an
    2 · liefert (Dateiname, bounds | None)
    """
    try:
        return ingest_single(pdf_path)
    except Exception as exc:           # ingest-Fehler nicht abstürzen lassen
        LOG.exception("Analyse fehlgeschlagen: %s", pdf_path.name)
        return pdf_path.name, None

# ───────────────────────────  Routes  ────────────────────────────────────
@upload_bp.route("/", methods=["GET", "POST"])
def index():
    # 1) GET – Upload-Formular
    if request.method == "GET":
        return render_template("index.html")

    # 2) POST – Batch-Upload
    files: List[FileStorage] = request.files.getlist("safe")
    if not files or files[0].filename == "":
        return _error("Keine Datei ausgewählt …")

    # 2a · Dateien speichern
    try:
        paths = [_save_file(f) for f in files]
    except (ValueError, RequestEntityTooLarge) as exc:
        LOG.warning("Upload abgewiesen: %s", exc)
        return _error(str(exc), 413)
    except Exception as exc:
        LOG.exception("Upload save failed")
        return _error(f"Upload fehlgeschlagen: {exc}", 500)

    # 2b · Analyse parallel
    results: Dict[str, Tuple[int, int] | None] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(_run_pipeline, p): p for p in paths}
        for fut in as_completed(futs):
            name, bounds = fut.result()
            results[name] = bounds

    LOG.info("Batch finished:\n%s",
             json.dumps(results, indent=2, ensure_ascii=False))

    # 2c · Antwort
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify(results)
    return render_template("index_partial.html", results=results)


# ------------------------------------------------------------------------
#  GET /lookup?title=…&author=…&year=…   (Mini-JSON-API)
# ------------------------------------------------------------------------
@upload_bp.get("/lookup")
def lookup():
    title  = request.args.get("title", "").strip()
    author = request.args.get("author", "").strip()
    year   = request.args.get("year", type=int)

    if not title:
        return jsonify(error="title-Parameter fehlt"), 400

    payload = get_stats(title, author, year)
    return jsonify(payload or {"msg": "not found"})