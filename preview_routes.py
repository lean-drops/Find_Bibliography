#!/usr/bin/env python3
# services/preview_routes.py  ·  Rev. 2025-05-01
# Zeigt eine PDF-Seite als PNG-Thumbnail (LRU-gecached im RAM)
# URL-Schema:  /preview/<filename>?page=1&dpi=120
# ─────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import io, logging, os
from functools    import lru_cache
from pathlib      import Path
from typing       import Tuple

import fitz                               # PyMuPDF
from flask       import Blueprint, abort, request, send_file
from dotenv      import load_dotenv

# ───────────── Settings / ENV ───────────────────────────────────────────────
load_dotenv()
LOG        = logging.getLogger("preview")
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "/tmp")).resolve()
DPI_DEF    = int(os.getenv("PREVIEW_DPI",   "120"))
CACHE_MAX  = int(os.getenv("PREVIEW_CACHE", "256"))

preview_bp = Blueprint("preview", __name__)

# ───────────── LRU-Cache-Renderer (PNG) ─────────────────────────────────────
@lru_cache(maxsize=CACHE_MAX)
def _render_png(pdf_path: Path, page_no: int, dpi: int) -> bytes:
    """Render eine Seite (0-basiert) als PNG, Ergebnis als Bytes."""
    try:
        with fitz.open(pdf_path) as doc:
            if not (0 <= page_no < doc.page_count):
                raise IndexError("page out of range")
            pix = doc.load_page(page_no).get_pixmap(dpi=dpi, alpha=False)
            return pix.tobytes("png")
    except Exception as exc:
        LOG.warning("Preview render failed for %s p%d – %s", pdf_path.name, page_no, exc)
        raise

# ───────────── Route ───────────────────────────────────────────────────────
@preview_bp.route("/preview/<path:filename>")
def preview(filename: str):
    """
    * filename … nur der Name der hochgeladenen Datei (kein absoluter Pfad)
    * GET-Parameter:
        page (1-basiert, default=1)
        dpi  (optional, default PREVIEW_DPI)
    """
    pdf_path = (UPLOAD_DIR / Path(filename).name).resolve()
    if not pdf_path.exists():
        abort(404, "file not found")

    page = request.args.get("page", default="1")
    dpi  = request.args.get("dpi",  default=str(DPI_DEF))

    try:
        page_i = max(0, int(page) - 1)          # 0-basiert intern
        dpi_i  = max(50, min(300, int(dpi)))    # sinnvolle Grenzen
    except ValueError:
        abort(400, "invalid page/dpi param")

    try:
        data = _render_png(pdf_path, page_i, dpi_i)
        return send_file(
            io.BytesIO(data),
            mimetype="image/png",
            download_name=f"{pdf_path.stem}_p{page_i+1}.png",
        )
    except Exception:
        abort(500, "rendering failed")