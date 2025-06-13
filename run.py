"""
run.py – App-Factory & Launcher
-------------------------------
Bindet alle Blueprints ein
  • upload_routes.upload_bp    (/  &  /lookup)
  • grouping_routes.grouping_bp (/grouping/…)
"""
from __future__ import annotations
import logging, os, tempfile
from pathlib import Path

from flask import Flask
from upload_routes   import upload_bp
from grouping_routes import grouping_bp
from preview_routes import preview_bp
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)-8s | %(message)s")

# Gemeinsames Upload-Verzeichnis (tmp + global für bib_handler)
UPLOAD_DIR = Path(tempfile.gettempdir()) / "bib_finder_uploads"
UPLOAD_DIR.mkdir(exist_ok=True)
os.environ.setdefault("UPLOAD_DIR", str(UPLOAD_DIR))   # ← wichtiger Eintrag

# ---------------------------------------------------------------------------

def create_app() -> Flask:
    app = Flask(__name__,
                static_folder="static",
                template_folder="templates")

    # Maximal zulässige Upload-Größe (Env-Override möglich)
    app.config["MAX_CONTENT_LENGTH"] = int(
        os.getenv("MAX_UPLOAD_MB", "200")) * 1024 * 1024

    # Blueprints registrieren
    app.register_blueprint(upload_bp)
    app.register_blueprint(grouping_bp)
    app.register_blueprint(preview_bp)

    return app

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=9890, debug=True)