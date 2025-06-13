#!/usr/bin/env python3
"""
Batch run für detect_bibliography auf allen PDFs in einem Ordner
Aufruf:
    python batch_detect.py <PDF-DIR>
oder:
    python -m delb.batch_detect <PDF-DIR>
"""
import concurrent.futures as cf
import json
import sys
from pathlib import Path


def worker(pdf_path: Path):
    """Wird in jedem Sub-Prozess ausgeführt."""
    # Import erst hier: Pickling-Problem umgangen
    from services.delb import detect_bibliography

    return pdf_path.name, detect_bibliography(pdf_path)


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit("Pfad zum PDF-Ordner fehlt!")

    pdf_dir = Path(sys.argv[1]).expanduser()
    if not pdf_dir.is_dir():
        sys.exit(f"{pdf_dir} ist kein Verzeichnis")

    pdfs = sorted(pdf_dir.glob("*.pdf"))
    if not pdfs:
        sys.exit("Keine PDFs gefunden")

    results = {}
    with cf.ProcessPoolExecutor() as pool:
        for name, bounds in pool.map(worker, pdfs):
            results[name] = bounds
            print(f"{name:45} → {bounds}")

    out = pdf_dir / "bibliography_bounds.json"
    out.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"\n✓ Fertig – Ergebnisse in {out}")

if __name__ == "__main__":
    main()