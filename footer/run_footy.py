#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
run.py
======
Launcher für die EXTRAKTIONS-Suite:
- PDFs verarbeiten (Pipeline)
- Review/Editor öffnen
- (optional) Export: Fußnoten als TXT, Zitate als CSV
- Referenzen auflösen (Ebd./wie Anm./ders.)
- .env Reload (OPENAI-Key etc.)

Keine Dedupe-Funktionen – nur extrahieren, ansehen, bearbeiten.
Footer bleibt autark (azk_footer.sqlite). Keine Schreibzugriffe in db_core.
"""

from __future__ import annotations
import csv, os, sqlite3, subprocess, sys, time
from pathlib import Path
from typing import List, Optional

BASE_DIR = Path(__file__).resolve().parent
DB_PATH  = Path("../data/azk_footer.sqlite")
PIPELINE = BASE_DIR / "footnotes_pipeline.py"
REVIEW   = BASE_DIR / "footnotes_review_gui.py"
RESOLVER = BASE_DIR / "citations_resolver.py"

# --- .env früh laden (wenn vorhanden)
def _early_env_load() -> None:
    try:
        from azk_env import load_env_early  # optional
        load_env_early()
        print("[ENV] .env (falls vorhanden) wurde geladen.", flush=True)
    except Exception as e:
        print(f"[ENV][WARN] azk_env.load_env_early nicht verfügbar ({e})", flush=True)

_early_env_load()

try:
    import tkinter as tk
    from tkinter import ttk, messagebox, filedialog
    TK_OK = True
except Exception:
    TK_OK = False

def dprint(msg: str) -> None:
    print(msg, flush=True)

def open_path(path: Path) -> None:
    try:
        if sys.platform.startswith("darwin"):
            subprocess.Popen(["open", str(path)])
        elif os.name == "nt":
            os.startfile(str(path))  # type: ignore[attr-defined]
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception as e:
        dprint(f"[ERROR] Open path: {e}")

def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(DB_PATH))
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def export_footnotes_txt() -> int:
    if not DB_PATH.exists():
        messagebox.showerror("Fehler", f"DB nicht gefunden: {DB_PATH}")
        return 0
    with _connect() as con:
        cur = con.cursor()
        cur.execute("SELECT id, path, COALESCE(title,'') FROM documents ORDER BY id")
        docs = cur.fetchall()
        n = 0
        for doc_id, path, title in docs:
            path = Path(path)
            out = path.with_suffix(".footnotes.txt") if path.exists() else BASE_DIR / f"{(title or f'doc_{doc_id}').replace('/','_')}.footnotes.txt"
            cur.execute("""
                SELECT p.page_no, f.index_in_page, f.marker, f.text
                FROM footnotes f JOIN pages p ON p.id=f.page_id
                WHERE f.document_id=? ORDER BY p.page_no, f.index_in_page, f.id
            """, (doc_id,))
            rows = cur.fetchall()
            if not rows:
                continue
            with out.open("w", encoding="utf-8") as f:
                for pno, idx, m, t in rows:
                    head = f"[p.{pno} #{idx}]"
                    if m: head += f" [{m}]"
                    f.write(f"{head} {t}\n")
            dprint(f"[INFO] TXT: {out}")
            n += 1
        return n

def export_citations_csv() -> int:
    if not DB_PATH.exists():
        messagebox.showerror("Fehler", f"DB nicht gefunden: {DB_PATH}")
        return 0
    save = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV","*.csv")])
    if not save:
        return 0
    with _connect() as con, open(save, "w", encoding="utf-8", newline="") as f:
        cur = con.cursor()
        w = csv.writer(f)
        w.writerow(["document_path","page_no","footnote_index","citation_index",
                    "raw_text","type","authors_json","year","title","container_title",
                    "volume","issue","pages","publisher","place","doi","url","method"])
        cur.execute("""
            SELECT d.path, p.page_no, f.index_in_page, c.index_in_footnote,
                   c.raw_text, c.type, c.authors_json, c.year, c.title, c.container_title,
                   c.volume, c.issue, c.pages, c.publisher, c.place, c.doi, c.url, c.method
            FROM citations c
            JOIN footnotes f ON f.id = c.footnote_id
            JOIN pages p ON p.id = f.page_id
            JOIN documents d ON d.id = p.document_id
            ORDER BY d.id, p.page_no, f.index_in_page, c.index_in_footnote, c.id
        """)
        for r in cur.fetchall():
            w.writerow(r)
    dprint(f"[INFO] CSV exportiert: {save}")
    return 1

def _spawn_detached(cmd: List[str], cwd: Path) -> Optional[subprocess.Popen]:
    try:
        p = subprocess.Popen(cmd, cwd=str(cwd))
        dprint(f"[INFO] Prozess gestartet: {' '.join(cmd)} (pid={p.pid})")
        return p
    except Exception as e:
        dprint(f"[ERROR] Start fehlgeschlagen: {e}")
        return None

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("AZK Footnotes – Run")
        self.geometry("1024x680")
        self._build()

    def _build(self):
        top = ttk.Frame(self); top.pack(fill="x", padx=10, pady=8)
        ttk.Label(top, text="AZK Footnotes – Extraction Only", font=("Helvetica", 16, "bold")).pack(side="left")
        self.info_var = tk.StringVar()
        self._refresh_info()
        ttk.Label(top, textvariable=self.info_var).pack(side="right")

        btns = ttk.Frame(self); btns.pack(fill="x", padx=10, pady=8)
        ttk.Button(btns, text="PDFs verarbeiten (Pipeline)", command=self._run_pipeline).grid(row=0, column=0, padx=6, pady=6, sticky="ew")
        ttk.Button(btns, text="Review/Editor öffnen", command=self._run_review).grid(row=0, column=1, padx=6, pady=6, sticky="ew")
        ttk.Button(btns, text="Referenzen auflösen (Ebd./wie Anm./ders.)", command=self._run_resolver).grid(row=0, column=2, padx=6, pady=6, sticky="ew")

        ttk.Button(btns, text="Fußnoten → TXT (pro Dokument)", command=self._export_txt).grid(row=1, column=0, padx=6, pady=6, sticky="ew")
        ttk.Button(btns, text="Zitate → CSV (gesamt)", command=self._export_csv).grid(row=1, column=1, padx=6, pady=6, sticky="ew")
        ttk.Button(btns, text=".env neu laden (OPENAI)", command=self._reload_env).grid(row=1, column=2, padx=6, pady=6, sticky="ew")

        ttk.Button(btns, text="DB-Ordner öffnen", command=lambda: open_path(BASE_DIR)).grid(row=2, column=0, padx=6, pady=6, sticky="ew")
        ttk.Button(btns, text="DB-Datei öffnen", command=lambda: open_path(DB_PATH)).grid(row=2, column=1, padx=6, pady=6, sticky="ew")
        ttk.Button(btns, text="Beenden", command=self.destroy).grid(row=2, column=2, padx=6, pady=6, sticky="ew")

        logf = ttk.Frame(self); logf.pack(fill="both", expand=True, padx=10, pady=8)
        self.log = tk.Text(logf, wrap="word"); self.log.pack(side="left", fill="both", expand=True)
        sb = ttk.Scrollbar(logf, command=self.log.yview); sb.pack(side="right", fill="y")
        self.log.configure(yscrollcommand=sb.set)
        self._log("[INFO] Bereit.")

    def _refresh_info(self):
        info = []
        info.append(f"Python {sys.version.split()[0]}")
        info.append(f"DB: {DB_PATH.name}{' ✓' if DB_PATH.exists() else ' – wird erstellt'}")
        info.append(f"OPENAI_API_KEY: {'✓' if os.environ.get('OPENAI_API_KEY') else '–'}")
        info.append(f"DB_CORE_SQLITE: {'✓' if os.environ.get('DB_CORE_SQLITE') else '–'}")
        self.info_var.set("  |  ".join(info))

    def _log(self, line: str) -> None:
        ts = time.strftime("%H:%M:%S")
        self.log.insert("end", f"{ts} {line}\n"); self.log.see("end"); dprint(line)

    def _run_pipeline(self):
        if not PIPELINE.exists():
            messagebox.showerror("Fehler", f"Script fehlt: {PIPELINE}")
            return
        if not os.environ.get("DB_CORE_SQLITE"):
            self._log("[WARN] DB_CORE_SQLITE nicht gesetzt – Core-Verknüpfung wird übersprungen.")
        p = _spawn_detached([sys.executable, str(PIPELINE)], BASE_DIR)
        if p: self._log(f"[INFO] Pipeline gestartet: {PIPELINE.name}")

    def _run_review(self):
        if not REVIEW.exists():
            messagebox.showerror("Fehler", f"Script fehlt: {REVIEW}")
            return
        p = _spawn_detached([sys.executable, str(REVIEW)], BASE_DIR)
        if p: self._log("[INFO] Review-GUI gestartet.")

    def _run_resolver(self):
        if not RESOLVER.exists():
            messagebox.showerror("Fehler", f"Script fehlt: {RESOLVER}")
            return
        p = _spawn_detached([sys.executable, str(RESOLVER)], BASE_DIR)
        if p: self._log("[INFO] Resolver gestartet (alle Dokumente).")

    def _export_txt(self):
        n = export_footnotes_txt()
        self._log(f"[INFO] TXT-Export: {n} Dateien.")

    def _export_csv(self):
        n = export_citations_csv()
        self._log(f"[INFO] CSV-Export: {('OK' if n else 'abgebrochen')}")

    def _reload_env(self):
        self._log("[INFO] .env neu laden …")
        _early_env_load()
        self._refresh_info()
        self._log("[INFO] .env Reload fertig.")

def main():
    if not TK_OK:
        raise RuntimeError("Tkinter nicht verfügbar.")
    App().mainloop()

if __name__ == "__main__":
    main()