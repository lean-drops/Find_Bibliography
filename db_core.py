
# FILE: db/db_core.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
db_core.py
- DB & Ordner-Management
- Duplikat-sicheres Schema (UNIQUE über author_key, title_key, year_key)
- Dedupe-Migration
- Library-Sync
- add_work_with_file(...), attach_file_to_work(...)
- NEU: get_work(...), update_work(...), refresh_file_stub_and_rename(...), delete_work(...)
"""

from __future__ import annotations
from pathlib import Path
import os, sqlite3, time, json, shutil, unicodedata, re
from typing import Optional, Dict, Any, List, Sequence

DB_PATH = Path("data/db/azk_biblio.sqlite")
LIB_DIR = Path("data/azk_library")
DATA_DIR = Path("data")
JSON_WORKS = Path("db/data/azk_biblio.json")
JSON_AVAIL = Path("db/data/azk_biblio.json")

ALLOWED_EXTS: Sequence[str] = (".pdf", ".epub", ".djvu", ".txt", ".docx", ".zip")

def debug(msg: str) -> None: print(f"[DEBUG] {msg}")

def ensure_dirs() -> None:
    (DATA_DIR / "db").mkdir(parents=True, exist_ok=True)
    LIB_DIR.mkdir(parents=True, exist_ok=True)

def check_writeable(p: Path) -> None:
    parent = p.parent; parent.mkdir(parents=True, exist_ok=True)
    if not os.access(parent, os.W_OK | os.X_OK): raise PermissionError(f"Kein Schreibrecht in: {parent}")
    test = parent / f".write_test_{int(time.time())}.tmp"
    with test.open("wb") as fh:
        fh.write(b"ok"); fh.flush(); os.fsync(fh.fileno())
    try: test.unlink()
    except Exception: pass
    debug(f"Schreibtest OK: {parent}")

def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    debug(f"Verbinde zu SQLite-DB: {db_path}")
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    with conn:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
    return conn

def checkpoint_and_vacuum(conn: sqlite3.Connection) -> None:
    try:
        debug("Checkpoint WAL (TRUNCATE) …"); conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        debug("VACUUM …");                  conn.execute("VACUUM;")
    except sqlite3.OperationalError as e:
        debug(f"VACUUM/Checkpoint nicht möglich: {e}")

WS = re.compile(r"\s+")
QUOTES = str.maketrans({"«": '"', "»": '"', "‚": "'", "’": "'", "“": '"', "”": '"'})

def normalize_text(s: Optional[str]) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", s).translate(QUOTES)
    s = "".join(ch for ch in s if not unicodedata.combining(ch)).lower()
    return WS.sub(" ", s).strip()

def normalize_year_key(y: Optional[int]) -> int:
    return int(y) if isinstance(y, int) else -1

def slugify(s: Optional[str], max_len: int = 64) -> str:
    if not s: return "anon"
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch)).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return (s or "anon")[:max_len]

# --- Basis-DDL + Migration ---
DDL_BASE = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS works (
  id INTEGER PRIMARY KEY,
  author TEXT, title TEXT NOT NULL, year INTEGER, year_key INTEGER NOT NULL DEFAULT -1,
  type TEXT, container_title TEXT, container_editors TEXT, publisher TEXT, place TEXT,
  series TEXT, volume TEXT, issue TEXT, pages TEXT,
  isbn TEXT, issn TEXT, doi TEXT, url TEXT,
  oa INTEGER DEFAULT 0, owned INTEGER DEFAULT 0, notes TEXT,
  author_key TEXT, title_key  TEXT,
  file_stub TEXT, file_ext TEXT, file_exists INTEGER DEFAULT 0,
  file_size INTEGER, file_mtime REAL, file_path TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS registries (
  id INTEGER PRIMARY KEY,
  registry_name TEXT NOT NULL, external_id TEXT, permalink TEXT,
  work_id INTEGER, notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (registry_name, external_id, work_id),
  FOREIGN KEY(work_id) REFERENCES works(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS acquisitions (
  id INTEGER PRIMARY KEY,
  work_id INTEGER,
  action TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'todo', priority INTEGER DEFAULT 50,
  proposed_url TEXT, proposed_url_key TEXT,
  library TEXT, due_date TEXT, assigned_to TEXT,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(work_id) REFERENCES works(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_works_owned ON works(owned);
CREATE INDEX IF NOT EXISTS idx_works_file  ON works(file_exists);
CREATE INDEX IF NOT EXISTS idx_acq_status  ON acquisitions(status, priority);
"""

def _colnames(conn: sqlite3.Connection, table: str) -> List[str]:
    return [r["name"] for r in conn.execute(f"PRAGMA table_info('{table}');").fetchall()]

def ensure_schema(conn: sqlite3.Connection) -> None:
    debug("Lege/aktualisiere Schema …")
    with conn: conn.executescript(DDL_BASE)

    # Migration: fehlende Spalten
    wcols = _colnames(conn, "works")
    with conn:
        if "author_key" not in wcols:
            conn.execute("ALTER TABLE works ADD COLUMN author_key TEXT NOT NULL DEFAULT ''")
        if "title_key" not in wcols:
            conn.execute("ALTER TABLE works ADD COLUMN title_key  TEXT NOT NULL DEFAULT ''")
    acols = _colnames(conn, "acquisitions")
    with conn:
        if "proposed_url_key" not in acols:
            conn.execute("ALTER TABLE acquisitions ADD COLUMN proposed_url_key TEXT NOT NULL DEFAULT ''")

    _backfill_keys(conn)

    # UNIQUE-Index
    with conn:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_works_uniq
            ON works(author_key, title_key, year_key);
        """)

    dedupe_works(conn)
    with conn:
        conn.execute("UPDATE acquisitions SET proposed_url_key=COALESCE(proposed_url,'') WHERE proposed_url_key IS NULL OR proposed_url_key='';")
    debug("Schema OK.")

def _backfill_keys(conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT id, author, title, year FROM works WHERE author_key IS NULL OR author_key='' OR title_key IS NULL OR title_key='';").fetchall()
    if not rows: return
    debug(f"Backfill author_key/title_key für {len(rows)} Einträge …")
    with conn:
        for r in rows:
            ak = normalize_text(r["author"]); tk = normalize_text(r["title"]); yk = normalize_year_key(r["year"])
            conn.execute("UPDATE works SET author_key=?, title_key=?, year_key=?, updated_at=CURRENT_TIMESTAMP WHERE id=?;", (ak, tk, yk, r["id"]))

def dedupe_works(conn: sqlite3.Connection) -> None:
    dups = conn.execute("""
        SELECT author_key, title_key, year_key, COUNT(*) AS c
        FROM works
        GROUP BY author_key, title_key, year_key
        HAVING c > 1;
    """).fetchall()
    if not dups: return
    debug(f"Dedupe: {len(dups)} Schlüsselgruppen mit Duplikaten …")
    with conn:
        for g in dups:
            rows = conn.execute("""
              SELECT * FROM works WHERE author_key=? AND title_key=? AND year_key=? ORDER BY id ASC;
            """, (g["author_key"], g["title_key"], g["year_key"])).fetchall()
            keeper = rows[0]
            for r in rows[1:]:
                conn.execute("UPDATE acquisitions SET work_id=? WHERE work_id=?;", (keeper["id"], r["id"]))
                conn.execute("UPDATE registries  SET work_id=? WHERE work_id=?;", (keeper["id"], r["id"]))
                if not keeper["file_exists"] and r["file_exists"]:
                    conn.execute("""
                        UPDATE works SET file_exists=?, file_ext=?, file_size=?, file_mtime=?, file_path=?, owned=1, updated_at=CURRENT_TIMESTAMP
                        WHERE id=?;
                    """, (r["file_exists"], r["file_ext"], r["file_size"], r["file_mtime"], r["file_path"], keeper["id"]))
                conn.execute("DELETE FROM works WHERE id=?;", (r["id"],))
    debug("Dedupe abgeschlossen.")

UPSERT_WORK = """
INSERT INTO works(author, title, year, year_key, type, container_title, container_editors, publisher, place, series,
                  volume, issue, pages, isbn, issn, doi, url, oa, owned, notes, author_key, title_key)
VALUES(:author, :title, :year, :year_key, :type, :container_title, :container_editors, :publisher, :place, :series,
       :volume, :issue, :pages, :isbn, :issn, :doi, :url, :oa, :owned, :notes, :author_key, :title_key)
ON CONFLICT(author_key, title_key, year_key) DO UPDATE SET
 type=excluded.type, container_title=excluded.container_title, container_editors=excluded.container_editors,
 publisher=excluded.publisher, place=excluded.place, series=excluded.series, volume=excluded.volume,
 issue=excluded.issue, pages=excluded.pages, isbn=excluded.isbn, issn=excluded.issn, doi=excluded.doi,
 url=excluded.url, oa=excluded.oa, owned=excluded.owned, notes=excluded.notes,
 author=COALESCE(excluded.author, author), title=COALESCE(excluded.title, title),
 updated_at=CURRENT_TIMESTAMP;
"""

def upsert_works(conn: sqlite3.Connection, records: List[Dict[str, Any]]) -> None:
    debug(f"UPSERT {len(records)} Werke …")
    normed = []
    for rec in records:
        r = dict(rec)
        r["author_key"] = normalize_text(r.get("author"))
        r["title_key"]  = normalize_text(r.get("title"))
        r["year_key"]   = normalize_year_key(r.get("year"))
        normed.append(r)
    with conn: conn.executemany(UPSERT_WORK, normed)
    _backfill_keys(conn); dedupe_works(conn)
    n = conn.execute("SELECT COUNT(*) AS n FROM works;").fetchone()["n"]
    debug(f"Anzahl Werke: {n}")

def assign_file_stubs(conn: sqlite3.Connection) -> None:
    debug("Aktualisiere file_stubs …")
    rows = conn.execute("SELECT id, author, file_stub FROM works;").fetchall()
    ups = []
    for r in rows:
        stub = f"{slugify(r['author'])}__{r['id']}"
        if r["file_stub"] != stub:
            ups.append((stub, r["id"]))
    if ups:
        with conn: conn.executemany("UPDATE works SET file_stub=?, updated_at=CURRENT_TIMESTAMP WHERE id=?;", ups)
    debug(f"file_stubs: {len(ups)} geändert, {len(rows)-len(ups)} unverändert.")

def _find_existing_file(lib_dir: Path, stub: str) -> Optional[Path]:
    for ext in ALLOWED_EXTS:
        p = lib_dir / f"{stub}{ext}"
        if p.exists() and p.is_file(): return p
    return None

def sync_library(conn: sqlite3.Connection, lib_dir: Path = LIB_DIR, update_owned_from_files: bool = True) -> None:
    debug(f"Library-Sync {lib_dir} …")
    rows = conn.execute("SELECT id, file_stub FROM works ORDER BY id;").fetchall()
    found = 0
    with conn:
        for r in rows:
            stub = r["file_stub"]
            if not stub:
                a = conn.execute("SELECT author FROM works WHERE id=?;", (r["id"],)).fetchone()["author"]
                stub = f"{slugify(a)}__{r['id']}"
                conn.execute("UPDATE works SET file_stub=? WHERE id=?;", (stub, r["id"]))
            fp = _find_existing_file(lib_dir, stub)
            if fp:
                st = fp.stat()
                conn.execute("""
                    UPDATE works SET file_exists=1, file_ext=?, file_size=?, file_mtime=?, file_path=?, updated_at=CURRENT_TIMESTAMP
                    WHERE id=?;""", (fp.suffix.lower(), st.st_size, st.st_mtime, fp.name, r["id"]))
                if update_owned_from_files:
                    conn.execute("UPDATE works SET owned=1 WHERE id=?;", (r["id"],))
                found += 1
            else:
                conn.execute("""
                    UPDATE works SET file_exists=0, file_ext=NULL, file_size=NULL, file_mtime=NULL, file_path=NULL, updated_at=CURRENT_TIMESTAMP
                    WHERE id=?;""", (r["id"],))
                if update_owned_from_files:
                    conn.execute("UPDATE works SET owned=0 WHERE id=?;", (r["id"],))
    debug(f"Library-Sync: {found} Datei(en) gefunden, {len(rows)-found} fehlen.")

def attach_file_to_work(conn: sqlite3.Connection, work_id: int, file_path: Path, overwrite: bool = False, lib_dir: Path = LIB_DIR) -> str:
    row = conn.execute("SELECT file_stub FROM works WHERE id=?;", (work_id,)).fetchone()
    if not row: raise ValueError(f"work_id {work_id} nicht gefunden")
    stub = row["file_stub"]
    if not stub:
        assign_file_stubs(conn)
        stub = conn.execute("SELECT file_stub FROM works WHERE id=?;", (work_id,)).fetchone()["file_stub"]
    ext = (file_path.suffix.lower() or ".pdf")
    target = lib_dir / f"{stub}{ext}"
    if target.exists() and not overwrite:
        st = target.stat()
        with conn:
            conn.execute("""
                UPDATE works SET file_exists=1, file_ext=?, file_size=?, file_mtime=?, file_path=?, owned=1, updated_at=CURRENT_TIMESTAMP
                WHERE id=?;""", (ext, st.st_size, st.st_mtime, target.name, work_id))
        return str(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(file_path, target)
    st = target.stat()
    with conn:
        conn.execute("""
            UPDATE works SET file_exists=1, file_ext=?, file_size=?, file_mtime=?, file_path=?, owned=1, updated_at=CURRENT_TIMESTAMP
            WHERE id=?;""", (ext, st.st_size, st.st_mtime, target.name, work_id))
    return str(target)

def add_work_with_file(conn: sqlite3.Connection, data: Dict[str, Any], file_path: Optional[Path], lib_dir: Path = LIB_DIR) -> int:
    d = dict(data)
    d["author_key"] = normalize_text(d.get("author"))
    d["title_key"]  = normalize_text(d.get("title"))
    d["year_key"]   = normalize_year_key(d.get("year"))
    with conn:
        conn.execute(UPSERT_WORK, d)
        wid = conn.execute("""
            SELECT id FROM works WHERE author_key=? AND title_key=? AND year_key=? LIMIT 1;
        """, (d["author_key"], d["title_key"], d["year_key"])).fetchone()["id"]
    assign_file_stubs(conn)
    if file_path:
        attach_file_to_work(conn, wid, file_path, overwrite=False, lib_dir=lib_dir)
    conn.commit()
    return wid

def list_works(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute("""
        SELECT w.*,
               (SELECT COUNT(*) FROM acquisitions a WHERE a.work_id=w.id AND a.status!='done') AS open_acq
        FROM works w ORDER BY w.author, w.title;""").fetchall()
    return [dict(r) for r in rows]

def add_acquisition(conn: sqlite3.Connection, work_id: int, action: str, priority: int = 50,
                    proposed_url: Optional[str] = None, library: Optional[str] = None, notes: Optional[str] = None) -> None:
    key = proposed_url or ""
    with conn:
        conn.execute("""
            INSERT INTO acquisitions(work_id, action, status, priority, proposed_url, proposed_url_key, library, notes)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(work_id, action, proposed_url_key) DO UPDATE SET
              status=excluded.status, priority=excluded.priority, notes=excluded.notes, updated_at=CURRENT_TIMESTAMP;
        """, (work_id, action, "todo", priority, proposed_url, key, library, notes))
    conn.commit()

def list_acquisitions(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute("""
        SELECT a.*, w.author, w.title
        FROM acquisitions a
        LEFT JOIN works w ON w.id=a.work_id
        ORDER BY a.status, a.priority DESC, a.id DESC;""").fetchall()
    return [dict(r) for r in rows]

# =========================
# NEU: CRUD-Helpers
# =========================
def get_work(conn: sqlite3.Connection, work_id: int) -> Dict[str, Any]:
    row = conn.execute("SELECT * FROM works WHERE id=?;", (work_id,)).fetchone()
    if not row:
        raise ValueError(f"work_id {work_id} nicht gefunden")
    return dict(row)

def refresh_file_stub_and_rename(conn: sqlite3.Connection, work_id: int) -> Optional[str]:
    """Vergibt neuen file_stub (aus author) und benennt vorhandene Datei um."""
    row = conn.execute("SELECT id, author, file_stub, file_path FROM works WHERE id=?;", (work_id,)).fetchone()
    if not row:
        raise ValueError(f"work_id {work_id} nicht gefunden")
    new_stub = f"{slugify(row['author'])}__{row['id']}"
    old_stub = row["file_stub"]
    if new_stub == old_stub:
        return None
    # Datei umbenennen, wenn vorhanden
    if row["file_path"]:
        old_path = LIB_DIR / row["file_path"]
        ext = old_path.suffix if old_path.suffix else ".pdf"
        new_path = LIB_DIR / f"{new_stub}{ext}"
        if old_path.exists():
            old_path.replace(new_path)
            with conn:
                conn.execute("UPDATE works SET file_path=?, file_stub=?, updated_at=CURRENT_TIMESTAMP WHERE id=?;",
                             (new_path.name, new_stub, work_id))
            return str(new_path)
    with conn:
        conn.execute("UPDATE works SET file_stub=?, updated_at=CURRENT_TIMESTAMP WHERE id=?;", (new_stub, work_id))
    return None

def update_work(conn: sqlite3.Connection, work_id: int, changes: Dict[str, Any], rename_file: bool = True) -> None:
    """Aktualisiert Felder des Werks. Achtung: UNIQUE (author_key,title_key,year_key) wird geprüft."""
    current = get_work(conn, work_id)
    new = dict(current)
    new.update(changes or {})
    # Keys neu berechnen
    new["author_key"] = normalize_text(new.get("author"))
    new["title_key"]  = normalize_text(new.get("title"))
    new["year_key"]   = normalize_year_key(new.get("year"))
    # Update
    sets = ", ".join([f"{k}=:{k}" for k in ("author","title","year","year_key","type","container_title","container_editors",
                                           "publisher","place","series","volume","issue","pages","isbn","issn","doi",
                                           "url","oa","owned","notes","author_key","title_key")])
    with conn:
        conn.execute(f"UPDATE works SET {sets}, updated_at=CURRENT_TIMESTAMP WHERE id=:id;",
                     {**{k:new.get(k) for k in ("author","title","year","year_key","type","container_title","container_editors",
                                                "publisher","place","series","volume","issue","pages","isbn","issn","doi",
                                                "url","oa","owned","notes","author_key","title_key")},
                      "id": work_id})
    # ggf. Datei / stub umbenennen
    if rename_file and (new.get("author") != current.get("author")):
        refresh_file_stub_and_rename(conn, work_id)

def delete_work(conn: sqlite3.Connection, work_id: int, delete_file: bool = False) -> None:
    """Löscht Werk; je nach Flag auch Datei im Library-Ordner. Registries/Acqs werden FK-kaskadiert gelöscht."""
    row = conn.execute("SELECT file_path FROM works WHERE id=?;", (work_id,)).fetchone()
    if not row:
        return
    fpath = row["file_path"]
    if delete_file and fpath:
        try:
            (LIB_DIR / fpath).unlink(missing_ok=True)
        except Exception as e:
            debug(f"Datei konnte nicht gelöscht werden: {e}")
    with conn:
        conn.execute("DELETE FROM works WHERE id=?;", (work_id,))
    debug(f"Werk {work_id} gelöscht (Datei={'ja' if delete_file else 'nein'})")

# ---------- Bootstrap ----------
def summarize(conn: sqlite3.Connection) -> None:
    stats = conn.execute("""
        SELECT
          (SELECT COUNT(*) FROM works) AS n_works,
          (SELECT COUNT(*) FROM works WHERE file_exists=1) AS n_have,
          (SELECT COUNT(*) FROM works WHERE file_exists=0) AS n_missing
    """).fetchone()
    debug(f"Werke: {stats['n_works']}  vorhanden: {stats['n_have']}  fehlend: {stats['n_missing']}")


def seed_availability_from_json(conn, JSON_AVAIL):
    pass


def bootstrap() -> None:
    print("=== Bootstrap DB ===")
    ensure_dirs(); check_writeable(DB_PATH)
    conn = connect(DB_PATH)
    try:
        ensure_schema(conn)
        if JSON_WORKS.exists():
            raw = json.loads(JSON_WORKS.read_text(encoding="utf-8")); upsert_works(conn, raw)
        assign_file_stubs(conn); sync_library(conn, LIB_DIR, True)
        if JSON_AVAIL.exists(): seed_availability_from_json(conn, JSON_AVAIL)
        summarize(conn); checkpoint_and_vacuum(conn)
    finally:
        conn.close()
    print("=== Bootstrap OK ===")