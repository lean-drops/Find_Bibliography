#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
footnotes_review_gui.py
-----------------------
Einfache Review-GUI (Tkinter) für 'azk_footer.sqlite':
- Baum: Dokument → Seite → Fußnoten → Zitate
- Rechtsklick: Fußnote/Zitat bearbeiten oder löschen
- KEINE DEDUPES, nur Editing/Viewing
"""

from __future__ import annotations
import sqlite3
from pathlib import Path
import tkinter as tk
from tkinter import ttk, simpledialog, messagebox

DB_FILENAME = "../data/azk_footer.sqlite"

def dprint(msg: str) -> None:
    print(msg, flush=True)

class ReviewApp(tk.Tk):
    def __init__(self, db_path: Path):
        super().__init__()
        self.title("AZK Footnotes – Review")
        self.geometry("1100x720")
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute("PRAGMA foreign_keys=ON;")

        self.tree = ttk.Treeview(self, columns=("info",), show="tree headings")
        self.tree.heading("info", text="Info")
        self.tree.pack(fill="both", expand=True)

        self.popup = tk.Menu(self, tearoff=0)
        self.popup.add_command(label="Text bearbeiten …", command=self._edit_selected)
        self.popup.add_command(label="Löschen", command=self._delete_selected)

        self.tree.bind("<Button-3>", self._on_right_click)

        self._load_data()

    def _on_right_click(self, event):
        iid = self.tree.identify_row(event.y)
        if iid and (iid.startswith("f") or iid.startswith("c")):
            self.tree.selection_set(iid)
            self.popup.tk_popup(event.x_root, event.y_root)

    def _load_data(self):
        self.tree.delete(*self.tree.get_children())
        cur = self.conn.cursor()
        for doc_id, path, title in cur.execute("SELECT id, path, COALESCE(title,'') FROM documents ORDER BY id DESC"):
            dnode = self.tree.insert("", "end", text=title or Path(path).name, values=(path,), open=False, iid=f"d{doc_id}")
            for page_id, page_no in cur.execute("SELECT id, page_no FROM pages WHERE document_id=? ORDER BY page_no", (doc_id,)):
                pnode = self.tree.insert(dnode, "end", text=f"Seite {page_no}", values=(f"page_id={page_id}",), open=False, iid=f"p{page_id}")
                for fn_id, idx, marker, text in cur.execute(
                        "SELECT id, COALESCE(index_in_page,0), marker, text FROM footnotes WHERE page_id=? ORDER BY index_in_page, id", (page_id,)):
                    label = f"{idx:03d} {('['+marker+'] ') if marker else ''}{(text[:120] + ('…' if len(text)>120 else ''))}"
                    fnode = self.tree.insert(pnode, "end", text=label, values=(f"fn_id={fn_id}",), iid=f"f{fn_id}")
                    for cid, cidx, raw in cur.execute(
                        "SELECT id, COALESCE(index_in_footnote,0), raw_text FROM citations WHERE footnote_id=? ORDER BY index_in_footnote, id",
                        (fn_id,)):
                        clabel = f"{cidx:02d} {raw[:140]}{'…' if len(raw)>140 else ''}"
                        self.tree.insert(fnode, "end", text=clabel, values=(f"cit_id={cid}",), iid=f"c{cid}")

    def _edit_selected(self):
        iid = self._sel()
        if not iid:
            return
        if iid.startswith("f"):
            fn_id = int(iid[1:])
            cur = self.conn.cursor()
            cur.execute("SELECT text FROM footnotes WHERE id=?", (fn_id,))
            row = cur.fetchone()
            if not row:
                return
            new = simpledialog.askstring("Fußnote bearbeiten", "Neuer Text:", initialvalue=row[0])
            if new is None:
                return
            self.conn.execute("UPDATE footnotes SET text=?, normalized_text=LOWER(TRIM(REPLACE(REPLACE(text, X'0A',' '), X'0D',' '))) WHERE id=?",
                              (new, fn_id))
            self.conn.commit()
            dprint(f"[INFO][GUI] Fußnote aktualisiert id={fn_id}")
            self._load_data()
        elif iid.startswith("c"):
            cid = int(iid[1:])
            cur = self.conn.cursor()
            cur.execute("SELECT raw_text FROM citations WHERE id=?", (cid,))
            row = cur.fetchone()
            if not row:
                return
            new = simpledialog.askstring("Zitat bearbeiten", "Neuer Rohtext:", initialvalue=row[0])
            if new is None:
                return
            self.conn.execute("UPDATE citations SET raw_text=?, normalized_text=LOWER(TRIM(REPLACE(REPLACE(raw_text, X'0A',' '), X'0D',' '))) WHERE id=?",
                              (new, cid))
            self.conn.commit()
            dprint(f"[INFO][GUI] Zitat aktualisiert id={cid}")
            self._load_data()

    def _delete_selected(self):
        iid = self._sel()
        if not iid:
            return
        if not messagebox.askyesno("Löschen", "Wirklich löschen?"):
            return
        if iid.startswith("f"):
            fn_id = int(iid[1:])
            self.conn.execute("DELETE FROM footnotes WHERE id=?", (fn_id,))
            self.conn.commit()
            dprint(f"[INFO][GUI] Fußnote gelöscht id={fn_id}")
        elif iid.startswith("c"):
            cid = int(iid[1:])
            self.conn.execute("DELETE FROM citations WHERE id=?", (cid,))
            self.conn.commit()
            dprint(f"[INFO][GUI] Zitat gelöscht id={cid}")
        self._load_data()

    def _sel(self) -> str | None:
        sel = self.tree.selection()
        return sel[0] if sel else None

def main():
    db_path = Path.cwd() / DB_FILENAME
    if not db_path.exists():
        messagebox.showerror("Fehler", f"DB nicht gefunden: {db_path}")
        return
    app = ReviewApp(db_path)
    app.mainloop()

if __name__ == "__main__":
    main()