# FILE: db/overview/gui_dialogs.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
gui_dialogs.py
- NewWorkDialog (neu anlegen)
- EditWorkDialog (bearbeiten)
- NewAcqDialog  (Auftrag)
- WorkDetailDialog (DETAILS)  ← erweitert: selektierbare Felder + Copy-Buttons

Hinweise:
- Alle Textfelder in WorkDetailDialog sind selektierbar (read-only).
- Copy-Buttons: Autor / Titel / Jahr / Zitat (einzeilig) / Vollständiger Datensatz
- Registries (Bezugslinks) werden gelistet – mit 'Copy Link' pro Eintrag.
"""

from pathlib import Path
from typing import Dict, Any, Optional

# PySide6 bevorzugt, sonst PySide2
try:
    from PySide6.QtWidgets import (
        QDialog, QFormLayout, QLineEdit, QComboBox, QDialogButtonBox, QFileDialog,
        QSpinBox, QTextEdit, QWidget, QVBoxLayout, QLabel, QPushButton, QGridLayout,
        QHBoxLayout, QScrollArea, QFrame
    )
    from PySide6.QtCore import Qt
    from PySide6.QtGui import QGuiApplication
except Exception:  # pragma: no cover
    from PySide2.QtWidgets import (
        QDialog, QFormLayout, QLineEdit, QComboBox, QDialogButtonBox, QFileDialog,
        QSpinBox, QTextEdit, QWidget, QVBoxLayout, QLabel, QPushButton, QGridLayout,
        QHBoxLayout, QScrollArea, QFrame
    )
    from PySide2.QtCore import Qt
    from PySide2.QtGui import QGuiApplication

from db_core import normalize_year_key


def _dbg(msg: str) -> None:
    print(f"[DEBUG][gui_dialogs] {msg}")


# ---------------- NewWorkDialog ----------------
TYPES = ["monograph", "edited_volume", "chapter", "article", "brochure", "encyclopedia"]

class NewWorkDialog(QDialog):
    def __init__(self, parent=None, preset: Optional[Dict[str, Any]] = None, file_path: Optional[Path] = None):
        super().__init__(parent)
        self.setWindowTitle("Neues Werk anlegen")
        preset = preset or {}

        f = QFormLayout(self)
        self.e_author = QLineEdit(preset.get("author", ""))
        self.e_title  = QLineEdit(preset.get("title", ""))
        self.e_year   = QSpinBox(); self.e_year.setRange(1200, 2100); self.e_year.setValue(preset.get("year", 0) or 0)
        self.e_type   = QComboBox(); self.e_type.addItems(TYPES); self.e_type.setCurrentText(preset.get("type", "article"))

        self.e_container = QLineEdit()
        self.e_publisher = QLineEdit()
        self.e_place     = QLineEdit()
        self.e_series    = QLineEdit()
        self.e_notes     = QTextEdit()

        self.e_path = QLineEdit(str(file_path) if file_path else ""); self.e_path.setReadOnly(True)
        btn_browse  = QPushButton("Datei wählen …"); btn_browse.clicked.connect(self._pick_file)
        row = QWidget(); gl = QGridLayout(row); gl.setContentsMargins(0, 0, 0, 0); gl.addWidget(self.e_path, 0, 0); gl.addWidget(btn_browse, 0, 1)

        f.addRow("Autor", self.e_author)
        f.addRow("Titel", self.e_title)
        f.addRow("Jahr", self.e_year)
        f.addRow("Typ", self.e_type)
        f.addRow("Publiziert in (optional)", self.e_container)
        f.addRow("Verlag (optional)", self.e_publisher)
        f.addRow("Ort (optional)", self.e_place)
        f.addRow("Serie (optional)", self.e_series)
        f.addRow("Datei", row)
        f.addRow("Notiz", self.e_notes)

        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        bb.accepted.connect(self.accept); bb.rejected.connect(self.reject)
        f.addRow(bb)

    def _pick_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Datei wählen", "", "Dokumente (*.pdf *.epub *.djvu *.txt *.docx *.zip);;Alle Dateien (*.*)")
        if path:
            self.e_path.setText(path)

    def get_result(self):
        year = int(self.e_year.value()) if self.e_year.value() != 0 else None
        data = {
            "author": self.e_author.text().strip() or None,
            "title":  self.e_title.text().strip(),
            "year": year, "year_key": normalize_year_key(year),
            "type": self.e_type.currentText(),
            "container_title": self.e_container.text().strip() or None,
            "container_editors": None,
            "publisher": self.e_publisher.text().strip() or None,
            "place": self.e_place.text().strip() or None,
            "series": self.e_series.text().strip() or None,
            "volume": None, "issue": None, "pages": None,
            "isbn": None, "issn": None, "doi": None,
            "url": None, "oa": 0, "owned": 0,
            "notes": self.e_notes.toPlainText().strip() or None
        }
        fpath = Path(self.e_path.text()) if self.e_path.text() else None
        return data, fpath


# ---------------- EditWorkDialog ----------------
class EditWorkDialog(QDialog):
    """Ein Werk bearbeiten (inkl. OA/Besitz)."""
    def __init__(self, info: Dict[str, Any], parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Werk bearbeiten – ID {info.get('id')}")

        f = QFormLayout(self)
        self.e_author = QLineEdit(info.get("author", "") or "")
        self.e_title  = QLineEdit(info.get("title", "") or "")
        self.e_year   = QSpinBox(); self.e_year.setRange(1200, 2100); self.e_year.setValue(int(info.get("year") or 0))
        self.e_type   = QComboBox(); self.e_type.addItems(TYPES); self.e_type.setCurrentText(info.get("type", "article") or "article")
        self.e_container = QLineEdit(info.get("container_title", "") or "")
        self.e_publisher = QLineEdit(info.get("publisher", "") or "")
        self.e_place     = QLineEdit(info.get("place", "") or "")
        self.e_series    = QLineEdit(info.get("series", "") or "")
        self.e_volume    = QLineEdit(info.get("volume", "") or "")
        self.e_issue     = QLineEdit(info.get("issue", "") or "")
        self.e_pages     = QLineEdit(info.get("pages", "") or "")
        self.e_isbn      = QLineEdit(info.get("isbn", "") or "")
        self.e_issn      = QLineEdit(info.get("issn", "") or "")
        self.e_doi       = QLineEdit(info.get("doi", "") or "")
        self.e_url       = QLineEdit(info.get("url", "") or "")
        self.e_notes     = QTextEdit(); self.e_notes.setPlainText(info.get("notes", "") or "")

        try:
            from PySide6.QtWidgets import QCheckBox
        except Exception:
            from PySide2.QtWidgets import QCheckBox
        self.cb_oa    = QCheckBox(); self.cb_oa.setChecked(bool(info.get("oa")))
        self.cb_owned = QCheckBox(); self.cb_owned.setChecked(bool(info.get("owned")))

        f.addRow("Autor", self.e_author); f.addRow("Titel", self.e_title); f.addRow("Jahr", self.e_year)
        f.addRow("Typ", self.e_type); f.addRow("Publiziert in", self.e_container)
        f.addRow("Verlag", self.e_publisher); f.addRow("Ort", self.e_place); f.addRow("Serie", self.e_series)
        row3 = QWidget(); h3 = QHBoxLayout(row3); h3.setContentsMargins(0,0,0,0); h3.addWidget(self.e_volume); h3.addWidget(self.e_issue); h3.addWidget(self.e_pages)
        f.addRow("Band / Heft / Seiten", row3)
        rowid = QWidget(); hid = QHBoxLayout(rowid); hid.setContentsMargins(0,0,0,0); hid.addWidget(self.e_isbn); hid.addWidget(self.e_issn); hid.addWidget(self.e_doi)
        f.addRow("ISBN / ISSN / DOI", rowid)
        f.addRow("URL", self.e_url); f.addRow("OA", self.cb_oa); f.addRow("Besitz", self.cb_owned); f.addRow("Notiz", self.e_notes)

        bb = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        bb.accepted.connect(self.accept); bb.rejected.connect(self.reject)
        f.addRow(bb)

    def get_updates(self) -> Dict[str, Any]:
        year = int(self.e_year.value()) if self.e_year.value() != 0 else None
        return {
            "author": self.e_author.text().strip() or None,
            "title":  self.e_title.text().strip(),
            "year": year,
            "type": self.e_type.currentText(),
            "container_title": self.e_container.text().strip() or None,
            "publisher": self.e_publisher.text().strip() or None,
            "place": self.e_place.text().strip() or None,
            "series": self.e_series.text().strip() or None,
            "volume": self.e_volume.text().strip() or None,
            "issue": self.e_issue.text().strip() or None,
            "pages": self.e_pages.text().strip() or None,
            "isbn": self.e_isbn.text().strip() or None,
            "issn": self.e_issn.text().strip() or None,
            "doi":  self.e_doi.text().strip() or None,
            "url":  self.e_url.text().strip() or None,
            "oa":   1 if self.cb_oa.isChecked() else 0,
            "owned":1 if self.cb_owned.isChecked() else 0,
            "notes": self.e_notes.toPlainText().strip() or None
        }


# ---------------- NewAcqDialog ----------------
class NewAcqDialog(QDialog):
    def __init__(self, parent=None, default_work_id: Optional[int] = None):
        super().__init__(parent)
        self.setWindowTitle("Neuer Auftrag")
        try:
            from PySide6.QtWidgets import QCheckBox
        except Exception:
            from PySide2.QtWidgets import QCheckBox
        f = QFormLayout(self)
        self.e_work = QSpinBox(); self.e_work.setRange(1, 10**9); self.e_work.setValue(default_work_id or 1)
        self.e_action = QComboBox(); self.e_action.addItems(["download","scan_request","purchase","ill","investigate"])
        self.e_prio   = QSpinBox(); self.e_prio.setRange(1, 100); self.e_prio.setValue(50)
        self.e_url    = QLineEdit(); self.e_lib = QLineEdit("swisscovery")
        self.e_notes  = QTextEdit()
        f.addRow("Werk-ID", self.e_work); f.addRow("Aktion", self.e_action); f.addRow("Prio", self.e_prio)
        f.addRow("URL (optional)", self.e_url); f.addRow("Bibliothek", self.e_lib); f.addRow("Notiz", self.e_notes)
        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel); bb.accepted.connect(self.accept); bb.rejected.connect(self.reject); f.addRow(bb)

    def get_result(self):
        return {
            "work_id": int(self.e_work.value()),
            "action": self.e_action.currentText(),
            "priority": int(self.e_prio.value()),
            "proposed_url": (self.e_url.text().strip() or None),
            "library": (self.e_lib.text().strip() or None),
            "notes": (self.e_notes.toPlainText().strip() or None)
        }


# ---------------- WorkDetailDialog (kopierfreundlich) ----------------
class WorkDetailDialog(QDialog):
    """
    Details mit selektierbaren Feldern + Copy-Buttons:
      - Autor / Titel / Jahr einzeln kopieren
      - Zitat (einzeilig) kopieren
      - Vollständiger Datensatz (mehrzeilig) kopieren
      - Registries/Links auflisten + Copy-Link pro Eintrag
    """
    def __init__(self, info: Dict[str, Any], parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Werkdetails – ID {info.get('id')}")
        self.info = info

        root = QVBoxLayout(self)
        # Oberer Block: Felder
        form = QFormLayout()
        self.f_author = QLineEdit(info.get("author") or ""); self.f_author.setReadOnly(True)
        self.f_title  = QLineEdit(info.get("title") or "");  self.f_title.setReadOnly(True)
        self.f_year   = QLineEdit(str(info.get("year") or "")); self.f_year.setReadOnly(True)
        self.f_type   = QLineEdit(info.get("type") or "");   self.f_type.setReadOnly(True)
        self.f_cont   = QLineEdit(info.get("container_title") or ""); self.f_cont.setReadOnly(True)
        self.f_pages  = QLineEdit(info.get("pages") or "");  self.f_pages.setReadOnly(True)
        self.f_ids    = QLineEdit(" / ".join([x for x in [info.get("isbn"), info.get("issn"), info.get("doi")] if x])); self.f_ids.setReadOnly(True)
        self.f_url    = QLineEdit(info.get("url") or ""); self.f_url.setReadOnly(True)
        self.f_notes  = QTextEdit(); self.f_notes.setPlainText(info.get("notes") or ""); self.f_notes.setReadOnly(True)
        self.f_notes.setMinimumHeight(60)

        form.addRow("Autor", self.f_author)
        form.addRow("Titel", self.f_title)
        form.addRow("Jahr",  self.f_year)
        form.addRow("Typ",   self.f_type)
        form.addRow("Publiziert in", self.f_cont)
        form.addRow("Seiten", self.f_pages)
        form.addRow("ISBN / ISSN / DOI", self.f_ids)
        form.addRow("URL", self.f_url)
        form.addRow("Notiz", self.f_notes)

        root.addLayout(form)

        # Copy-Leiste
        copy_bar = QHBoxLayout()
        btn_copy_author = QPushButton("Copy Autor"); btn_copy_author.clicked.connect(lambda: self._copy(self.f_author.text(), "author"))
        btn_copy_title  = QPushButton("Copy Titel"); btn_copy_title.clicked.connect(lambda: self._copy(self.f_title.text(), "title"))
        btn_copy_year   = QPushButton("Copy Jahr");  btn_copy_year.clicked.connect(lambda: self._copy(self.f_year.text(), "year"))
        btn_copy_cite   = QPushButton("Copy Zitat (1 Zeile)"); btn_copy_cite.clicked.connect(self._copy_citation)
        btn_copy_full   = QPushButton("Copy Vollständig");     btn_copy_full.clicked.connect(self._copy_full)
        for b in (btn_copy_author, btn_copy_title, btn_copy_year, btn_copy_cite, btn_copy_full):
            copy_bar.addWidget(b)
        copy_bar.addStretch(1)
        root.addLayout(copy_bar)

        # Registries (Links) mit Copy-Link pro Zeile
        regs = info.get("registries", [])
        if regs:
            root.addWidget(self._make_separator("Registries / Bezugslinks"))
            regs_area = QVBoxLayout()
            for r in regs:
                line = QHBoxLayout()
                text = QLineEdit(f"{r.get('registry_name','')}  —  {r.get('permalink','')}")
                text.setReadOnly(True)
                btn = QPushButton("Copy Link")
                link = r.get("permalink") or ""
                btn.clicked.connect(lambda _, L=link: self._copy(L, "link"))
                line.addWidget(text); line.addWidget(btn)
                regs_area.addLayout(line)
            w = QWidget(); w.setLayout(regs_area)
            root.addWidget(w)
        else:
            root.addWidget(self._make_separator("Registries / Bezugslinks"))
            root.addWidget(QLabel("—"))

        # Aufträge (nur Anzeige – kopierbar über Markieren)
        acqs = info.get("acquisitions", [])
        root.addWidget(self._make_separator("Aufträge"))
        if acqs:
            for a in acqs:
                row = QLineEdit(f"{a.get('action')} [{a.get('status')}] · prio={a.get('priority')} · url={a.get('proposed_url') or '—'}")
                row.setReadOnly(True)
                root.addWidget(row)
        else:
            root.addWidget(QLabel("—"))

        # Close
        bb = QDialogButtonBox(QDialogButtonBox.Close)
        bb.accepted.connect(self.accept); bb.rejected.connect(self.reject)
        root.addWidget(bb)

    # ----- helpers -----
    def _make_separator(self, title: str) -> QWidget:
        w = QWidget(); l = QHBoxLayout(w); l.setContentsMargins(0, 10, 0, 5)
        lab = QLabel(f"<b>{title}</b>"); lab.setTextFormat(Qt.RichText)
        line = QFrame(); line.setFrameShape(QFrame.HLine); line.setFrameShadow(QFrame.Sunken)
        l.addWidget(lab); l.addWidget(line, 1); return w

    def _copy(self, text: str, what: str) -> None:
        QGuiApplication.clipboard().setText(text or "")
        _dbg(f"copy -> {what}: {repr((text or '')[:80])}…")

    def _make_citation_one_line(self) -> str:
        a = self.f_author.text().strip()
        t = self.f_title.text().strip()
        y = self.f_year.text().strip()
        c = self.f_cont.text().strip()
        p = self.f_publisher_text()  # ggf. leer
        place = self.f_place_text()
        parts = []
        if a: parts.append(a)
        if t: parts.append(t)
        if y: parts.append(f"({y})")
        # Container / Verlag / Ort
        tail = " · ".join([x for x in [c, p, place] if x])
        if tail: parts.append(tail)
        # Seiten
        pages = self.f_pages.text().strip()
        if pages: parts.append(pages)
        return " — ".join(parts)

    def _f(self, name: str) -> str:
        return getattr(self, name).text().strip()

    def _f_or_dash(self, name: str) -> str:
        txt = self._f(name); return txt if txt else "—"

    def _publisher_info(self) -> str:
        # Der Publisher/Ort steckt im Form oben einzeln; hier nur zusammensetzen
        return " · ".join([x for x in [self._f("f_publisher") if hasattr(self, "f_publisher") else "",
                                        self._f("f_place") if hasattr(self, "f_place") else ""] if x])

    # WorkDetailDialog hat oben nur read-only QLineEdits (publisher/place sind im one-line-citation gewünscht)
    # Für eine robuste Zusammensetzung greifen wir direkt auf self.info zurück:
    def f_publisher_text(self) -> str:
        return str(self.info.get("publisher") or "").strip()

    def f_place_text(self) -> str:
        return str(self.info.get("place") or "").strip()

    def _copy_citation(self):
        one = self._make_citation_one_line()
        self._copy(one, "citation-one-line")

    def _copy_full(self):
        i = self.info
        lines = [
            f"ID: {i.get('id')}",
            f"Autor: {i.get('author') or ''}",
            f"Titel: {i.get('title') or ''}",
            f"Jahr: {i.get('year') or ''}",
            f"Typ: {i.get('type') or ''}",
            f"Publiziert in: {i.get('container_title') or ''}",
            f"Verlag: {i.get('publisher') or ''}",
            f"Ort: {i.get('place') or ''}",
            f"Serie: {i.get('series') or ''}",
            f"Band/Heft/Seiten: {i.get('volume') or ''} / {i.get('issue') or ''} / {i.get('pages') or ''}",
            f"ISBN: {i.get('isbn') or ''}",
            f"ISSN: {i.get('issn') or ''}",
            f"DOI: {i.get('doi') or ''}",
            f"URL: {i.get('url') or ''}",
            f"OA: {i.get('oa') or 0}",
            f"Besitz: {i.get('owned') or 0}",
            f"Datei: {i.get('file_path') or '—'}",
        ]
        regs = i.get("registries", [])
        if regs:
            lines.append("Registries:")
            for r in regs:
                lines.append(f"  - {r.get('registry_name','')}: {r.get('permalink','')}")
        acqs = i.get("acquisitions", [])
        if acqs:
            lines.append("Aufträge:")
            for a in acqs:
                lines.append(f"  - {a.get('action')} [{a.get('status')}] prio={a.get('priority')} url={a.get('proposed_url') or '—'}")
        notes = (i.get("notes") or "").strip()
        if notes:
            lines.append("Notiz:")
            lines.append(notes)

        full_text = "\n".join(lines)
        self._copy(full_text, "full-record")