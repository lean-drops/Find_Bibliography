"""
GUI (PySide6) – Konzept-Finder mit Auto-Session, Cache und DETAIL-Scan für HTML-Report.

Nutzung
-------
python organizer/keyword/gui.py

Pipeline
--------
Seed -> OpenAI-Kandidaten (Cache) -> Deduplizieren/Varianten -> DETAIL-Scan (Term×Datei, Autor, Ko-Auftreten)
-> JSON + HTML-Report. Nach Export wird der Session-Name automatisch erhöht.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=False)

from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex
from PySide6.QtGui import QPalette, QColor
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QSplitter, QFileDialog, QMessageBox,
    QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton, QComboBox,
    QTableView, QHeaderView, QPlainTextEdit, QCheckBox, QSpinBox, QFormLayout
)

import search_term
import keyword_utils
import html_report

DEFAULT_LIBRARY = "/Users/programming/PycharmProjects/Find_Bibliography_NEw/data/azk_library"
DEFAULT_OUTPUT_BASE = "/Users/programming/PycharmProjects/Find_Bibliography_NEw/data/keyword_data"


@dataclass
class CandidateRow:
    term: str
    category: str
    selected: bool = True


class CandidatesModel(QAbstractTableModel):
    def __init__(self, rows: List[CandidateRow]):
        super().__init__()
        self._rows = rows
        self._headers = ["✓", "Begriff", "Kategorie"]

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 3

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None
        row = self._rows[index.row()]
        col = index.column()
        if role == Qt.DisplayRole:
            return "✔" if col == 0 and row.selected else (row.term if col == 1 else (row.category if col == 2 else None))
        if role == Qt.TextAlignmentRole:
            return Qt.AlignCenter if col == 0 else Qt.AlignLeft
        if role == Qt.CheckStateRole and col == 0:
            return Qt.Checked if row.selected else Qt.Unchecked
        return None

    def flags(self, index: QModelIndex):
        if not index.isValid():
            return Qt.ItemIsEnabled
        if index.column() == 0:
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def setData(self, index: QModelIndex, value, role: int = Qt.EditRole):
        if not index.isValid():
            return False
        if index.column() == 0 and role == Qt.CheckStateRole:
            self._rows[index.row()].selected = (value == Qt.Checked)
            self.dataChanged.emit(index, index)
            return True
        return False

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self._headers[section]
        return super().headerData(section, orientation, role)

    def rows(self) -> List[CandidateRow]:
        return self._rows

    def set_rows(self, rows: List[CandidateRow]) -> None:
        self.beginResetModel()
        self._rows = rows
        self.endResetModel()

    def force_select_all(self) -> None:
        for r in self._rows:
            r.selected = True
        self.layoutChanged.emit()


def apply_dark_theme(app: QApplication) -> None:
    app.setStyle("Fusion")
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(37, 37, 38))
    palette.setColor(QPalette.WindowText, Qt.white)
    palette.setColor(QPalette.Base, QColor(30, 30, 30))
    palette.setColor(QPalette.AlternateBase, QColor(45, 45, 48))
    palette.setColor(QPalette.Text, Qt.white)
    palette.setColor(QPalette.Button, QColor(45, 45, 48))
    palette.setColor(QPalette.ButtonText, Qt.white)
    palette.setColor(QPalette.Highlight, QColor(14, 99, 156))
    palette.setColor(QPalette.HighlightedText, Qt.white)
    app.setPalette(palette)
    app.setStyleSheet("""
        QMainWindow, QWidget { font-family: 'Segoe UI', Arial; font-size: 12pt; }
        QLineEdit, QPlainTextEdit, QComboBox, QSpinBox { padding: 6px; border: 1px solid #3a3a3a; border-radius: 6px; }
        QPushButton { padding: 8px 12px; border: 1px solid #3a3a3a; border-radius: 6px; background: #2f2f30; }
        QPushButton:hover { background: #3a3a3b; }
        QPushButton:pressed { background: #1f1f20; }
        QHeaderView::section { background: #2f2f30; padding: 6px; border: none; }
        QTableView { gridline-color: #3a3a3a; selection-background-color: #0e639c; selection-color: #ffffff; }
        QLabel[role="title"] { font-size: 14pt; font-weight: 600; }
    """)


def _propose_initial_session(base_dir: Path) -> str:
    base = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    idx = 0
    while True:
        name = f"{base}_{idx:02d}" if idx > 0 else base
        if not (base_dir / name).exists():
            return name
        idx += 1


def _next_session_name(current: str, base_dir: Path) -> str:
    m = re.match(r"^(.*?)(?:_(\d{2,}))?$", current)
    if not m:
        base = current
        start = 1
    else:
        base = m.group(1)
        start = int(m.group(2) or "0") + 1
    idx = start
    while True:
        name = f"{base}_{idx:02d}"
        if not (base_dir / name).exists():
            return name
        idx += 1


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Konzept-Finder – AZK Sekundärliteratur")
        self.resize(1280, 820)

        self.output_base = Path(DEFAULT_OUTPUT_BASE)
        self.library_dir = Path(DEFAULT_LIBRARY)

        self.generated_raw: Dict[str, Any] = {}
        self.expanded_terms: Dict[str, List[str]] = {}
        self.scan_result: Optional[Dict[str, Any]] = None

        root = QSplitter(Qt.Horizontal)
        self.setCentralWidget(root)

        # Left
        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(10, 10, 10, 10)

        title = QLabel("Seed & Session")
        title.setProperty("role", "title")
        left_layout.addWidget(title)

        form = QFormLayout()
        self.seed_edit = QLineEdit()
        self.seed_edit.setPlaceholderText("z. B. Frieden, Abschied, Waffenstillstand …")
        self.domain_edit = QLineEdit("Alte Zürichkrieg-Forschung, spätmittelalterliche Chronistik, DE/CH/AT")
        init_session = _propose_initial_session(self.output_base)
        self.session_edit = QLineEdit(init_session)
        self.lang_combo = QComboBox()
        self.lang_combo.addItems(["de", "en"])
        self.model_edit = QLineEdit(os.getenv("OPENAI_MODEL", "gpt-4.1-mini"))
        self.kwic_spin = QSpinBox()
        self.kwic_spin.setRange(20, 200)
        self.kwic_spin.setValue(80)

        form.addRow("Seed-Begriff", self.seed_edit)
        form.addRow("Domain-Hinweis", self.domain_edit)
        form.addRow("Session-Name", self.session_edit)
        form.addRow("Sprache", self.lang_combo)
        form.addRow("OpenAI-Model", self.model_edit)
        form.addRow("KWIC-Zeichen", self.kwic_spin)
        left_layout.addLayout(form)

        key_status = "geladen" if os.getenv("OPENAI_API_KEY") else "fehlt"
        left_layout.addWidget(QLabel(f"OpenAI-Key (.env): {key_status}"))

        path_box = QHBoxLayout()
        self.library_edit = QLineEdit(str(self.library_dir))
        btn_lib = QPushButton("…")
        btn_lib.clicked.connect(self.choose_library)
        path_box.addWidget(QLabel("Sekundärliteratur"))
        path_box.addWidget(self.library_edit, 1)
        path_box.addWidget(btn_lib)
        left_layout.addLayout(path_box)

        out_box = QHBoxLayout()
        self.out_edit = QLineEdit(str(self.output_base))
        btn_out = QPushButton("…")
        btn_out.clicked.connect(self.choose_output)
        out_box.addWidget(QLabel("Output-Basis"))
        out_box.addWidget(self.out_edit, 1)
        out_box.addWidget(btn_out)
        left_layout.addLayout(out_box)

        btns1 = QHBoxLayout()
        self.btn_generate = QPushButton("Kandidaten erzeugen")
        self.btn_generate.clicked.connect(self.on_generate)
        self.btn_dedupe = QPushButton("Deduplizieren & erweitern")
        self.btn_dedupe.clicked.connect(self.on_dedupe)
        btns1.addWidget(self.btn_generate)
        btns1.addWidget(self.btn_dedupe)
        left_layout.addLayout(btns1)

        btns2 = QHBoxLayout()
        self.btn_scan = QPushButton("Scan starten")
        self.btn_scan.clicked.connect(self.on_scan)
        self.btn_export = QPushButton("JSON + HTML exportieren")
        self.btn_export.clicked.connect(self.on_export)
        btns2.addWidget(self.btn_scan)
        btns2.addWidget(self.btn_export)
        left_layout.addLayout(btns2)

        left_layout.addWidget(QLabel("Protokoll"))
        self.log = QPlainTextEdit()
        self.log.setReadOnly(True)
        left_layout.addWidget(self.log, 1)

        # Right
        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(10, 10, 10, 10)

        lab = QLabel("Kandidaten")
        lab.setProperty("role", "title")
        right_layout.addWidget(lab)

        self.table = QTableView()
        self.model = CandidatesModel([])
        self.table.setModel(self.model)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.setAlternatingRowColors(True)
        right_layout.addWidget(self.table, 3)

        sel_box = QHBoxLayout()
        self.chk_select_all = QCheckBox("Alle auswählen")
        self.chk_select_all.stateChanged.connect(self.on_select_all)
        sel_box.addWidget(self.chk_select_all)
        sel_box.addStretch(1)
        right_layout.addLayout(sel_box)

        right_layout.addWidget(QLabel("Vorschau / JSON / Regex"))
        self.preview = QPlainTextEdit()
        self.preview.setReadOnly(True)
        right_layout.addWidget(self.preview, 2)

        root.addWidget(left)
        root.addWidget(right)
        root.setSizes([520, 760])

        self.append_log("[DEBUG] GUI bereit")

    # --- helpers ---
    def append_log(self, text: str) -> None:
        self.log.appendPlainText(text)

    def choose_library(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "Sekundärliteratur-Ordner wählen", self.library_edit.text())
        if d:
            self.library_edit.setText(d)

    def choose_output(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "Output-Basisordner wählen", self.out_edit.text())
        if d:
            self.out_edit.setText(d)

    def rows(self) -> List[CandidateRow]:
        return self.model.rows()

    def collect_selected_terms(self) -> List[str]:
        rows = self.rows()
        if not rows:
            self.append_log("[DEBUG] Keine Kandidatenzeilen vorhanden.")
            return []
        selected = [r.term for r in rows if r.selected]
        if not selected:
            self.append_log("[DEBUG] Keine Checkbox-Auswahl erkannt. Fallback: alle Begriffe verwenden.")
            self.model.force_select_all()
            self.chk_select_all.setChecked(True)
            selected = [r.term for r in rows]
        self.append_log(f"[DEBUG] Ausgewählt: {len(selected)}/{len(rows)} Begriffe")
        return selected

    # --- actions ---
    def on_generate(self) -> None:
        seed = self.seed_edit.text().strip()
        if not seed:
            QMessageBox.warning(self, "Hinweis", "Bitte einen Seed-Begriff eingeben.")
            return
        domain = self.domain_edit.text().strip()
        lang = self.lang_combo.currentText()
        model = self.model_edit.text().strip() or "gpt-4.1-mini"
        self.append_log(f"[DEBUG] OpenAI Anfrage: seed='{seed}' lang={lang} model={model}")

        try:
            payload = search_term.generate_keyword_candidates(
                seed_term=seed,
                language=lang,
                domain_hint=domain,
                model=model,
                use_cache=True,
            )
        except Exception as e:
            QMessageBox.critical(self, "OpenAI-Fehler", str(e))
            self.append_log(f"[ERROR] OpenAI: {e}")
            return

        self.generated_raw = payload
        rows: List[CandidateRow] = []
        count_terms = 0
        for cat, terms in payload.get("terms_by_category", {}).items():
            for t in terms:
                rows.append(CandidateRow(term=t, category=cat, selected=True))
                count_terms += 1

        self.model.set_rows(rows)
        self.model.force_select_all()
        self.chk_select_all.setChecked(True)

        self.preview.setPlainText(json.dumps(payload, ensure_ascii=False, indent=2))
        self.append_log(f"[DEBUG] Kandidaten geladen: {count_terms} (Zeilen: {len(rows)})")

    def on_dedupe(self) -> None:
        selected = self.collect_selected_terms()
        if not selected:
            QMessageBox.information(self, "Hinweis", "Keine Begriffe gefunden. Bitte zuerst Kandidaten erzeugen.")
            return

        deduped = keyword_utils.deduplicate_terms(selected, threshold=0.88)
        families = keyword_utils.build_families(deduped, add_historic_variants=True)
        patterns = keyword_utils.compile_family_patterns(families)

        self.expanded_terms = {fam["root"]: fam["variants"] for fam in families}
        self.preview.setPlainText(
            "Dedupliziert & erweitert:\n"
            + json.dumps(self.expanded_terms, ensure_ascii=False, indent=2)
            + "\n\nMaster-Regex:\n"
            + patterns.pattern
        )
        self.append_log(f"[DEBUG] Dedupliziert={len(deduped)} Familien={len(families)}")

    def on_scan(self) -> None:
        if not self.expanded_terms:
            self.append_log("[DEBUG] Keine erweiterten Begriffe vorhanden. Auto-Dedupe wird ausgeführt.")
            self.on_dedupe()
            if not self.expanded_terms:
                QMessageBox.information(self, "Hinweis", "Deduplizieren fehlgeschlagen. Bitte Kandidaten prüfen.")
                return

        lib = Path(self.library_edit.text())
        if not lib.exists():
            QMessageBox.warning(self, "Fehler", f"Ordner nicht gefunden:\n{lib}")
            return

        fam_list = [{"root": k, "variants": v} for k, v in self.expanded_terms.items()]
        self.append_log(f"[DEBUG] DETAIL-Scan startet in {lib}")
        result = keyword_utils.scan_library_for_families_detailed(
            base_dir=lib,
            families=fam_list,
            exclude_dir_name="chroniken",
            kwic_chars=int(self.kwic_spin.value()),
            max_samples_per_root=4,
        )
        self.scan_result = result
        summary = {
            "files_scanned": result["summary"]["files_scanned"],
            "files_with_hits": result["summary"]["files_with_hits"],
            "total_hits": result["summary"]["total_hits"],
            "top_root": (result["summary"]["totals_by_root"][0]["root"] if result["summary"]["totals_by_root"] else None),
        }
        self.preview.setPlainText("Scan-Resultate (kurz):\n" + json.dumps(summary, ensure_ascii=False, indent=2))
        self.append_log(f"[DEBUG] DETAIL-Scan fertig. Dateien={summary['files_scanned']} Hits={summary['total_hits']}")

    def _unique_session_dir(self, base_dir: Path, desired_name: str) -> Tuple[Path, str]:
        idx = 0
        while True:
            name = f"{desired_name}_{idx:02d}" if idx > 0 else desired_name
            p = base_dir / name
            if not p.exists():
                return p, name
            idx += 1

    def on_export(self) -> None:
        out_base = Path(self.out_edit.text())
        desired = self.session_edit.text().strip()
        session_dir, session_name = self._unique_session_dir(out_base, desired)
        session_dir.mkdir(parents=True, exist_ok=True)

        if self.expanded_terms:
            master_regex = keyword_utils.compile_family_patterns(
                [{"root": k, "variants": v} for k, v in self.expanded_terms.items()]
            ).pattern
        else:
            master_regex = ""

        payload = {
            "session": {"name": session_name, "created_at": datetime.utcnow().isoformat() + "Z"},
            "seed": self.seed_edit.text().strip(),
            "generated_raw": self.generated_raw,
            "expanded_terms": self.expanded_terms,
            "master_regex": master_regex,
            "scan_result": self.scan_result,
            "config": {
                "library_dir": self.library_edit.text(),
                "kwic_chars": int(self.kwic_spin.value()),
                "model": self.model_edit.text().strip(),
                "language": self.lang_combo.currentText(),
            }
        }
        out_json = session_dir / "candidates_and_scan.json"
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        paths = html_report.render_html_report(payload, session_dir)

        self.append_log(f"[DEBUG] JSON exportiert: {out_json}")
        self.append_log(f"[DEBUG] HTML: {paths.get('html')}")
        self.append_log(f"[DEBUG] CSS : {paths.get('css')}")
        self.append_log(f"[DEBUG] SCSS: {paths.get('scss')}")
        QMessageBox.information(self, "Export", f"Gespeichert:\n{out_json}\n{paths.get('html')}")

        next_name = _next_session_name(session_name, out_base)
        self.session_edit.setText(next_name)
        self.append_log(f"[DEBUG] Nächster Session-Name: {next_name}")

    def on_select_all(self, state: int) -> None:
        if state == Qt.Checked:
            self.model.force_select_all()
        else:
            for r in self.model.rows():
                r.selected = False
            self.model.layoutChanged.emit()


def main() -> None:
    print("[DEBUG] Start gui.py")
    app = QApplication([])
    apply_dark_theme(app)
    win = MainWindow()
    win.show()
    app.exec()
    print("[DEBUG] Ende gui.py")


if __name__ == "__main__":
    main()
