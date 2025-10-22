# FILE: db/overview/gui_widgets.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
gui_widgets.py
- Bücher-Tab: Suche, Filter (Alle/Vorhanden/Fehlend/OA/Beauftragt), Sortierleiste,
  Drag&Drop direkt auf Zeile, Kontextmenü (Datei anhängen/Bearbeiten/Löschen).
- Analyse-Tab: liest Analyse-DB, erzeugt TEMP-Views in der Session, zeigt Bibliographie-Ergebnisse
  und kann den BIB-Bereich als PDF exportieren; optional Container-Netz-Graph.
"""

from pathlib import Path
from typing import Callable, Optional
import sqlite3

# Qt
try:
    from PySide6.QtWidgets import (
        QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, QLabel,
        QTabWidget, QSplitter, QTableView, QFileDialog, QToolButton, QMenu, QMessageBox,
        QCheckBox, QInputDialog, QAbstractItemView, QComboBox, QTableWidget, QTableWidgetItem,
        QHeaderView, QFrame
    )
    from PySide6.QtCore import Qt, QSize, QTimer, QModelIndex
    from PySide6.QtGui import QAction, QDragEnterEvent, QDragMoveEvent, QDropEvent, QContextMenuEvent
except Exception:
    from PySide2.QtWidgets import (
        QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, QLabel,
        QTabWidget, QSplitter, QTableView, QFileDialog, QToolButton, QMenu, QMessageBox,
        QCheckBox, QInputDialog, QAbstractItemView, QComboBox, QTableWidget, QTableWidgetItem,
        QHeaderView, QFrame
    )
    from PySide2.QtCore import Qt, QSize, QTimer, QModelIndex
    from PySide2.QtGui import QAction, QDragEnterEvent, QDragMoveEvent, QDropEvent, QContextMenuEvent

# Projekt
import db_core as core
from .gui_models   import WorksModel
from .gui_dialogs  import NewWorkDialog, NewAcqDialog, WorkDetailDialog, EditWorkDialog
from .uploader_id  import upload_pdf_by_id



# Optional Plot / Netzwerk
HAVE_PLOT = True
try:
    import matplotlib
    matplotlib.use("QtAgg")
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure
except Exception:
    HAVE_PLOT = False

HAVE_NX = True
try:
    import networkx as nx
except Exception:
    HAVE_NX = False

def _info(msg: str) -> None:
    print(f"[INFO][gui_widgets] {msg}")

def _err(msg: str) -> None:
    print(f"[ERROR][gui_widgets] {msg}")

# ---------------------- WorksTableView ----------------------
class WorksTableView(QTableView):
    """Datei auf Zeile ziehen -> Datei an genau dieses Werk anhängen. Kontextmenü mit Datei anhängen / Bearbeiten / Löschen."""
    def __init__(self, model: WorksModel, on_refresh: Callable[[], None], parent=None):
        super().__init__(parent)
        self.setModel(model)
        self._refresh = on_refresh
        self.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.setSortingEnabled(True)
        self.setAlternatingRowColors(True)
        self.setAcceptDrops(True)
        self.setDropIndicatorShown(True)

    def _current_work_id(self, pos) -> int:
        idx = self.indexAt(pos)
        if not idx.isValid(): return -1
        return self.model().work_id_at(idx.row())  # type: ignore

    def dragEnterEvent(self, e: QDragEnterEvent):
        if e.mimeData().hasUrls(): e.acceptProposedAction()

    def dragMoveEvent(self, e: QDragMoveEvent):
        pos = getattr(e, "position", None)
        pnt = pos().toPoint() if callable(pos) else e.pos()
        idx = self.indexAt(pnt)
        if idx.isValid():
            self.selectRow(idx.row()); e.acceptProposedAction()

    def dropEvent(self, e: QDropEvent):
        pos = getattr(e, "position", None)
        pnt = pos().toPoint() if callable(pos) else e.pos()
        wid = self._current_work_id(pnt)
        if wid < 0: return
        conn = core.connect(core.DB_PATH)
        try:
            core.ensure_schema(conn)
            for url in e.mimeData().urls():
                p = Path(url.toLocalFile())
                if not (p.exists() and p.is_file() and p.suffix.lower() in core.ALLOWED_EXTS): continue
                target = core.attach_file_to_work(conn, int(wid), p, overwrite=False, lib_dir=core.LIB_DIR)
                QMessageBox.information(self, "OK", f"Datei angehängt an ID {wid}\n{target}")
        except Exception as ex:
            QMessageBox.critical(self, "Fehler", str(ex))
        finally:
            conn.close()
        self._refresh()

    def contextMenuEvent(self, event: QContextMenuEvent) -> None:  # type: ignore
        wid = self._current_work_id(event.pos())
        if wid < 0: return
        self.selectRow(self.indexAt(event.pos()).row())
        menu = QMenu(self)

        # Datei anhängen
        act_attach = QAction("Datei an dieses Werk anhängen…", self)
        def do_attach():
            pdf_path, _ = QFileDialog.getOpenFileName(self, f"PDF an ID {wid} anhängen", "", "PDF (*.pdf);;Alle Dateien (*.*)")
            if not pdf_path: return
            try:
                target = upload_pdf_by_id(pdf_path, work_id=wid, overwrite=False)
                QMessageBox.information(self, "OK", f"Datei angehängt an ID {wid}\n{target}")
                self._refresh()
            except Exception as e:
                QMessageBox.critical(self, "Fehler", str(e))
        act_attach.triggered.connect(do_attach)
        menu.addAction(act_attach)

        # Bearbeiten
        act_edit = QAction("Bearbeiten …", self)
        def do_edit():
            conn = core.connect(core.DB_PATH)
            try:
                info = core.get_work(conn, wid)
                dlg = EditWorkDialog(info, self)
                if dlg.exec_():
                    updates = dlg.get_updates()
                    core.update_work(conn, wid, updates, rename_file=True)
                    QMessageBox.information(self, "Gespeichert", "Eintrag aktualisiert.")
                    self._refresh()
            except Exception as e:
                QMessageBox.critical(self, "Fehler", str(e))
            finally:
                conn.close()
        act_edit.triggered.connect(do_edit)
        menu.addAction(act_edit)

        # Löschen
        act_del = QAction("Löschen …", self)
        def do_delete():
            conn = core.connect(core.DB_PATH)
            try:
                info = core.get_work(conn, wid)
                txt = f"Wirklich löschen?\n\nID {wid}\n{info.get('author') or '(o. A.)'} — {info.get('title')}"
                ret = QMessageBox.question(self, "Löschen bestätigen", txt, QMessageBox.Yes|QMessageBox.No)
                if ret != QMessageBox.Yes: return
                del_file = False
                if info.get("file_path"):
                    ret2 = QMessageBox.question(self, "Datei mit löschen?", f"Auch die Datei '{info['file_path']}' im Library-Ordner entfernen?",
                                                QMessageBox.Yes|QMessageBox.No)
                    del_file = (ret2 == QMessageBox.Yes)
                core.delete_work(conn, wid, delete_file=del_file)
                QMessageBox.information(self, "Gelöscht", "Eintrag entfernt.")
                self.clearSelection()
                self._refresh()
            except Exception as e:
                QMessageBox.critical(self, "Fehler", str(e))
            finally:
                conn.close()
        act_del.triggered.connect(do_delete)
        menu.addAction(act_del)

        menu.exec_(event.globalPos())

# ---------------------- Analyse-Pane ----------------------

# ---------------------- MainWindow ----------------------
class MainWindow(QMainWindow):
    def __init__(self, db_path: Path, lib_dir: Path):
        super().__init__()
        self.db_path = db_path
        self.lib_dir = lib_dir
        self.setWindowTitle("AZK Bibliografie – Übersicht")
        self.setMinimumSize(QSize(1200, 780))

        self.conn = core.connect(db_path)
        core.ensure_schema(self.conn)

        self._build_ui()
        self._load_data(initial=True)

    def _build_ui(self):
        central = QWidget(self); self.setCentralWidget(central)
        root = QVBoxLayout(central); root.setContentsMargins(10,10,10,10); root.setSpacing(10)

        # Topbar
        top = QHBoxLayout()
        self.e_search = QLineEdit(placeholderText="Suche (Autor/Titel)")
        self.e_search.textChanged.connect(self._on_search)
        btn_refresh   = QPushButton("Aktualisieren");   btn_refresh.clicked.connect(self._load_data)
        btn_resync    = QPushButton("Resync Library");  btn_resync.clicked.connect(self._resync)
        btn_new       = QPushButton("Neues Werk …");    btn_new.clicked.connect(self._new_work)
        btn_upload_id = QPushButton("PDF → ID …");      btn_upload_id.clicked.connect(self._upload_pdf_by_id)
        btn_export    = QToolButton(text="Export")
        menu = QMenu(btn_export); act = QAction("Fehlende als CSV …", self); act.triggered.connect(self._export_missing)
        menu.addAction(act); btn_export.setMenu(menu); btn_export.setPopupMode(QToolButton.InstantPopup)
        top.addWidget(self.e_search, 1); top.addWidget(btn_refresh); top.addWidget(btn_resync)
        top.addWidget(btn_new); top.addWidget(btn_upload_id); top.addWidget(btn_export)
        root.addLayout(top)

        # Sortierleiste
        sortbar = QHBoxLayout()
        sortbar.addWidget(QLabel("Sortieren nach:"))
        self.sort_field = QComboBox(); self.sort_field.addItems(["ID","Autor","Titel","Jahr","Typ","OA","Besitz","Datei","Offene Aufträge"])
        self.sort_field.currentIndexChanged.connect(self._apply_sort)
        self.sort_dir_btn = QPushButton("▲"); self.sort_dir_btn.setCheckable(True); self.sort_dir_btn.setToolTip("▲=aufsteigend, ▼=absteigend")
        self.sort_dir_btn.clicked.connect(self._apply_sort)
        sortbar.addWidget(self.sort_field); sortbar.addWidget(self.sort_dir_btn)

        self.btn_edit   = QPushButton("Bearbeiten …"); self.btn_edit.clicked.connect(self._edit_selected)
        self.btn_delete = QPushButton("Löschen …");    self.btn_delete.clicked.connect(self._delete_selected)
        sortbar.addWidget(self.btn_edit); sortbar.addWidget(self.btn_delete)
        sortbar.addStretch(1)
        root.addLayout(sortbar)

        # Split
        split = QSplitter(); split.setOrientation(Qt.Horizontal); root.addWidget(split, 1)

        # Left: Filter
        left = QWidget(); ll = QVBoxLayout(left)
        self.cb_all=QCheckBox("Alle"); self.cb_all.setChecked(True)
        self.cb_owned=QCheckBox("Vorhanden"); self.cb_missing=QCheckBox("Fehlend")
        self.cb_oa=QCheckBox("Open Access"); self.cb_order=QCheckBox("Beauftragt/Offen")
        for cb in (self.cb_all,self.cb_owned,self.cb_missing,self.cb_oa,self.cb_order):
            cb.stateChanged.connect(self._apply_filters); ll.addWidget(cb)
        split.addWidget(left)

        # Right: Tabs
        tabs = QTabWidget(); split.addWidget(tabs); split.setStretchFactor(1,1)

        # Bücher-Tab
        w_tab = QWidget(); wlay = QVBoxLayout(w_tab)
        self.m_works = WorksModel(lambda: core.list_works(self.conn))
        self.v_works = WorksTableView(self.m_works, on_refresh=self._load_data, parent=w_tab)
        self.v_works.doubleClicked.connect(self._open_details)
        wlay.addWidget(self.v_works)
        bar = QHBoxLayout()
        btn_new_acq = QPushButton("Neuer Auftrag …"); btn_new_acq.clicked.connect(self._new_acq)
        btn_attach  = QPushButton("Datei an Auswahl…"); btn_attach.clicked.connect(self._attach_to_selected)
        bar.addWidget(btn_attach); bar.addStretch(1); bar.addWidget(btn_new_acq)
        wlay.addLayout(bar)
        tabs.addTab(w_tab, "Bücher")



        # Status
        self.status = QLabel("Bereit"); self.statusBar().addPermanentWidget(self.status)

    # ------- Sort & Filter -------
    def _apply_sort(self):
        field_to_col = {"ID":0, "Autor":1, "Titel":2, "Jahr":3, "Typ":4, "OA":5, "Besitz":6, "Datei":7, "Offene Aufträge":8}
        col = field_to_col.get(self.sort_field.currentText(), 0)
        order = Qt.DescendingOrder if self.sort_dir_btn.isChecked() else Qt.AscendingOrder
        self.sort_dir_btn.setText("▼" if order==Qt.DescendingOrder else "▲")
        self.m_works.sort(col, order)

    def _apply_filters(self):
        specials = [self.cb_owned.isChecked(), self.cb_missing.isChecked(), self.cb_oa.isChecked(), self.cb_order.isChecked()]
        if any(specials):
            if self.cb_all.isChecked():
                self.cb_all.blockSignals(True); self.cb_all.setChecked(False); self.cb_all.blockSignals(False)
        else:
            if not self.cb_all.isChecked():
                self.cb_all.blockSignals(True); self.cb_all.setChecked(True); self.cb_all.blockSignals(False)
        if self.cb_all.isChecked():
            self.m_works.set_filter(None); return
        f = {"owned": self.cb_owned.isChecked(), "missing": self.cb_missing.isChecked(),
             "oa": self.cb_oa.isChecked(), "ordered": self.cb_order.isChecked()}
        self.m_works.set_filter(f)

    # ------- Data Ops -------
    def _load_data(self, initial: bool = False):
        self.m_works.reload()
        self._apply_sort(); self._apply_filters()
        self.v_works.clearSelection()
        self.status.setText(f"Werke: {self.m_works.rowCount()}")
        if initial: QTimer.singleShot(50, self._resize_cols)

    def _resize_cols(self):
        self.v_works.resizeColumnsToContents()
        self.v_works.horizontalHeader().setStretchLastSection(True)

    def _on_search(self, text: str):
        self.m_works.set_search(text); self._apply_filters()

    def _resync(self):
        core.assign_file_stubs(self.conn)
        core.sync_library(self.conn, core.LIB_DIR, True)
        self._load_data()

    def _new_work(self):
        dlg = NewWorkDialog(self)
        if dlg.exec_():
            data, fpath = dlg.get_result()
            try:
                core.add_work_with_file(self.conn, data, fpath, core.LIB_DIR)
                QMessageBox.information(self, "OK", "Werk angelegt.")
                self._load_data()
            except Exception as e:
                QMessageBox.critical(self, "Fehler", str(e))

    def _new_acq(self):
        idx = self.v_works.currentIndex()
        if not idx.isValid():
            QMessageBox.information(self,"Hinweis","Bitte zuerst ein Werk auswählen."); return
        wid = self.m_works.work_id_at(idx.row())
        dlg = NewAcqDialog(self, default_work_id=wid)
        if dlg.exec_():
            p = dlg.get_result()
            try:
                core.add_acquisition(self.conn, **p)
                QMessageBox.information(self,"OK","Auftrag erfasst."); self._load_data()
            except Exception as e:
                QMessageBox.critical(self,"Fehler",str(e))

    def _upload_pdf_by_id(self):
        wid_str, ok = QInputDialog.getText(self, "Werk-ID eingeben", "ID:")
        if not ok or not wid_str.strip(): return
        try: wid = int(wid_str.strip())
        except ValueError: QMessageBox.warning(self,"Ungültig","Die ID muss eine Zahl sein."); return
        chk = self.conn.execute("SELECT COUNT(*) AS c FROM works WHERE id=?;", (wid,)).fetchone()["c"]
        if chk == 0: QMessageBox.warning(self,"Nicht gefunden",f"Kein Werk mit ID {wid}."); return
        pdf_path, _ = QFileDialog.getOpenFileName(self, f"PDF an ID {wid} anhängen", "", "PDF (*.pdf);;Alle Dateien (*.*)")
        if not pdf_path: return
        try:
            target = upload_pdf_by_id(pdf_path, work_id=wid, overwrite=False)
            QMessageBox.information(self,"OK",f"Datei zugeordnet (ID {wid})\n{target}"); self._load_data()
        except Exception as e:
            QMessageBox.critical(self,"Fehler",str(e))

    def _attach_to_selected(self):
        idx = self.v_works.currentIndex()
        if not idx.isValid(): QMessageBox.information(self,"Hinweis","Bitte zuerst ein Werk auswählen."); return
        wid = self.m_works.work_id_at(idx.row())
        pdf_path, _ = QFileDialog.getOpenFileName(self, "Datei an ausgewähltes Werk anhängen", "", "Dokumente (*.pdf *.epub *.djvu *.txt *.docx *.zip);;Alle Dateien (*.*)")
        if not pdf_path: return
        try:
            target = core.attach_file_to_work(self.conn, int(wid), Path(pdf_path), overwrite=False, lib_dir=core.LIB_DIR)
            QMessageBox.information(self,"OK",f"Datei zugeordnet (ID {wid})\n{target}"); self._load_data()
        except Exception as e:
            QMessageBox.critical(self,"Fehler",str(e))

    def _edit_selected(self):
        idx = self.v_works.currentIndex()
        if not idx.isValid(): QMessageBox.information(self,"Hinweis","Bitte zuerst ein Werk auswählen."); return
        wid = self.m_works.work_id_at(idx.row())
        try:
            info = core.get_work(self.conn, wid)
            dlg = EditWorkDialog(info, self)
            if dlg.exec_():
                updates = dlg.get_updates()
                core.update_work(self.conn, wid, updates, rename_file=True)
                QMessageBox.information(self,"Gespeichert","Eintrag aktualisiert."); self._load_data()
        except Exception as e:
            QMessageBox.critical(self,"Fehler",str(e))

    def _delete_selected(self):
        idx = self.v_works.currentIndex()
        if not idx.isValid(): QMessageBox.information(self,"Hinweis","Bitte zuerst ein Werk auswählen."); return
        wid = self.m_works.work_id_at(idx.row())
        try:
            info = core.get_work(self.conn, wid)
            txt = f"Wirklich löschen?\n\nID {wid}\n{info.get('author') or '(o. A.)'} — {info.get('title')}"
            ret = QMessageBox.question(self,"Löschen bestätigen",txt,QMessageBox.Yes|QMessageBox.No)
            if ret != QMessageBox.Yes: return
            del_file = False
            if info.get("file_path"):
                ret2 = QMessageBox.question(self,"Datei mit löschen?", f"Auch die Datei '{info['file_path']}' im Library-Ordner entfernen?",
                                            QMessageBox.Yes|QMessageBox.No)
                del_file = (ret2 == QMessageBox.Yes)
            core.delete_work(self.conn, wid, delete_file=del_file)
            QMessageBox.information(self,"Gelöscht","Eintrag entfernt."); self.v_works.clearSelection(); self._load_data()
        except Exception as e:
            QMessageBox.critical(self,"Fehler",str(e))

    def _export_missing(self):
        path, _ = QFileDialog.getSaveFileName(self,"Fehlende als CSV exportieren","","CSV (*.csv)")
        if not path: return
        rows = core.list_works(self.conn); missing = [r for r in rows if not r.get("file_exists")]
        try:
            import csv
            with open(path,"w",newline="",encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow(["work_id","author","title","year","type","suggested_filename","oa","suggested_action"])
                for r in missing:
                    fname = r.get("file_stub") or "unknown"
                    action = "download" if r.get("oa")==1 else "scan_request"
                    w.writerow([r["id"], r.get("author",""), r.get("title",""), r.get("year") or "", r.get("type",""), fname, r.get("oa",0), action])
            QMessageBox.information(self,"Export",f"CSV gespeichert ({len(missing)} Zeilen).")
        except Exception as e:
            QMessageBox.critical(self,"Fehler",str(e))

    def _open_details(self, index: QModelIndex):
        wid = self.m_works.work_id_at(index.row())
        w = [r for r in core.list_works(self.conn) if r["id"]==wid][0]
        acqs = core.list_acquisitions(self.conn)
        w["acquisitions"] = [a for a in acqs if a.get("work_id")==wid]
        WorkDetailDialog(w, self).exec_()