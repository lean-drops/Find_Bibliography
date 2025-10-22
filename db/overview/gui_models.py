# FILE: db/overview/gui_models.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Qt-TableModels für Werke & Aufträge
- Sauberes Filtern (owned / missing / oa / ordered)
- Programmgesteuerte Sortierung (sort(column, order)) + Header-Sort
"""

from typing import List, Dict, Optional, Any

try:
    from PySide6.QtCore import QAbstractTableModel, Qt
except Exception:  # pragma: no cover
    from PySide2.QtCore import QAbstractTableModel, Qt


class WorksModel(QAbstractTableModel):
    COLS = ["id", "author", "title", "year", "type", "oa", "owned", "file_exists", "open_acq"]
    HDRS = ["ID", "Autor", "Titel", "Jahr", "Typ", "OA", "Besitz", "Datei", "Offene Aufträge"]

    def __init__(self, list_works_callable):
        super().__init__()
        self._list_works = list_works_callable
        self.rows: List[Dict[str, Any]] = []
        self._search: str = ""
        self._filter: Optional[Dict[str, bool]] = None
        self._sort_col: int = 0
        self._sort_order = Qt.AscendingOrder

    # --- Qt API ---
    def rowCount(self, parent=None):  # noqa
        return len(self._filtered_sorted())

    def columnCount(self, parent=None):  # noqa
        return len(self.COLS)

    def data(self, idx, role=Qt.DisplayRole):
        if not idx.isValid():
            return None
        row = self._filtered_sorted()[idx.row()]
        key = self.COLS[idx.column()]
        if role in (Qt.DisplayRole, Qt.EditRole):
            val = row.get(key, "")
            return "" if val is None else str(val)
        if role == Qt.TextAlignmentRole and key in ("year", "oa", "owned", "file_exists", "open_acq"):
            return Qt.AlignCenter
        return None

    def headerData(self, section, orient, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        return self.HDRS[section] if orient == 1 else section + 1  # Horizontal == 1

    def sort(self, column: int, order: int = Qt.AscendingOrder) -> None:
        """Wird von Header-Klicks ODER der Sortierleiste aufgerufen."""
        if 0 <= column < len(self.COLS):
            self._sort_col = column
            self._sort_order = order
            self.layoutChanged.emit()

    # --- Public API ---
    def reload(self):
        self.rows = self._list_works()
        self.layoutChanged.emit()

    def set_search(self, s: str):
        self._search = (s or "").lower()
        self.layoutChanged.emit()

    def set_filter(self, f: Optional[Dict[str, bool]]):
        """f = {'owned': bool, 'missing': bool, 'oa': bool, 'ordered': bool} oder None"""
        self._filter = f
        self.layoutChanged.emit()

    def work_id_at(self, row: int) -> int:
        return int(self._filtered_sorted()[row]["id"])

    # --- intern ---
    def _filtered_sorted(self) -> List[Dict[str, Any]]:
        rs = self._apply_search(self.rows)
        rs = self._apply_filter(rs)
        rs = self._apply_sort(rs)
        return rs

    def _apply_search(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not self._search:
            return items
        s = self._search
        return [
            r for r in items
            if s in (r.get("author", "") or "").lower()
            or s in (r.get("title", "") or "").lower()
        ]

    def _apply_filter(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        f = self._filter
        if not f:
            return items

        out: List[Dict[str, Any]] = []
        for r in items:
            is_owned   = bool(r.get("owned"))
            is_present = bool(r.get("file_exists"))
            is_oa      = bool(r.get("oa"))
            ordered    = int(r.get("open_acq", 0)) > 0

            keep = True
            if f.get("owned"):     # nur vorhandene Titel (owned=1)
                keep = keep and is_owned
            if f.get("missing"):   # nur fehlende Dateien
                keep = keep and (not is_present)
            if f.get("oa"):        # nur OA
                keep = keep and is_oa
            if f.get("ordered"):   # nur mit offenen Aufträgen
                keep = keep and ordered

            if keep:
                out.append(r)
        return out

    def _apply_sort(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        col = self._sort_col
        keyname = self.COLS[col]

        def k(r):
            v = r.get(keyname)
            # numerische Spalten stabil sortieren
            if keyname in ("id", "year", "oa", "owned", "file_exists", "open_acq"):
                try:
                    return int(v if v is not None and v != "" else -10**9)
                except Exception:
                    return -10**9
            # strings (case-insensitive)
            return (str(v or "")).lower()

        rev = (self._sort_order == Qt.DescendingOrder)
        return sorted(items, key=k, reverse=rev)


class AcqModel(QAbstractTableModel):
    COLS = ["id", "work_id", "author", "title", "action", "status", "priority", "proposed_url", "library", "notes"]
    HDRS = ["ID", "Werk-ID", "Autor", "Titel", "Aktion", "Status", "Prio", "URL", "Bibliothek", "Notiz"]

    def __init__(self, list_acq_callable):
        super().__init__()
        self._list_acq = list_acq_callable
        self.rows: List[Dict[str, Any]] = []
        self._sort_col: int = 0
        self._sort_order = Qt.AscendingOrder

    def rowCount(self, parent=None):  # noqa
        return len(self.rows)

    def columnCount(self, parent=None):  # noqa
        return len(self.COLS)

    def data(self, idx, role=Qt.DisplayRole):
        if not idx.isValid():
            return None
        row = self.rows[idx.row()]
        key = self.COLS[idx.column()]
        if role in (Qt.DisplayRole, Qt.EditRole):
            return "" if row.get(key) is None else str(row.get(key))
        if role == Qt.TextAlignmentRole and key in ("priority", "work_id"):
            return Qt.AlignCenter
        return None

    def headerData(self, section, orient, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        return self.HDRS[section] if orient == 1 else section + 1

    def reload(self):
        self.rows = self._list_acq()
        # Anwenden der aktuellen Sortierung:
        self.sort(self._sort_col, self._sort_order)

    def sort(self, column: int, order: int = Qt.AscendingOrder) -> None:
        if 0 <= column < len(self.COLS):
            self._sort_col = column
            self._sort_order = order

            keyname = self.COLS[column]
            def k(r):
                v = r.get(keyname)
                if keyname in ("id", "work_id", "priority"):
                    try:
                        return int(v if v is not None and v != "" else -10**9)
                    except Exception:
                        return -10**9
                return (str(v or "")).lower()

            rev = (order == Qt.DescendingOrder)
            self.rows = sorted(self.rows, key=k, reverse=rev)
            self.layoutChanged.emit()