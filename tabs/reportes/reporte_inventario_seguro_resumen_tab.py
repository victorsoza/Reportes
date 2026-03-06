from typing import Optional, Sequence, List, Any, cast
import logging
import re
import functools

from PyQt6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QMenu,
    QWidgetAction,
    QListWidget,
    QListWidgetItem,
    QToolButton,
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QGuiApplication

logger = logging.getLogger("app")


def _parse_number(v) -> float:
    try:
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        # remove currency symbol and spaces
        s = s.replace('$', '').replace('\xa0', '').replace(' ', '')
        # If both '.' and ',' present, assume '.' thousand sep and ',' decimal
        if '.' in s and ',' in s:
            s = s.replace('.', '').replace(',', '.')
        else:
            # If only comma present, treat it as decimal separator
            if ',' in s and '.' not in s:
                s = s.replace(',', '.')
        # remove any remaining non-number characters except dot and minus
        s = re.sub(r'[^0-9.\-]', '', s)
        return float(s) if s not in ('', '-', '.') else 0.0
    except Exception:
        return 0.0


class ReporteInventarioSeguroResumenTab(QWidget):
    """Subpestaña 'Resumen' que muestra dos tablas agregadas e independientes.

    Tabla 1 (por `Clase`): columnas `Clase`, `Costo Dolares` (suma). Filtro: `No. Scm`.
    Tabla 2 (por `No. Scm`): columnas `No. Scm`, `Costo Dolares` (suma). Filtro: `Clase`.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._last_rows: Optional[Sequence] = None
        self._last_cols: Optional[Sequence[str]] = None

        # Selected filter sets (None means no multi-select filter applied)
        self._filter_scm_values: Optional[set] = None
        self._filter_clase_values: Optional[set] = None
        # SCM filter specific for the right (SCM) table
        self._filter_scm_right_values: Optional[set] = None
        # Default-unchecked items (will be applied when data is available)
        self._default_unchecked_scm = {"14", "22", "121", "4", "0", "099_C", "15_lgdn", "100_auxcc","101_cc", "102_cc"}
        self._default_unchecked_clase = {"TABLETAS", "LLANTAS", "ESCAPE", "COMPRESOR", "CELULARES"}

        layout = QVBoxLayout(self)

        # Horizontal container with two panels: left=Clase, right=No. Scm
        panels_h = QHBoxLayout()

        # Left panel (Clase)
        left_v = QVBoxLayout()
        hdr1 = QHBoxLayout()
        hdr1.addWidget(QLabel("Tabla por Clase"))
        hdr1.addStretch()
        hdr1.addWidget(QLabel("Filtrar por No. Scm:"))
        self._filter_scm = QLineEdit(self)
        self._filter_scm.setPlaceholderText("Dejar vacío para todas")
        hdr1.addWidget(self._filter_scm)
        # filter button for SCM
        self._filter_scm_btn = QToolButton(self)
        self._filter_scm_btn.setText('\u25BE')
        self._filter_scm_btn.setToolTip('Abrir filtro (No. Scm)')
        hdr1.addWidget(self._filter_scm_btn)
        left_v.addLayout(hdr1)

        self._table_clase = QTableWidget(self)
        self._table_clase.setColumnCount(2)
        self._table_clase.setHorizontalHeaderLabels(["Clase", "Costo Dolares"])
        hdr = self._table_clase.horizontalHeader()
        if hdr is not None:
            hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
            hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
            try:
                f = hdr.font()
                f.setBold(True)
                hdr.setFont(f)
                hdr.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
            except Exception:
                pass
        # Improve visuals
        self._table_clase.setAlternatingRowColors(True)
        self._table_clase.setShowGrid(False)
        vh = self._table_clase.verticalHeader()
        if vh is not None:
            vh.setVisible(False)
        self._table_clase.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table_clase.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table_clase.setSortingEnabled(True)
        left_v.addWidget(self._table_clase)

        # Right panel (No. Scm)
        right_v = QVBoxLayout()
        hdr2 = QHBoxLayout()
        hdr2.addWidget(QLabel("Tabla por No. Scm"))
        hdr2.addStretch()
        hdr2.addWidget(QLabel("Filtrar por Clase:"))
        self._filter_clase = QLineEdit(self)
        self._filter_clase.setPlaceholderText("Dejar vacío para todas")
        hdr2.addWidget(self._filter_clase)
        # filter by SCM for the SCM table (right panel)
        hdr2.addWidget(QLabel("Filtrar por No. Scm:"))
        self._filter_scm_right = QLineEdit(self)
        self._filter_scm_right.setPlaceholderText("Dejar vacío para todas")
        hdr2.addWidget(self._filter_scm_right)
        # filter button for Clase
        self._filter_clase_btn = QToolButton(self)
        self._filter_clase_btn.setText('\u25BE')
        self._filter_clase_btn.setToolTip('Abrir filtro (Clase)')
        hdr2.addWidget(self._filter_clase_btn)
        # filter button for SCM (right table)
        self._filter_scm_right_btn = QToolButton(self)
        self._filter_scm_right_btn.setText('\u25BE')
        self._filter_scm_right_btn.setToolTip('Abrir filtro (No. Scm) para tabla SCM')
        hdr2.addWidget(self._filter_scm_right_btn)
        right_v.addLayout(hdr2)

        self._table_scm = QTableWidget(self)
        self._table_scm.setColumnCount(2)
        self._table_scm.setHorizontalHeaderLabels(["No. Scm", "Costo Dolares"])
        hdr2h = self._table_scm.horizontalHeader()
        if hdr2h is not None:
            hdr2h.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
            hdr2h.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
            try:
                f2 = hdr2h.font()
                f2.setBold(True)
                hdr2h.setFont(f2)
                hdr2h.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
            except Exception:
                pass
        # Improve visuals for SCM table
        self._table_scm.setAlternatingRowColors(True)
        self._table_scm.setShowGrid(False)
        vh2 = self._table_scm.verticalHeader()
        if vh2 is not None:
            vh2.setVisible(False)
        self._table_scm.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table_scm.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table_scm.setSortingEnabled(True)
        right_v.addWidget(self._table_scm)

        # enable per-table context menus for copying
        try:
            self._table_clase.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
            self._table_clase.customContextMenuRequested.connect(
                lambda pos: self._show_table_context_menu(self._table_clase, pos)
            )
            self._table_scm.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
            self._table_scm.customContextMenuRequested.connect(
                lambda pos: self._show_table_context_menu(self._table_scm, pos)
            )
        except Exception:
            pass

        # Add panels to horizontal layout with equal stretch (half/half)
        panels_h.addLayout(left_v, 1)
        panels_h.addLayout(right_v, 1)
        layout.addLayout(panels_h)

        # connect filter controls
        try:
            self._filter_scm_btn.clicked.connect(self._open_scm_filter_menu)
            self._filter_clase_btn.clicked.connect(self._open_clase_filter_menu)
            self._filter_scm_right_btn.clicked.connect(self._open_scm_right_filter_menu)
        except Exception:
            pass
        self._filter_scm.textChanged.connect(self._on_filters_changed)
        self._filter_clase.textChanged.connect(self._on_filters_changed)
        self._filter_scm_right.textChanged.connect(self._on_filters_changed)

    def _build_filter_menu(self, items: List[str], checked: Optional[set]) -> QMenu:
        menu = QMenu(self)
        logger.debug("Building filter menu with %d items", len(items))
        container = QWidget(self)
        v = QVBoxLayout(container)
        search = QLineEdit(container)
        search.setPlaceholderText('Buscar...')
        list_widget = QListWidget(container)
        list_widget.setSelectionMode(QListWidget.SelectionMode.NoSelection)
        for val in items:
            itm = QListWidgetItem(str(val))
            itm.setFlags(itm.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            if checked is None:
                itm.setCheckState(Qt.CheckState.Checked)
            else:
                itm.setCheckState(Qt.CheckState.Checked if val in checked else Qt.CheckState.Unchecked)
            list_widget.addItem(itm)
        v.addWidget(search)
        v.addWidget(list_widget)
        wa = QWidgetAction(menu)
        wa.setDefaultWidget(container)
        menu.addAction(wa)

        # helper filtering
        def _filter_list(text: str) -> None:
            needle = text.lower()
            for i in range(list_widget.count()):
                item = list_widget.item(i)
                if item is None:
                    continue
                item.setHidden(needle not in item.text().lower())

        search.textChanged.connect(_filter_list)

        # Actions
        ok = menu.addAction('Aceptar')
        cancel = menu.addAction('Cancelar')
        cast(Any, menu)._list_widget = list_widget
        cast(Any, menu)._ok = ok
        cast(Any, menu)._cancel = cancel
        return menu

    def _collect_checked_from_menu(self, menu: QMenu) -> set:
        res = set()
        lw: QListWidget = getattr(menu, '_list_widget')
        logger.debug("Collecting checked items from menu (total candidates=%d)", lw.count())
        for i in range(lw.count()):
            it = lw.item(i)
            if it is None:
                continue
            if it.checkState() == Qt.CheckState.Checked:
                res.add(it.text())
        logger.debug("Collected %d checked items from menu", len(res))
        return res

    def _open_scm_filter_menu(self):
        # Build unique list of SCM values from last rows
        if not self._last_rows or not self._last_cols:
            return
        scm_idx = None
        for i, c in enumerate(self._last_cols):
            if 'scm' in c.lower():
                scm_idx = i
                break
        if scm_idx is None:
            return
        values = set()
        for r in self._last_rows:
            try:
                v = r[scm_idx]
                values.add('' if v is None else str(v))
            except Exception:
                pass
        items = sorted(values, key=lambda x: (x == '', x))
        menu = self._build_filter_menu(items, self._filter_scm_values)
        logger.info("Opening SCM filter menu with %d items", len(items))
        action = menu.exec(self._filter_scm_btn.mapToGlobal(self._filter_scm_btn.rect().bottomLeft()))
        if action == getattr(menu, '_ok'):
            newset = self._collect_checked_from_menu(menu)
            # if all selected, treat as no filter
            if len(newset) == len(items):
                self._filter_scm_values = None
                if getattr(self, '_filter_scm', None) is not None:
                    self._filter_scm.setText('')
                logger.info("SCM filter cleared (all selected)")
            else:
                self._filter_scm_values = newset
                if getattr(self, '_filter_scm', None) is not None:
                    self._filter_scm.setText(f"({len(newset)}) seleccionados")
                logger.info("SCM filter applied: %d selected", len(newset))
            self._on_filters_changed()
        else:
            logger.info("SCM filter dialog closed without applying changes")

    def _open_scm_right_filter_menu(self):
        # Build unique list of SCM values from last rows for the right SCM table
        if not self._last_rows or not self._last_cols:
            return
        scm_idx = None
        for i, c in enumerate(self._last_cols):
            if 'scm' in c.lower():
                scm_idx = i
                break
        if scm_idx is None:
            return
        values = set()
        for r in self._last_rows:
            try:
                v = r[scm_idx]
                values.add('' if v is None else str(v))
            except Exception:
                pass
        items = sorted(values, key=lambda x: (x == '', x))
        menu = self._build_filter_menu(items, self._filter_scm_right_values if hasattr(self, '_filter_scm_right_values') else None)
        logger.info("Opening SCM(right) filter menu with %d items", len(items))
        action = menu.exec(self._filter_scm_right_btn.mapToGlobal(self._filter_scm_right_btn.rect().bottomLeft()))
        if action == getattr(menu, '_ok'):
            newset = self._collect_checked_from_menu(menu)
            # if all selected, treat as no filter
            if len(newset) == len(items):
                self._filter_scm_right_values = None
                if getattr(self, '_filter_scm_right', None) is not None:
                    self._filter_scm_right.setText('')
                logger.info("SCM(right) filter cleared (all selected)")
            else:
                self._filter_scm_right_values = newset
                if getattr(self, '_filter_scm_right', None) is not None:
                    self._filter_scm_right.setText(f"({len(newset)}) seleccionados")
                logger.info("SCM(right) filter applied: %d selected", len(newset))
            self._on_filters_changed()
        else:
            logger.info("SCM(right) filter dialog closed without applying changes")

    def _open_clase_filter_menu(self):
        if not self._last_rows or not self._last_cols:
            return
        clase_idx = None
        for i, c in enumerate(self._last_cols):
            if 'clase' in c.lower():
                clase_idx = i
                break
        if clase_idx is None:
            return
        values = set()
        for r in self._last_rows:
            try:
                v = r[clase_idx]
                values.add('' if v is None else str(v))
            except Exception:
                pass
        items = sorted(values, key=lambda x: (x == '', x))
        menu = self._build_filter_menu(items, self._filter_clase_values)
        logger.info("Opening Clase filter menu with %d items", len(items))
        action = menu.exec(self._filter_clase_btn.mapToGlobal(self._filter_clase_btn.rect().bottomLeft()))
        if action == getattr(menu, '_ok'):
            newset = self._collect_checked_from_menu(menu)
            if len(newset) == len(items):
                self._filter_clase_values = None
                if getattr(self, '_filter_clase', None) is not None:
                    self._filter_clase.setText('')
                logger.info("Clase filter cleared (all selected)")
            else:
                self._filter_clase_values = newset
                if getattr(self, '_filter_clase', None) is not None:
                    self._filter_clase.setText(f"({len(newset)}) seleccionados")
                logger.info("Clase filter applied: %d selected", len(newset))
            self._on_filters_changed()
        else:
            logger.info("Clase filter dialog closed without applying changes")

    def _on_filters_changed(self, _=None):
        # Recompute aggregates using stored rows/cols and current filters
        try:
            fsr = getattr(self, '_filter_scm_right', None)
            fsr_text = fsr.text() if fsr is not None else ''
            # compute local refs to avoid passing None to len()
            m_scm = self._filter_scm_values
            m_scm_right = getattr(self, '_filter_scm_right_values', None)
            m_clase = self._filter_clase_values
            multi_scm = None if m_scm is None else f"{len(m_scm)} items"
            multi_scm_right = None if m_scm_right is None else f"{len(m_scm_right)} items"
            multi_clase = None if m_clase is None else f"{len(m_clase)} items"
            logger.info(
                "Filters changed: text_scm=%r text_scm_right=%r text_clase=%r multi_scm=%s multi_scm_right=%s multi_clase=%s",
                self._filter_scm.text(),
                fsr_text,
                self._filter_clase.text(),
                multi_scm,
                multi_scm_right,
                multi_clase,
            )
            self.update_summary(self._last_rows, self._last_cols)
        except Exception:
            logger.exception("Error aplicando filtros en Resumen")

    def update_summary(self, rows: Optional[Sequence[Sequence]], cols: Optional[Sequence[str]]) -> None:
        """Recalcula y muestra las dos tablas a partir de `rows` y `cols`.

        `rows`: sequence of sequences; `cols`: sequence of column names.
        """
        try:
            self._last_rows = rows
            self._last_cols = cols
            logger.info(
                "update_summary called: rows=%d cols=%d",
                0 if rows is None else len(rows),
                0 if cols is None else len(cols),
            )
            if not rows or not cols:
                # Clear tables
                self._table_clase.setRowCount(0)
                self._table_scm.setRowCount(0)
                logger.info("update_summary: no data, cleared tables")
                return

            # Determine indices
            clase_idx = None
            costo_idx = None
            scm_idx = None
            for i, c in enumerate(cols):
                lc = c.lower()
                if 'clase' == lc or 'clase' in lc:
                    clase_idx = i
                if 'costo' in lc and 'dola' in lc:
                    costo_idx = i
                if 'scm' in lc:
                    scm_idx = i

            # Apply default unchecked filters once when data is present and no user selection exists
            try:
                if rows and scm_idx is not None:
                    avail_scm = set()
                    for r in rows:
                        try:
                            v = r[scm_idx]
                            avail_scm.add('' if v is None else str(v))
                        except Exception:
                            pass
                    # choose checked = all available minus defaults (case-insensitive match)
                    if self._filter_scm_values is None:
                        checked = set()
                        for val in avail_scm:
                            if str(val).upper() not in self._default_unchecked_scm:
                                checked.add(val)
                        # If checked equals all avail, treat as no filter (None)
                        if len(checked) == len(avail_scm):
                            self._filter_scm_values = None
                        else:
                            self._filter_scm_values = checked
                            # reflect in line edit
                            if getattr(self, '_filter_scm', None) is not None:
                                self._filter_scm.setText(f"({len(checked)}) seleccionados")
                    # also apply defaults for right-side SCM filter if not set
                    if self._filter_scm_right_values is None:
                        checked_r = set()
                        for val in avail_scm:
                            if str(val).upper() not in self._default_unchecked_scm:
                                checked_r.add(val)
                        if len(checked_r) == len(avail_scm):
                            self._filter_scm_right_values = None
                        else:
                            self._filter_scm_right_values = checked_r
                            if getattr(self, '_filter_scm_right', None) is not None:
                                self._filter_scm_right.setText(f"({len(checked_r)}) seleccionados")

                if rows and clase_idx is not None and self._filter_clase_values is None:
                    avail_clase = set()
                    for r in rows:
                        try:
                            v = r[clase_idx]
                            avail_clase.add('' if v is None else str(v))
                        except Exception:
                            pass
                    checked2 = set()
                    for val in avail_clase:
                        if str(val).upper() not in self._default_unchecked_clase:
                            checked2.add(val)
                    if len(checked2) == len(avail_clase):
                        self._filter_clase_values = None
                    else:
                        self._filter_clase_values = checked2
                        if getattr(self, '_filter_clase', None) is not None:
                            self._filter_clase.setText(f"({len(checked2)}) seleccionados")
            except Exception:
                logger.exception("Error aplicando filtros por defecto en Resumen")

            # Apply text filters (they filter source rows prior to aggregation)
            f_scm = self._filter_scm.text().strip()
            f_clase = self._filter_clase.text().strip()
            _fsr = getattr(self, '_filter_scm_right', None)
            f_scm_right = _fsr.text().strip() if _fsr is not None else ""

            # Build aggregates independently so filters affect only their target table
            by_clase = {}
            by_scm = {}
            for r in rows:
                try:
                    # Extract values safely
                    clase_val = r[clase_idx] if clase_idx is not None and clase_idx < len(r) else None
                    scm_val = r[scm_idx] if scm_idx is not None and scm_idx < len(r) else None
                    costo_val = r[costo_idx] if costo_idx is not None and costo_idx < len(r) else 0
                    # Normalize strings
                    clase_str = "" if clase_val is None else str(clase_val).strip()
                    scm_str = "" if scm_val is None else str(scm_val).strip()
                    cnum = _parse_number(costo_val)

                    # Decide contribution to Clase-aggregate (left table)
                    add_to_clase = True
                    if self._filter_scm_values is None and f_scm:
                        if f_scm not in scm_str:
                            add_to_clase = False
                    if self._filter_scm_values is not None and scm_str not in self._filter_scm_values:
                        add_to_clase = False
                    if add_to_clase:
                        by_clase[clase_str] = by_clase.get(clase_str, 0.0) + cnum

                    # Decide contribution to SCM-aggregate (right table)
                    add_to_scm = True
                    # apply Clase-based filters first (right table is filtered by Clase)
                    if self._filter_clase_values is None and f_clase:
                        if f_clase not in clase_str:
                            add_to_scm = False
                    if self._filter_clase_values is not None and clase_str not in self._filter_clase_values:
                        add_to_scm = False
                    # apply SCM-right filters (text and multi-select)
                    if add_to_scm:
                        if self._filter_scm_right_values is None and f_scm_right:
                            if f_scm_right not in scm_str:
                                add_to_scm = False
                        if self._filter_scm_right_values is not None and scm_str not in self._filter_scm_right_values:
                            add_to_scm = False
                    if add_to_scm:
                        by_scm[scm_str] = by_scm.get(scm_str, 0.0) + cnum
                except Exception:
                    logger.exception("Error procesando fila para resumen")

            # Populate tables sorted by descending cost
            clase_items = sorted(by_clase.items(), key=lambda x: (-x[1], x[0]))
            scm_items = sorted(by_scm.items(), key=lambda x: (-x[1], x[0]))

            # Fill Clase table (append total row at end)
            try:
                prev_sort = self._table_clase.isSortingEnabled()
            except Exception:
                prev_sort = True
            try:
                self._table_clase.setSortingEnabled(False)
            except Exception:
                pass
            total_clase = sum(v for _, v in clase_items)
            self._table_clase.setRowCount(len(clase_items) + 1)
            for r, (cl, s) in enumerate(clase_items):
                it_cl = QTableWidgetItem(str(cl))
                it_cl.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
                it_cost = QTableWidgetItem(f"{s:,.2f}")
                it_cost.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight)
                self._table_clase.setItem(r, 0, it_cl)
                self._table_clase.setItem(r, 1, it_cost)
            # total row
            tr0 = QTableWidgetItem("Total general")
            tr0.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
            tr1 = QTableWidgetItem(f"{total_clase:,.2f}")
            tr1.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight)
            # Bold font for totals
            try:
                f_tot = tr0.font()
                f_tot.setBold(True)
                tr0.setFont(f_tot)
                tr1.setFont(f_tot)
            except Exception:
                pass
            # place total in last row
            last_r = len(clase_items)
            self._table_clase.setItem(last_r, 0, tr0)
            self._table_clase.setItem(last_r, 1, tr1)
            try:
                self._table_clase.setSortingEnabled(bool(prev_sort))
            except Exception:
                pass

            # Fill SCM table (append total row at end)
            try:
                prev_sort2 = self._table_scm.isSortingEnabled()
            except Exception:
                prev_sort2 = True
            try:
                self._table_scm.setSortingEnabled(False)
            except Exception:
                pass
            total_scm = sum(v for _, v in scm_items)
            self._table_scm.setRowCount(len(scm_items) + 1)
            for r, (scm, s) in enumerate(scm_items):
                it_sc = QTableWidgetItem(str(scm))
                it_sc.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                it_cost = QTableWidgetItem(f"{s:,.2f}")
                it_cost.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight)
                self._table_scm.setItem(r, 0, it_sc)
                self._table_scm.setItem(r, 1, it_cost)
            # total row
            tr0s = QTableWidgetItem("Total general")
            tr0s.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            tr1s = QTableWidgetItem(f"{total_scm:,.2f}")
            tr1s.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight)
            try:
                f_tot2 = tr0s.font()
                f_tot2.setBold(True)
                tr0s.setFont(f_tot2)
                tr1s.setFont(f_tot2)
            except Exception:
                pass
            last_r2 = len(scm_items)
            self._table_scm.setItem(last_r2, 0, tr0s)
            self._table_scm.setItem(last_r2, 1, tr1s)
            try:
                self._table_scm.setSortingEnabled(bool(prev_sort2))
            except Exception:
                pass
            logger.info(
                "update_summary finished: clase_rows=%d scm_rows=%d total_clase=%.2f total_scm=%.2f",
                len(clase_items),
                len(scm_items),
                total_clase,
                total_scm,
            )

        except Exception:
            logger.exception("Error actualizando resumen")

    def _show_table_context_menu(self, table: QTableWidget, pos) -> None:
        try:
            menu = QMenu(self)
            act_sel = menu.addAction('Copiar selección')
            act_all = menu.addAction('Copiar todo')
            act = menu.exec(table.mapToGlobal(pos))
            if act == act_sel:
                self._copy_table_to_clipboard(table, all_rows=False)
            elif act == act_all:
                self._copy_table_to_clipboard(table, all_rows=True)
        except Exception:
            logger.exception('Error mostrando menú de tabla')

    def _copy_table_to_clipboard(self, table: QTableWidget, all_rows: bool = False) -> None:
        try:
            rows = table.rowCount()
            cols = table.columnCount()
            # headers
            headers = []
            for c in range(cols):
                hi = table.horizontalHeaderItem(c)
                headers.append('' if hi is None else hi.text())
            lines = ['\t'.join(headers)]

            if not all_rows:
                ranges = table.selectedRanges()
                if ranges:
                    rng = ranges[0]
                    for r in range(rng.topRow(), rng.bottomRow() + 1):
                        row_vals = []
                        for c in range(rng.leftColumn(), rng.rightColumn() + 1):
                            it = table.item(r, c)
                            row_vals.append('' if it is None else it.text())
                        lines.append('\t'.join(row_vals))
                else:
                    # fallback: copy all (guard item lookup to satisfy static checkers)
                    for r in range(rows):
                        row_vals = []
                        for c in range(cols):
                            it = table.item(r, c)
                            row_vals.append(it.text() if it is not None else '')
                        lines.append('\t'.join(row_vals))
            else:
                for r in range(rows):
                    row_vals = []
                    for c in range(cols):
                        it = table.item(r, c)
                        row_vals.append(it.text() if it is not None else '')
                    lines.append('\t'.join(row_vals))

            text = '\n'.join(lines)
            cb = QGuiApplication.clipboard()
            if cb is not None:
                cb.setText(text)
                logger.info('Copied %d rows from table to clipboard', max(0, len(lines) - 1))
            else:
                logger.warning('Clipboard not available; copy skipped')
        except Exception:
            logger.exception('Error copiando tabla al portapapeles')
