from PyQt6.QtWidgets import QWidget, QVBoxLayout, QPushButton, QFileDialog, QMessageBox, QTabWidget, QTableWidget, QTableWidgetItem, QInputDialog, QHeaderView, QMenu, QWidgetAction, QLineEdit, QVBoxLayout as QVLayout, QListWidget, QListWidgetItem, QApplication, QStyledItemDelegate, QCompleter
from PyQt6.QtCore import Qt, QPoint, QSize, QObject, QThread, pyqtSignal, QStringListModel, QEvent
from PyQt6.QtGui import QIcon, QGuiApplication, QBrush, QColor, QKeySequence
from typing import Any, cast
import os


class _ProcessingWorker(QObject):
    """Worker que procesa archivos Excel en un hilo separado y emite el resultado.

    Señal: finished(dict) donde el dict contiene claves: 'combined', 'cols_list', 'files_count', 'error'
    """
    finished = pyqtSignal(object)

    def __init__(self, files: list[str]):
        super().__init__()
        self.files = files

    def run(self) -> None:
        try:
            import pandas as pd
        except Exception as e:
            self.finished.emit({'combined': None, 'cols_list': [], 'files_count': len(self.files), 'error': f"Falta pandas: {e}"})
            return

        dfs = []
        for f in self.files:
            try:
                df = pd.read_excel(f, sheet_name='DETALLE', dtype=str, keep_default_na=False)
            except Exception as e:
                # continuar con los demás archivos, reportaremos al final si no hay dfs
                continue

            try:
                df = df.fillna('').ffill()
            except Exception:
                pass
            # añadir columna con el nombre del archivo origen para cada fila
            try:
                df['Archivo'] = os.path.basename(f)
            except Exception:
                pass
            try:
                df['SISTEMA'] = ''
            except Exception:
                pass
            dfs.append(df)

        if not dfs:
            self.finished.emit({'combined': None, 'cols_list': [], 'files_count': len(self.files), 'error': 'No se encontró la hoja DETALLE en los archivos seleccionados'})
            return

        cols_list = []
        for d in dfs:
            for c in list(d.columns):
                if c not in cols_list:
                    cols_list.append(c)

        norm_dfs = []
        for d in dfs:
            try:
                norm = d.reindex(columns=cols_list, fill_value='')
            except Exception:
                norm = d
            norm_dfs.append(norm)

        try:
            combined = pd.concat(norm_dfs, ignore_index=True)
        except Exception:
            try:
                combined = norm_dfs[0]
            except Exception:
                combined = None

        if combined is None:
            self.finished.emit({'combined': None, 'cols_list': [], 'files_count': len(self.files), 'error': 'No se obtuvieron filas de DETALLE'})
            return
        # Evitar acceso opcional a 'shape' que Pylance marca como inseguro;
        # comprobar de forma explícita que existe y tiene filas.
        try:
            if getattr(combined, 'shape', (0,))[0] == 0:
                self.finished.emit({'combined': None, 'cols_list': [], 'files_count': len(self.files), 'error': 'No se obtuvieron filas de DETALLE'})
                return
        except Exception:
            pass

        # calcular MES2 y AÑO
        try:
            col_mes = None
            for c in combined.columns:
                try:
                    if str(c).strip().upper() == 'MES':
                        col_mes = c
                        break
                except Exception:
                    continue
            month_map = {
                1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
                7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
            }
            if col_mes is not None:
                try:
                    mes_num = pd.to_numeric(combined[col_mes].astype(str).str.extract(r'(\d+)', expand=False), errors='coerce').fillna(0).astype(int)
                    combined['MES2'] = mes_num.map(month_map).fillna('')
                except Exception:
                    combined['MES2'] = ''
            else:
                combined['MES2'] = ''

            col_fecha = None
            for c in combined.columns:
                try:
                    if str(c).strip().upper() in ('FECHA', 'DATE'):
                        col_fecha = c
                        break
                except Exception:
                    continue
            if col_fecha is not None:
                try:
                    s = combined[col_fecha].astype(str).str.strip()
                    dt = pd.to_datetime(s, errors='coerce', dayfirst=True)
                    years = dt.dt.year
                    year_str = years.apply(lambda y: str(int(y)) if (y is not None and not pd.isna(y)) else '')
                    try:
                        missing_mask = year_str == ''
                        if missing_mask.any():
                            ext = s[missing_mask].str.extract(r'(\d{4})', expand=False)
                            ext = ext.fillna('')
                            year_str.loc[missing_mask] = ext.astype(str)
                    except Exception:
                        pass
                    combined['AÑO'] = year_str.fillna('')
                except Exception:
                    combined['AÑO'] = ''
            else:
                combined['AÑO'] = ''
        except Exception:
            pass

        # intentar mapear SISTEMA desde SQL
        try:
            from db_config import connect_db
            try:
                conn = connect_db('compras_internacionales')
                cursor = conn.cursor()
            except Exception:
                conn = None
                cursor = None
            articulo_col = None
            for c in combined.columns:
                try:
                    if str(c).strip().upper() == 'ARTICULO':
                        articulo_col = c
                        break
                except Exception:
                    continue
            if cursor is not None and articulo_col is not None:
                try:
                    cursor.execute("SELECT [ARTICULO],[SISTEMA] FROM [ComprasInternacionales].[CI].[MarketshareRepuestos]")
                    rows = cursor.fetchall()
                    mapping: dict[str, str] = {}
                    for r in rows:
                        a = r[0]
                        s = r[1]
                        if a is None:
                            continue
                        key = str(a).strip().upper()
                        if key:
                            mapping[key] = str(s).strip() if s is not None else ''
                    if mapping:
                        for idx in range(combined.shape[0]):
                            try:
                                val = combined.iat[idx, combined.columns.get_loc(articulo_col)]
                                key = str(val).strip().upper() if val is not None else ''
                                if key and key in mapping:
                                    combined.at[idx, 'SISTEMA'] = mapping[key]
                            except Exception:
                                continue
                except Exception:
                    pass
            try:
                if cursor is not None:
                    cursor.close()
            except Exception:
                pass
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
        except Exception:
            pass

        # eliminar filas con 'TOTAL' en la segunda columna
        try:
            if len(cols_list) >= 2:
                col2 = cols_list[1]
                mask = combined[col2].astype(str).str.strip().str.upper().str.contains('TOTAL', na=False)
                if mask.any():
                    combined = combined.loc[~mask].reset_index(drop=True)
        except Exception:
            pass

        self.finished.emit({'combined': combined, 'cols_list': cols_list, 'files_count': len(self.files), 'error': None})
        return


class MarketshareRepuestosTab(QWidget):
    """Pestaña que permite seleccionar archivos y mostrar la hoja 'DETALLE' en tablas por archivo."""

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        self.process_btn = QPushButton("Procesar archivos")
        self.process_btn.clicked.connect(self.process_files)
        layout.addWidget(self.process_btn)
        # botón para guardar resultados en la tabla SQL
        self.save_btn = QPushButton("Guardar en SQL")
        self.save_btn.setEnabled(False)
        self.save_btn.clicked.connect(self.save_current_table_to_sql)
        layout.addWidget(self.save_btn)

        # contenedor de resultados: una pestaña por archivo procesado
        self.results_tabs = QTabWidget()
        layout.addWidget(self.results_tabs)

    def process_files(self) -> None:
        try:
            import pandas as pd
        except Exception:
            try:
                QMessageBox.critical(self, "Dependencia faltante", "Instale 'pandas' y 'openpyxl' en el entorno")
            except Exception:
                pass
            return

        try:
            files, _ = QFileDialog.getOpenFileNames(self, "Seleccionar archivos Excel", "", "Excel Files (*.xlsx *.xls)")
            if not files:
                return
        except Exception:
            try:
                QMessageBox.warning(self, "Error", "No se pudo abrir el diálogo de archivos")
            except Exception:
                pass
            return

        # intentar mostrar un diálogo de carga reutilizable (no crítico)
        ld = None
        try:
            try:
                from ..shared.loading_dialog import get_loading_dialog
            except Exception:
                from tabs.shared.loading_dialog import get_loading_dialog
            try:
                ld = get_loading_dialog(self)
                ld.show(f"Procesando {len(files)} archivos...")
            except Exception:
                ld = None
        except Exception:
            ld = None

        # limpiar resultados previos
        try:
            while self.results_tabs.count() > 0:
                self.results_tabs.removeTab(0)
        except Exception:
            pass

        # lanzar procesamiento en background
        try:
            worker = _ProcessingWorker(files)
            thread = QThread()
            worker.moveToThread(thread)
            thread.started.connect(worker.run)
            worker.finished.connect(lambda result: self._on_processing_finished(result, thread, worker, ld))
            # arrancar
            thread.start()
        except Exception as e:
            # Nota: no ocultar el diálogo de carga aún; esperar hasta que la tabla
            # esté creada y añadida al UI para evitar que el diálogo desaparezca
            # antes de que el usuario vea los datos.
            try:
                QMessageBox.warning(self, "Error", f"No se pudo iniciar el procesamiento en background: {e}")
            except Exception:
                pass

    def _install_header_filters(self, table: QTableWidget, cols: list[str]) -> None:
        """Añade iconos de filtro en el header y conecta el click para filtrar por columna."""
        try:
            header = table.horizontalHeader()
            if header is None:
                return
            # rutas a iconos
            base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'Iconos'))
            icon_off = os.path.join(base, 'FiltroInactivo.png')
            icon_on = os.path.join(base, 'FiltroActivo.png')
            icon_off_q = QIcon(icon_off) if os.path.exists(icon_off) else QIcon()
            icon_on_q = QIcon(icon_on) if os.path.exists(icon_on) else QIcon()

            # almacenar filtros por columna en el propio table para soportar múltiples tablas
            cast(Any, table)._col_filters = {i: '' for i in range(len(cols))}
            cast(Any, table)._col_filter_icons = (icon_off_q, icon_on_q)

            # crear botones de filtro sobre el header (uno por columna)
            cast(Any, table)._filter_buttons = []
            for i in range(len(cols)):
                try:
                    hi = table.horizontalHeaderItem(i)
                    if hi is None:
                        hi = QTableWidgetItem(str(cols[i]))
                        table.setHorizontalHeaderItem(i, hi)
                    # no asignar icono al item del header para evitar duplicado (usaremos solo el botón)
                except Exception:
                    continue
                try:
                    btn = QPushButton(header)
                    btn.setObjectName(f"rs_filter_btn_{i}")
                    btn.setFlat(True)
                    # botón con icono inactivo inicialmente
                    btn.setIcon(icon_off_q)
                    btn.setIconSize(QSize(16, 16))
                    btn.setFixedSize(20, 20)
                    btn.clicked.connect(lambda _checked, col=i, tbl=table: self.show_filter_menu_for_table(tbl, col))
                    btn.show()
                    btn.raise_()
                    cast(Any, table)._filter_buttons.append(btn)
                except Exception:
                    cast(Any, table)._filter_buttons.append(None)

            # Añadir padding a las secciones del header para que el texto no se superponga
            try:
                # usar un padding derecho al menos del ancho del botón + margen
                btn_width = 20
                pad = btn_width + 8
                header.setStyleSheet(f"QHeaderView::section {{ padding-right: {pad}px; }}")
            except Exception:
                pass

            # function to position buttons correctly
            def position_filter_buttons():
                try:
                    y = header.y()
                    h = header.height()
                    try:
                        sb = table.horizontalScrollBar()
                        scroll_val = sb.value() if sb is not None else 0
                    except Exception:
                        scroll_val = 0
                    fm = header.fontMetrics()
                    for i, btn in enumerate(cast(Any, table)._filter_buttons):
                        if btn is None:
                            continue
                        try:
                            if hasattr(header, 'sectionViewportPosition'):
                                try:
                                    x = header.sectionViewportPosition(i)
                                except Exception:
                                    x = header.sectionPosition(i) - scroll_val
                            else:
                                x = header.sectionPosition(i) - scroll_val
                            w = header.sectionSize(i)
                            max_bx = x + w - btn.width() - 4
                            bx = max_bx
                            by = y + (h - btn.height()) // 2
                            btn.move(bx, by)
                        except Exception:
                            continue
                except Exception:
                    pass

            # conectar repositioning a eventos del header/scroll
            try:
                header.sectionResized.connect(lambda logicalIndex, oldSize, newSize: position_filter_buttons())
                header.sectionMoved.connect(lambda logicalIndex, oldVisualIndex, newVisualIndex: position_filter_buttons())
                sb = table.horizontalScrollBar()
                if sb is not None:
                    sb.valueChanged.connect(lambda v: position_filter_buttons())
            except Exception:
                pass

            # inicializar posicionamiento
            try:
                position_filter_buttons()
            except Exception:
                pass
        except Exception:
            pass

    class _SistemaDelegate(QStyledItemDelegate):
        """Delegate que añade un QCompleter al editor de la columna SISTEMA
        y colorea el editor según exista o no el sistema en la lista conocida.
        """
        def __init__(self, systems: list[str], parent=None):
            super().__init__(parent)
            self._systems = sorted(set(systems))

        def createEditor(self, parent, option, index):
            editor = QLineEdit(parent)
            try:
                model = QStringListModel(self._systems, editor)
                completer = QCompleter(model, editor)
                completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
                try:
                    completer.setFilterMode(Qt.MatchFlag.MatchContains)
                except Exception:
                    pass
                completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
                editor.setCompleter(completer)
            except Exception:
                pass

            def _on_text_changed(txt: str) -> None:
                try:
                    if txt.strip().upper() in (s.upper() for s in self._systems):
                        cast(QLineEdit, editor).setStyleSheet('background: #e6ffea')
                    else:
                        cast(QLineEdit, editor).setStyleSheet('background: #fff1f0')
                except Exception:
                    pass

            editor.textChanged.connect(_on_text_changed)
            return editor

        def setEditorData(self, editor, index):
            try:
                model = index.model()
                if model is None:
                    return
                val = model.data(index, Qt.ItemDataRole.DisplayRole)
                if val is None:
                    val = ''
                cast(QLineEdit, editor).setText(str(val))
                # trigger style update
                try:
                    if str(val).strip().upper() in (s.upper() for s in self._systems):
                        cast(QLineEdit, editor).setStyleSheet('background: #e6ffea')
                    else:
                        cast(QLineEdit, editor).setStyleSheet('background: #fff1f0')
                except Exception:
                    pass
            except Exception:
                pass

        def updateEditorGeometry(self, editor, option, index):
            try:
                cast(QLineEdit, editor).setGeometry(option.rect)
            except Exception:
                pass

    # El método de clic sobre el encabezado ahora se maneja por los botones
    # y por `show_filter_menu_for_table`; la implementación antigua fue eliminada.

    def _apply_table_filters(self, table: QTableWidget) -> None:
        try:
            # build list of active filters from the table instance
            active = {i: v for i, v in getattr(table, '_col_filters', {}).items() if v}
            row_count = table.rowCount()
            col_count = table.columnCount()
            for r in range(row_count):
                show = True
                for ci, pattern in active.items():
                    try:
                        if ci < 0 or ci >= col_count:
                            continue
                        it = table.item(r, ci)
                        cell = it.text() if (it is not None and it.text() is not None) else ''
                        # special token for empty-cell filter
                        if pattern == '__RS_EMPTY__':
                            if str(cell).strip() != '':
                                show = False
                                break
                            else:
                                continue
                        if pattern.upper() not in cell.upper():
                            show = False
                            break
                    except Exception:
                        continue
                table.setRowHidden(r, not show)
        except Exception:
            pass

    def show_filter_menu_for_table(self, table: QTableWidget, col: int) -> None:
        """Muestra menú enriquecido con búsqueda y selección para `table` en la columna `col`."""
        try:
            # Collect unique values but respect other active filters (cascading filters)
            values = set()
            try:
                active_filters = getattr(table, '_col_filters', {})
            except Exception:
                active_filters = {}
            for r in range(table.rowCount()):
                try:
                    # comprobar si la fila cumple los filtros activos en las otras columnas
                    skip = False
                    for ci, pat in active_filters.items():
                        try:
                            if ci == col or not pat:
                                continue
                            # obtener valor de la otra columna
                            it_other = table.item(r, ci)
                            cell_other = it_other.text() if (it_other is not None and it_other.text() is not None) else ''
                            if pat == '__RS_EMPTY__':
                                if str(cell_other).strip() != '':
                                    skip = True
                                    break
                                else:
                                    continue
                            if pat.upper() not in cell_other.upper():
                                skip = True
                                break
                        except Exception:
                            continue
                    if skip:
                        continue
                    it = table.item(r, col)
                    v = it.text() if (it is not None and it.text() is not None) else ''
                    display = "Vacio" if v == "" else v
                    values.add(display)
                except Exception:
                    continue
            values_list = sorted(values, key=lambda x: (x is None, x))

            menu = QMenu(self)
            menu.setStyleSheet("""
                QMenu { background-color: #ffffff; border: 1px solid #cfcfcf; }
                QMenu::item { padding: 6px 24px; }
                QMenu::item:selected { background-color: #e0f7fa; }
            """)
            clear_action = menu.addAction("<Borrar filtro>")
            sort_asc_action = menu.addAction("Ordenar ascendente")
            sort_desc_action = menu.addAction("Ordenar descendente")
            menu.addSeparator()

            container = QWidget()
            container_layout = QVLayout(container)
            container_layout.setContentsMargins(4, 4, 4, 4)
            search = QLineEdit(container)
            search.setPlaceholderText("Buscar...")
            list_widget = QListWidget(container)
            list_widget.setAlternatingRowColors(True)
            for idx, v in enumerate(values_list):
                it = QListWidgetItem(str(v))
                if idx % 2 == 0:
                    it.setBackground(QBrush(QColor("#ffffff")))
                else:
                    it.setBackground(QBrush(QColor("#fbfbfb")))
                list_widget.addItem(it)
            container_layout.addWidget(search)
            container_layout.addWidget(list_widget)
            widget_action = QWidgetAction(menu)
            widget_action.setDefaultWidget(container)
            menu.addAction(widget_action)

            def _select_matches() -> None:
                st = search.text().strip()
                if st == "":
                    cast(Any, table)._col_filters[col] = ''
                else:
                    cast(Any, table)._col_filters[col] = st
                self._apply_table_filters(table)
                menu.close()

            select_action = menu.addAction("Todo")
            if select_action is not None:
                select_action.triggered.connect(_select_matches)

            try:
                if sort_asc_action is not None:
                    sort_asc_action.triggered.connect(lambda _, i=col: table.sortItems(i, Qt.SortOrder.AscendingOrder))
                if sort_desc_action is not None:
                    sort_desc_action.triggered.connect(lambda _, i=col: table.sortItems(i, Qt.SortOrder.DescendingOrder))
            except Exception:
                pass

            def _filter_list(text: str) -> None:
                needle = text.lower()
                for i in range(list_widget.count()):
                    item = list_widget.item(i)
                    if item is None:
                        continue
                    item.setHidden(needle not in item.text().lower())

            search.textChanged.connect(_filter_list)

            selected_value: dict[str, str | None] = {'v': None}
            def _on_item_clicked(item: QListWidgetItem) -> None:
                selected_value['v'] = item.text()
                menu.close()

            list_widget.itemClicked.connect(_on_item_clicked)

            # posicionar menu cerca del header
            header = table.horizontalHeader()
            pos = table.mapToGlobal(QPoint(0, table.height()))
            if header is not None:
                try:
                    try:
                        left = header.sectionViewportPosition(col)
                    except Exception:
                        left = header.sectionPosition(col)
                    section_w = header.sectionSize(col)
                    global_left = header.mapToGlobal(QPoint(left, 0)).x()
                    global_top = header.mapToGlobal(QPoint(0, header.height())).y()
                    menu_w = menu.sizeHint().width()
                    screen = QGuiApplication.screenAt(QPoint(global_left, global_top))
                    if screen is None:
                        screen = QApplication.primaryScreen()
                    if screen is not None:
                        avail = screen.availableGeometry()
                        if global_left + menu_w <= avail.x() + avail.width():
                            menu_x = global_left
                        else:
                            menu_x = global_left + max(0, section_w - menu_w)
                        menu_x = max(menu_x, avail.x())
                    else:
                        menu_x = global_left
                    pos = QPoint(int(menu_x), int(global_top))
                except Exception:
                    pos = header.mapToGlobal(QPoint(0, header.height()))

            selected_action = menu.exec(pos)
            if selected_action == clear_action:
                cast(Any, table)._col_filters[col] = ''
                # actualizar solo el botón (evitar tocar el icono del encabezado)
                try:
                    icons = getattr(table, '_col_filter_icons', (QIcon(), QIcon()))
                    btns = getattr(table, '_filter_buttons', [])
                    if 0 <= col < len(btns):
                        b = btns[col]
                        if b is not None:
                            try:
                                b.setIcon(icons[0])
                            except Exception:
                                pass
                except Exception:
                    pass
                self._apply_table_filters(table)
                return
            sel_text = selected_value.get('v')
            if sel_text is None:
                return
            # si el usuario escogió la representación 'Vacio', usar token especial
            if sel_text == "Vacio":
                cast(Any, table)._col_filters[col] = '__RS_EMPTY__'
            else:
                cast(Any, table)._col_filters[col] = sel_text
            # actualizar solo el botón a activo (evitar duplicar icono en header)
            try:
                icons = getattr(table, '_col_filter_icons', (QIcon(), QIcon()))
                btns = getattr(table, '_filter_buttons', [])
                if 0 <= col < len(btns):
                    b = btns[col]
                    if b is not None:
                        try:
                            b.setIcon(icons[1])
                        except Exception:
                            pass
            except Exception:
                pass
            self._apply_table_filters(table)
        except Exception:
            pass

    def _on_processing_finished(self, result: dict, thread: QThread, worker: QObject, ld) -> None:
        """Slot que maneja el resultado emitido por el worker en background."""
        try:
            # asegurar que el thread se pare y se limpie
            try:
                thread.quit()
            except Exception:
                pass
            try:
                thread.wait(2000)
            except Exception:
                pass
        except Exception:
            pass

        # NO ocultar aquí: moveremos el hide hasta después de añadir la tabla

        error = result.get('error') if isinstance(result, dict) else 'Resultado inesperado'
        if error:
            try:
                if ld is not None:
                    ld.hide()
            except Exception:
                pass
            try:
                QMessageBox.warning(self, "Error", str(error))
            except Exception:
                pass
            return

        combined = result.get('combined')
        cols_list = result.get('cols_list') or []
        files_count = result.get('files_count') or 0
        # crear tabla en hilo principal reutilizando la lógica previa
        if combined is None:
            try:
                QMessageBox.warning(self, "Error", "No se obtuvo la tabla combinada desde el worker")
            except Exception:
                pass
            return
        # Afirmación para ayudar al analizador estático (Pylance) a inferir el tipo
        assert combined is not None
        try:
            cols = list(cols_list)
            for extra in ('MES2', 'AÑO', 'SISTEMA'):
                if extra not in cols:
                    cols.append(extra)
            # asegurar columna 'Archivo' al final
            try:
                if 'Archivo' in cols:
                    try:
                        cols.remove('Archivo')
                    except Exception:
                        pass
                cols.append('Archivo')
            except Exception:
                pass

            total_rows = combined.shape[0]
            table = QTableWidget()
            table.setColumnCount(len(cols))
            table.setHorizontalHeaderLabels([str(c) for c in cols])
            table.setRowCount(total_rows)
            for r in range(total_rows):
                for c, col in enumerate(cols):
                    try:
                        val = combined.iat[r, combined.columns.get_loc(col)] if col in combined.columns else ('')
                        text = '' if val is None else str(val)
                        item = QTableWidgetItem(text)
                        # Permitir edición sólo en la columna 'SISTEMA'
                        try:
                            if str(col).strip().upper() == 'SISTEMA':
                                # dejar editable (por defecto)
                                pass
                            else:
                                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                        except Exception:
                            try:
                                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                            except Exception:
                                pass
                        # marcar visualmente la columna editable
                        try:
                            if str(col).strip().upper() == 'SISTEMA':
                                item.setBackground(QBrush(QColor('#fff9e6')))
                        except Exception:
                            pass
                        table.setItem(r, c, item)
                    except Exception:
                        continue

            # ajustar anchos
            try:
                header = table.horizontalHeader()
                tfm = table.fontMetrics()
                row_count = table.rowCount()
                max_rows = min(200, row_count)
                for ci in range(table.columnCount()):
                    try:
                        header_w = header.fontMetrics().horizontalAdvance(str(cols[ci])) if header is not None else 0
                        content_max = 0
                        for rr in range(max_rows):
                            try:
                                it = table.item(rr, ci)
                                if it is None:
                                    continue
                                text = it.text() or ''
                                w = tfm.horizontalAdvance(text)
                                if w > content_max:
                                    content_max = w
                            except Exception:
                                continue
                        final_w = max(header_w, content_max) + 32
                        try:
                            if header is not None:
                                try:
                                    header.setSectionResizeMode(ci, QHeaderView.ResizeMode.Fixed)
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        table.setColumnWidth(ci, int(final_w))
                    except Exception:
                        continue
            except Exception:
                pass

            try:
                self._install_header_filters(table, cols)
            except Exception:
                pass

            # cargar valores conocidos de SISTEMA desde la BD (para autocompletar)
            sistemas_list = []
            try:
                try:
                    from db_config import connect_db
                    conn = connect_db('compras_internacionales')
                    cur = conn.cursor()
                    try:
                        cur.execute("SELECT DISTINCT [SISTEMA] FROM [ComprasInternacionales].[CI].[MarketshareRepuestos] WHERE [SISTEMA] IS NOT NULL")
                        rows = cur.fetchall()
                        for r in rows:
                            try:
                                v = r[0]
                                if v is not None and str(v).strip() != '':
                                    sistemas_list.append(str(v).strip())
                            except Exception:
                                continue
                    except Exception:
                        pass
                    try:
                        cur.close()
                    except Exception:
                        pass
                    try:
                        conn.close()
                    except Exception:
                        pass
                except Exception:
                    pass
            except Exception:
                pass

            # añadir también valores presentes en la tabla combinada
            try:
                if 'SISTEMA' in combined.columns:
                    try:
                        extras = [str(x).strip() for x in combined['SISTEMA'].unique() if str(x).strip()]
                        for e in extras:
                            if e not in sistemas_list:
                                sistemas_list.append(e)
                    except Exception:
                        pass
            except Exception:
                pass

            # asignar delegate sólo si encontramos la columna
            try:
                try:
                    sistema_col_index = cols.index('SISTEMA')
                except Exception:
                    sistema_col_index = -1
                if sistema_col_index >= 0:
                    try:
                        delegate = self._SistemaDelegate(sistemas_list, table)
                        table.setItemDelegateForColumn(sistema_col_index, delegate)
                    except Exception:
                        pass
            except Exception:
                pass

            # instalar eventFilter para manejar Ctrl+C / Ctrl+V sobre la tabla
            try:
                try:
                    table.installEventFilter(self)
                except Exception:
                    pass
            except Exception:
                pass

            self.results_tabs.addTab(table, f"DETALLE (combinado) - {files_count} archivos")
            try:
                self.save_btn.setEnabled(True)
            except Exception:
                pass
            # ocultar el diálogo de carga sólo después de que la tabla esté en el UI
            try:
                if ld is not None:
                    ld.hide()
            except Exception:
                pass
        except Exception as e:
            try:
                QMessageBox.warning(self, "Error", f"No se pudo crear la tabla combinada: {e}")
            except Exception:
                pass

    def save_current_table_to_sql(self) -> None:
        """Guarda la tabla actualmente visible en la pestaña `results_tabs` en la tabla SQL target."""
        try:
            from db_config import connect_db
        except Exception:
            QMessageBox.critical(self, "Error DB", "Módulo db_config no disponible")
            return

        tbl = self.results_tabs.currentWidget()
        if not isinstance(tbl, QTableWidget):
            QMessageBox.information(self, "Nada que guardar", "No hay tabla activa para guardar")
            return

        # confirmar acción
        ok = QMessageBox.question(self, "Confirmar", "¿Guardar todas las filas visibles en la tabla [ComprasInternacionales].[CI].[MarketshareRepuestos]?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ok != QMessageBox.StandardButton.Yes:
            return

        # construir lista de cabeceras del table
        headers = []
        for i in range(tbl.columnCount()):
            hi = tbl.horizontalHeaderItem(i)
            headers.append(hi.text() if hi is not None else '')

        # helper de normalización para emparejar nombres
        def _norm(s: str) -> str:
            return ''.join(ch for ch in (s or '').upper() if ch.isalnum())

        header_map = { _norm(h): idx for idx, h in enumerate(headers) }

        # columnas objetivo en la BD (sin Id)
        db_cols = [
            'NRO_RUC','NOMBRE DEL IMPORTADOR','ADUANA','PAIS DE ORIGEN','PAIS DE PROCEDENCIA',
            'FOB_UNITARIO','FOB_TOTAL','CANTIDAD','UNIDAD_MED','ARTICULO','DESCRIPCION','MARCA','MODELO',
            'ESTADO','NRO_POLIZA','FECHA','SAC','PROVEEDOR','MES','MES2','AÑO','SISTEMA'
        ]

        # normalizar nombres de db_cols para búsqueda
        db_norm = [ _norm(c) for c in db_cols ]

        # Tipos especiales
        float_cols = { 'FOB_UNITARIO','FOB_TOTAL','CANTIDAD','SAC','MES','AÑO' }
        date_cols = { 'FECHA' }

        # conectar
        try:
            conn = connect_db('compras_internacionales')
            cursor = conn.cursor()
        except Exception as e:
            QMessageBox.critical(self, "Error DB", f"No se pudo conectar a la base de datos: {e}")
            return

        # preparar sentencia INSERT con placeholders
        cols_bracket = [f'[{c}]' for c in db_cols]
        placeholders = ','.join(['?'] * len(cols_bracket))
        insert_sql = f"INSERT INTO [ComprasInternacionales].[CI].[MarketshareRepuestos] ({', '.join(cols_bracket)}) VALUES ({placeholders})"

        inserted = 0
        failed = 0
        try:
            for r in range(tbl.rowCount()):
                try:
                    # si la fila está oculta por filtro, saltarla
                    if tbl.isRowHidden(r):
                        continue
                    values = []
                    for i_col, colname in enumerate(db_cols):
                        norm_target = _norm(colname)
                        val = None
                        if norm_target in header_map:
                            idx = header_map[norm_target]
                            it = tbl.item(r, idx)
                            raw = it.text().strip() if (it is not None and it.text() is not None) else ''
                            if raw == '':
                                val = None
                            else:
                                if colname in float_cols:
                                    try:
                                        val = float(raw.replace(',',''))
                                    except Exception:
                                        val = None
                                elif colname in date_cols:
                                    try:
                                        import pandas as pd
                                        dt = pd.to_datetime(raw, errors='coerce', dayfirst=True)
                                        if dt is not None and not pd.isna(dt):
                                            val = dt.to_pydatetime()
                                        else:
                                            val = None
                                    except Exception:
                                        val = None
                                else:
                                    val = raw
                        else:
                            val = None
                        values.append(val)

                    cursor.execute(insert_sql, values)
                    inserted += 1
                except Exception:
                    failed += 1
                    continue
            try:
                conn.commit()
            except Exception:
                pass
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

        QMessageBox.information(self, "Resultado", f"Filas insertadas: {inserted}\nFilas fallidas: {failed}")
        return

    def _copy_table_selection(self, table: QTableWidget) -> None:
        try:
            sel = table.selectedRanges()
            if sel:
                r = sel[0]
                rows = []
                for i in range(r.topRow(), r.bottomRow() + 1):
                    cols = []
                    for j in range(r.leftColumn(), r.rightColumn() + 1):
                        it = table.item(i, j)
                        cols.append('' if it is None else it.text())
                    rows.append('\t'.join(cols))
                text = '\n'.join(rows)
            else:
                # si no hay rango, usar selección de items
                items = table.selectedItems()
                if not items:
                    return
                # agrupar por filas
                byrow = {}
                for it in items:
                    r = it.row()
                    c = it.column()
                    byrow.setdefault(r, {})[c] = it.text()
                rows = []
                minc = min(min(d.keys()) for d in byrow.values())
                maxc = max(max(d.keys()) for d in byrow.values())
                for r in sorted(byrow.keys()):
                    cols = [byrow[r].get(c, '') for c in range(minc, maxc + 1)]
                    rows.append('\t'.join(cols))
                text = '\n'.join(rows)
            cb = QGuiApplication.clipboard()
            if cb is None:
                return
            try:
                cb.setText(text)
            except Exception:
                pass
        except Exception:
            pass

    def _paste_into_table(self, table: QTableWidget) -> None:
        try:
            cb = QGuiApplication.clipboard()
            if cb is None:
                return
            try:
                txt = cb.text()
            except Exception:
                txt = ''
            if not txt:
                return
            rows = [r.split('\t') for r in txt.splitlines()]
            n_rows = len(rows)
            n_cols = max(len(r) for r in rows) if n_rows > 0 else 0

            ranges = table.selectedRanges()
            if ranges and len(ranges) > 0:
                # trabajamos con el primer rango seleccionado
                rng = ranges[0]
                sel_r = rng.rowCount()
                sel_c = rng.columnCount()
                top = rng.topRow()
                left = rng.leftColumn()

                # caso: clipboard single value -> rellenar todo el rango
                if n_rows == 1 and n_cols == 1:
                    val = rows[0][0]
                    for rr in range(top, top + sel_r):
                        for cc in range(left, left + sel_c):
                            try:
                                while rr >= table.rowCount():
                                    table.insertRow(table.rowCount())
                                it = table.item(rr, cc)
                                if it is None:
                                    it = QTableWidgetItem('')
                                    table.setItem(rr, cc, it)
                                it.setText(val)
                            except Exception:
                                continue
                    return

                # caso: clipboard encaja exactamente en el rango -> pegar en bloque
                if n_rows == sel_r and n_cols == sel_c:
                    for r_off in range(n_rows):
                        for c_off in range(n_cols):
                            try:
                                rr = top + r_off
                                cc = left + c_off
                                while rr >= table.rowCount():
                                    table.insertRow(table.rowCount())
                                v = rows[r_off][c_off] if c_off < len(rows[r_off]) else ''
                                it = table.item(rr, cc)
                                if it is None:
                                    it = QTableWidgetItem('')
                                    table.setItem(rr, cc, it)
                                it.setText(v)
                            except Exception:
                                continue
                    return

                # caso: range es una única columna y clipboard es una columna -> pegar columna-wise
                if sel_c == 1 and n_cols == 1:
                    for r_off in range(min(sel_r, n_rows)):
                        try:
                            rr = top + r_off
                            cc = left
                            while rr >= table.rowCount():
                                table.insertRow(table.rowCount())
                            v = rows[r_off][0]
                            it = table.item(rr, cc)
                            if it is None:
                                it = QTableWidgetItem('')
                                table.setItem(rr, cc, it)
                            it.setText(v)
                        except Exception:
                            continue
                    return

                # fallback: si no encaja, pegar la primera celda en todo el rango
                try:
                    fallback = rows[0][0] if n_rows > 0 and n_cols > 0 else ''
                except Exception:
                    fallback = ''
                for rr in range(top, top + sel_r):
                    for cc in range(left, left + sel_c):
                        try:
                            while rr >= table.rowCount():
                                table.insertRow(table.rowCount())
                            it = table.item(rr, cc)
                            if it is None:
                                it = QTableWidgetItem('')
                                table.setItem(rr, cc, it)
                            it.setText(fallback)
                        except Exception:
                            continue
                return

            # si no hay ranges, usar selectedItems
            items = table.selectedItems()
            if items:
                # clipboard single value -> pegar en todas las celdas seleccionadas
                if n_rows == 1 and n_cols == 1:
                    val = rows[0][0]
                    for it in items:
                        try:
                            it.setText(val)
                        except Exception:
                            try:
                                table.setItem(it.row(), it.column(), QTableWidgetItem(val))
                            except Exception:
                                pass
                    return

                # if selection is single column and clipboard is columnar, paste sequentially
                cols = {it.column() for it in items}
                rows_sel = sorted({it.row() for it in items})
                if len(cols) == 1 and n_cols == 1:
                    for i, rr in enumerate(rows_sel):
                        try:
                            v = rows[i][0] if i < n_rows else ''
                            it = table.item(rr, list(cols)[0])
                            if it is None:
                                it = QTableWidgetItem('')
                                table.setItem(rr, list(cols)[0], it)
                            it.setText(v)
                        except Exception:
                            continue
                    return

                # fallback: paste first value into last selected cell
                try:
                    last = items[-1]
                    v = rows[0][0] if n_rows > 0 and n_cols > 0 else ''
                    last.setText(v)
                except Exception:
                    pass
                return
        except Exception:
            pass

    def eventFilter(self, obj, event) -> bool:
        try:
            if event is None:
                return False
            if event.type() == QEvent.Type.KeyPress:
                key = event.key()
                mods = event.modifiers()
                try:
                    ctrl = Qt.KeyboardModifier.ControlModifier
                except Exception:
                    ctrl = Qt.KeyboardModifier.ControlModifier
                if mods & ctrl:
                    try:
                        if key == Qt.Key.Key_C:
                            if isinstance(obj, QTableWidget):
                                self._copy_table_selection(obj)
                                return True
                        if key == Qt.Key.Key_V:
                            if isinstance(obj, QTableWidget):
                                self._paste_into_table(obj)
                                return True
                    except Exception:
                        pass
            return super().eventFilter(obj, event)
        except Exception:
            return False
        

