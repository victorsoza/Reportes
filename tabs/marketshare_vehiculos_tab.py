from typing import cast, Iterable, Any
import logging
from typing import Optional
from collections import Counter

import pandas as pd
import unicodedata
import re

from db_config import connect_db

from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel, QTableWidget, QHeaderView, QTableWidgetItem, QPushButton, QHBoxLayout, QFileDialog, QMessageBox, QMenu, QInputDialog, QLineEdit, QListWidget, QListWidgetItem, QWidgetAction, QDialog
from PyQt6.QtGui import QFont, QIcon, QAction, QColor, QBrush, QMovie
from PyQt6.QtCore import QSize, QEvent
from PyQt6 import QtCore
from PyQt6.QtGui import QGuiApplication
from PyQt6.QtWidgets import QApplication
import os


class MarketshareVehiculosTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.logger = logging.getLogger("ReportesApp")
        self.logger.info("Inicializando MarketshareVehiculosTab")
        layout = QVBoxLayout(self)
        title = QLabel("Marketshare - Vehículos")
        title.setFont(QFont("Arial", 12))
        layout.addWidget(title)

        # Botones para acciones: cargar múltiples archivos y limpiar tabla
        btn_layout = QHBoxLayout()
        self.load_files_button = QPushButton("Cargar Excel(s)")
        self.load_files_button.clicked.connect(lambda: self.load_excel_files())
        btn_layout.addWidget(self.load_files_button)

        self.detect_models_button = QPushButton("Detectar Modelos")
        self.detect_models_button.clicked.connect(lambda: self.identify_models())
        btn_layout.addWidget(self.detect_models_button)

        self.save_models_button = QPushButton("Guardar Modelos")
        self.save_models_button.clicked.connect(lambda: self.save_models_to_db())
        btn_layout.addWidget(self.save_models_button)

        self.clear_button = QPushButton("Limpiar tabla")
        self.clear_button.clicked.connect(lambda: self.clear_table())
        btn_layout.addWidget(self.clear_button)

        layout.addLayout(btn_layout)

        self.table = QTableWidget()
        # Definir columnas requeridas
        columns = [
            "NRO_RUC",
            "NOMBRE DEL IMPORTADOR",
            "DESCRIPCION",
            "ADUANA",
            "PAIS DE ORIGEN",
            "PESO_BRUTO",
            "VALOR_CIF",
            "CANTIDAD",
            "UNIDAD DE MEDIDA",
            "UNIDAD DE MEDIDA2",
            "MARCA",
            "MODELO",
            "CATEGORIA",
            "POLIZA",
            "FECHA",
            "MES",
            "AÑO",
            "SAC",
            "CONSIGNATARIO",
            "EXPORTADOR",
            "ESTADO",
        ]
        self.table.setColumnCount(len(columns))
        self.table.setHorizontalHeaderLabels(columns)
        self.logger.debug("Columnas definidas para Marketshare Vehículos: %s", columns)
        self.table.setRowCount(0)
        header = cast(QHeaderView, self.table.horizontalHeader())
        header.setStretchLastSection(True)
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setDefaultSectionSize(120)
        self.table.setAlternatingRowColors(True)
        # Botones de filtro integrados en el encabezado (icono solo). Se posicionan sobre el header.
        self.filter_buttons: list[QPushButton] = []
        self.filter_texts: list[str] = [""] * len(columns)
        icon_active = "Iconos/FiltroActivo.png"
        icon_inactive = "Iconos/FiltroInactivo.png"

        for i, col_name in enumerate(columns):
            btn = QPushButton(header)
            btn.setObjectName(f"filter_btn_{i}")
            btn.setFlat(True)
            btn.setIcon(QIcon(icon_inactive))
            btn.setIconSize(QSize(16, 16))
            btn.setFixedSize(20, 20)
            btn.clicked.connect(lambda _checked, col=i: self.show_filter_menu(col))
            btn.show()
            btn.raise_()
            self.filter_buttons.append(btn)

        # Reposicionar botones cuando se cambie tamaño o se muevan secciones
        header.sectionResized.connect(lambda logicalIndex, oldSize, newSize: self.position_filter_buttons())
        header.sectionMoved.connect(lambda logicalIndex, oldVisualIndex, newVisualIndex: self.position_filter_buttons())
        try:
            sb = self.table.horizontalScrollBar()
            if sb is not None:
                sb.valueChanged.connect(lambda v: self.position_filter_buttons())
        except Exception:
            pass
        vp = self.table.viewport()
        if vp is not None:
            vp.installEventFilter(self)
        # Context menu for paste into MODELO
        try:
            self.table.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
            self.table.customContextMenuRequested.connect(self._on_table_context_menu)
        except Exception:
            pass
        self.position_filter_buttons()
        layout.addWidget(self.table)
        # filas completadas por agrupación (índices)
        self.filled_rows: set[int] = set()
        # cache para el icono punto y set previo de filas con icono aplicado
        self._dot_icon: Optional[QIcon] = None
        self._last_filled_rows: set[int] = set()
        # loading dialog container
        self._loading_dialog = None

    def add_row(self, values: Iterable[Optional[object]]):
        """Añade una fila a la tabla y escribe un log de la acción.

        `values` debe ser un iterable con la misma longitud que las columnas.
        """
        try:
            row = self.table.rowCount()
            self.table.insertRow(row)
            for col_idx, val in enumerate(values):
                # Cast to Any to satisfy type-checkers when using pandas.isna
                if pd.isna(cast(Any, val)):
                    text = ""
                else:
                    # Trim whitespace for string values coming from Excel
                    if isinstance(val, str):
                        text = val.strip()
                    else:
                        text = str(val)
                item = QTableWidgetItem(text)
                self.table.setItem(row, col_idx, item)
                # Ajuste incremental de ancho de columna según el contenido añadido
                try:
                    fm = self.table.fontMetrics()
                    padding = 24
                    w = fm.horizontalAdvance(text) + padding
                    if w > self.table.columnWidth(col_idx):
                        self.table.setColumnWidth(col_idx, w)
                        # reposicionar botones tras cambio de ancho
                        self.position_filter_buttons()
                except Exception:
                    pass
            # actualizar marcadores visuales de filas completadas (solo cambia si nueva fila fue rellenada)
            try:
                self.update_row_markers()
            except Exception:
                pass
        except Exception as e:
            self.logger.exception("Error añadiendo fila a MarketshareVehiculosTab: %s", e)

    def clear_table(self):
        try:
            self.table.setRowCount(0)
            self.logger.info("Tabla MarketshareVehiculosTab limpiada")
            try:
                self.filled_rows.clear()
            except Exception:
                pass
        except Exception as e:
            self.logger.exception("Error limpiando tabla MarketshareVehiculosTab: %s", e)

    def _normalize(self, s: str) -> str:
        if s is None:
            return ""
        if not isinstance(s, str):
            s = str(s)
        s = s.strip().lower()
        s = unicodedata.normalize('NFKD', s)
        s = ''.join(ch for ch in s if not unicodedata.combining(ch))
        return s

    def load_models_from_db(self, db_key: str = 'compras_internacionales', query: Optional[str] = None) -> list:
        """Carga una lista de modelos desde la base de datos. Devuelve lista de strings.

        `query` puede ser personalizado; por defecto intenta algunas tablas comunes.
        """
        self.logger.info("Iniciando carga de modelos desde BD (db_key=%s)", db_key)
        try:
            conn = connect_db(db_key)
            cursor = conn.cursor()
            tried = []
            default_queries = [
                query,
                "SELECT [MODELO] FROM [ComprasInternacionales].[kdx].[MarcaModeloMarketshare]",
                "SELECT Modelo FROM ModelosVehiculos",
                "SELECT Nombre FROM Modelos",
                "SELECT model_name FROM vehicle_models",
            ]
            models = []
            for q in default_queries:
                if not q:
                    continue
                q = q.strip()
                if q in tried:
                    continue
                tried.append(q)
                try:
                    self.logger.debug("Ejecutando query de modelos: %s", q)
                    cursor.execute(q)
                    rows = cursor.fetchall()
                    if not rows:
                        continue
                    # first column assumed to be the model name
                    models = [str(r[0]).strip() for r in rows if r and r[0] is not None]
                    if models:
                        break
                except Exception as e:
                    self.logger.debug("Query fallida (%s): %s", q, e)
                    continue
            try:
                cursor.close()
                conn.close()
            except Exception:
                pass
            self.logger.info("Modelos cargados desde BD: %s modelos encontrados (db=%s)", len(models), db_key)
            return models
        except Exception as e:
            self.logger.exception("Error conectando a BD para cargar modelos: %s", e)
            return []

    def save_models_to_db(self, db_key: str = 'compras_internacionales') -> None:
        """Guarda en la BD los pares únicos (MARCA, MODELO) presentes en la tabla pero ausentes en la tabla de destino.

        Inserta en [ComprasInternacionales].[kdx].[MarcaModeloMarketshare] columnas [MARCA],[MODELO].
        """
        # guard: si se llama con booleano por señal
        if isinstance(db_key, bool):
            db_key = 'compras_internacionales'
        try:
            # localizar índices de MARCA y MODELO
            col_names = []
            for i in range(self.table.columnCount()):
                hi = self.table.horizontalHeaderItem(i)
                col_names.append(hi.text() if hi is not None else "")
            try:
                idx_marca = col_names.index('MARCA')
            except ValueError:
                idx_marca = None
            try:
                idx_modelo = col_names.index('MODELO')
            except ValueError:
                idx_modelo = None

            if idx_modelo is None:
                QMessageBox.critical(self, "Error", "Columna 'MODELO' no encontrada.")
                return

            # reunir pares únicos desde la tabla
            pairs: set[tuple[str, str]] = set()
            for r in range(self.table.rowCount()):
                try:
                    m_item = self.table.item(r, idx_modelo) if idx_modelo is not None else None
                    modelo = m_item.text().strip() if (m_item is not None and m_item.text() is not None) else ""
                    if not modelo:
                        continue
                    marca = ""
                    if idx_marca is not None:
                        mar_item = self.table.item(r, idx_marca)
                        marca = mar_item.text().strip() if (mar_item is not None and mar_item.text() is not None) else ""
                    pairs.add((marca, modelo))
                except Exception:
                    continue

            if not pairs:
                QMessageBox.information(self, "Guardar Modelos", "No hay modelos para guardar.")
                return

            conn = connect_db(db_key)
            cursor = conn.cursor()
            existing: set[tuple[str, str]] = set()
            try:
                cursor.execute("SELECT [MARCA], [MODELO] FROM [ComprasInternacionales].[kdx].[MarcaModeloMarketshare]")
                rows = cursor.fetchall()
                for r in rows:
                    try:
                        marca_db = str(r[0]).strip() if r and r[0] is not None else ""
                        modelo_db = str(r[1]).strip() if r and r[1] is not None else ""
                        existing.add((marca_db, modelo_db))
                    except Exception:
                        continue
            except Exception as e:
                self.logger.debug("No se pudieron leer modelos existentes: %s", e)

            to_insert = [p for p in pairs if p not in existing]
            inserted = 0
            inserted_pairs: list[tuple[str, str]] = []
            for marca, modelo in to_insert:
                try:
                    # usar parámetros para evitar inyección
                    cursor.execute("INSERT INTO [ComprasInternacionales].[kdx].[MarcaModeloMarketshare] ([MARCA], [MODELO]) VALUES (?, ?)", marca, modelo)
                    inserted += 1
                    inserted_pairs.append((marca, modelo))
                except Exception as e:
                    self.logger.exception("Error insertando modelo (%s, %s): %s", marca, modelo, e)
                    continue
            try:
                conn.commit()
            except Exception:
                pass
            try:
                cursor.close()
                conn.close()
            except Exception:
                pass

            # preparar mensaje con detalles (limitar si son muchos)
            if inserted == 0:
                QMessageBox.information(self, "Guardar Modelos", f"No se insertaron modelos nuevos (candidatos: {len(to_insert)}).")
            else:
                # mostrar hasta 50 ejemplos en el diálogo
                max_show = 50
                if inserted <= max_show:
                    details = '\n'.join([f"{m} | {mo}" for m, mo in inserted_pairs])
                    QMessageBox.information(self, "Guardar Modelos", f"Insertados {inserted} nuevos modelos (candidatos: {len(to_insert)}):\n\n{details}")
                else:
                    details = '\n'.join([f"{m} | {mo}" for m, mo in inserted_pairs[:max_show]])
                    QMessageBox.information(self, "Guardar Modelos", f"Insertados {inserted} nuevos modelos (candidatos: {len(to_insert)}). Mostrando primeros {max_show}:\n\n{details}")
            # log full list for audit
            try:
                if inserted_pairs:
                    self.logger.info("Guardados %s modelos nuevos en BD (candidatos=%s): %s", inserted, len(to_insert), inserted_pairs)
                else:
                    self.logger.info("Guardados %s modelos nuevos en BD (candidatos=%s)", inserted, len(to_insert))
            except Exception:
                pass
        except Exception as e:
            self.logger.exception("Error guardando modelos en BD: %s", e)
            QMessageBox.critical(self, "Error", "Ocurrió un error guardando modelos. Revisa el log.")

    def show_filter_menu(self, col: int):
        """Muestra un menú enriquecido con búsqueda y valores únicos para la columna indicada."""
        try:
            if col < 0 or col >= len(self.filter_buttons):
                return
            # Collect unique values from the column
            values = set()
            for r in range(self.table.rowCount()):
                it = self.table.item(r, col)
                if it is not None:
                    v = it.text()
                    display = "Vacio" if v == "" else v
                    values.add(display)
            values_list = sorted(values, key=lambda x: (x is None, x))

            menu = QMenu(self)
            # Style the menu for better visual clarity
            menu.setStyleSheet("""
                QMenu { background-color: #ffffff; border: 1px solid #cfcfcf; }
                QMenu::item { padding: 6px 24px; }
                QMenu::item:selected { background-color: #e0f7fa; }
            """)
            clear_action = menu.addAction("<Borrar filtro>")
            sort_asc_action = menu.addAction("Ordenar ascendente")
            sort_desc_action = menu.addAction("Ordenar descendente")
            menu.addSeparator()

            # Create searchable list inside menu
            container = QWidget()
            container_layout = QVBoxLayout(container)
            container_layout.setContentsMargins(4, 4, 4, 4)
            search = QLineEdit(container)
            search.setPlaceholderText("Buscar...")
            # Style the search box
            search.setStyleSheet("QLineEdit { padding: 4px; border: 1px solid #bbb; border-radius: 4px; }")
            list_widget = QListWidget(container)
            list_widget.setAlternatingRowColors(True)
            # Style list widget (selection color and background)
            list_widget.setStyleSheet("""
                QListWidget { background: #ffffff; }
                QListWidget::item { padding: 4px; }
                QListWidget::item:selected { background: #ffd54f; color: #000000; }
            """)
            for idx, v in enumerate(values_list):
                it = QListWidgetItem(str(v))
                # subtle alternating tint for readability
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

            # Action to apply search text as filter
            def _select_matches() -> None:
                st = search.text().strip()
                if st == "":
                    self.filter_texts[col] = ""
                else:
                    self.filter_texts[col] = st
                self.apply_all_filters()
                menu.close()

            select_action = menu.addAction("Todo")
            if select_action is not None:
                select_action.triggered.connect(_select_matches)

            # Connect sorting
            try:
                if sort_asc_action is not None:
                    sort_asc_action.triggered.connect(lambda _, i=col: self.table.sortItems(i, QtCore.Qt.SortOrder.AscendingOrder))
                if sort_desc_action is not None:
                    sort_desc_action.triggered.connect(lambda _, i=col: self.table.sortItems(i, QtCore.Qt.SortOrder.DescendingOrder))
            except Exception:
                pass

            # Filter list based on search
            def _filter_list(text: str) -> None:
                needle = text.lower()
                for i in range(list_widget.count()):
                    item = list_widget.item(i)
                    if item is None:
                        continue
                    item.setHidden(needle not in item.text().lower())

            search.textChanged.connect(_filter_list)

            selected_value: dict[str, Optional[str]] = {'v': None}
            def _on_item_clicked(item: QListWidgetItem) -> None:
                selected_value['v'] = item.text()
                menu.close()

            list_widget.itemClicked.connect(_on_item_clicked)

            # Position menu near header section (account for scrolling)
            pos = self.table.mapToGlobal(QtCore.QPoint(0, self.table.height()))
            header = self.table.horizontalHeader()
            if header is not None:
                try:
                    try:
                        left = header.sectionViewportPosition(col)
                    except Exception:
                        left = header.sectionPosition(col)
                    section_w = header.sectionSize(col)
                    global_left = header.mapToGlobal(QtCore.QPoint(left, 0)).x()
                    global_top = header.mapToGlobal(QtCore.QPoint(0, header.height())).y()
                    menu_w = menu.sizeHint().width()
                    screen = QGuiApplication.screenAt(QtCore.QPoint(global_left, global_top))
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
                    pos = QtCore.QPoint(int(menu_x), int(global_top))
                except Exception:
                    pos = header.mapToGlobal(QtCore.QPoint(0, header.height()))

            selected_action = menu.exec(pos)
            if selected_action == clear_action:
                self.filter_texts[col] = ""
                self.apply_all_filters()
                return
            sel_text = selected_value.get('v')
            if sel_text is None:
                return
            self.filter_texts[col] = sel_text
            self.apply_all_filters()
        except Exception as e:
            self.logger.exception("Error mostrando menú de filtro: %s", e)

    def set_filter_for_column(self, col: int):
        try:
            current = self.filter_texts[col] if col < len(self.filter_texts) else ""
            text, ok = QInputDialog.getText(self, "Filtro", f"Valor filtro para columna {col}:", text=current)
            if ok:
                self.filter_texts[col] = text.strip()
                self.apply_all_filters()
        except Exception as e:
            self.logger.exception("Error estableciendo filtro: %s", e)

    def clear_filter_for_column(self, col: int):
        try:
            if col < len(self.filter_texts):
                self.filter_texts[col] = ""
                self.apply_all_filters()
        except Exception as e:
            self.logger.exception("Error limpiando filtro: %s", e)

    def position_filter_buttons(self):
        """Coloca los botones de filtro en la esquina derecha de cada sección del encabezado."""
        try:
            header = cast(QHeaderView, self.table.horizontalHeader())
            y = header.y()
            h = header.height()
            # obtener desplazamiento horizontal actual
            try:
                sb = self.table.horizontalScrollBar()
                scroll_val = sb.value() if sb is not None else 0
            except Exception:
                scroll_val = 0
            fm = header.fontMetrics()
            for i, btn in enumerate(self.filter_buttons):
                try:
                    # Preferir sectionViewportPosition si está disponible (pos relativa al viewport)
                    if hasattr(header, 'sectionViewportPosition'):
                        try:
                            x = header.sectionViewportPosition(i)
                        except Exception:
                            x = header.sectionPosition(i) - scroll_val
                    else:
                        x = header.sectionPosition(i) - scroll_val
                    w = header.sectionSize(i)
                    # obtener texto del encabezado
                    header_item = self.table.horizontalHeaderItem(i)
                    col_text = header_item.text() if header_item is not None else ""
                    text_width = fm.horizontalAdvance(col_text)
                    # calcular posición del texto (centrado o con margen mínimo)
                    min_margin = 6
                    text_left = x + max(min_margin, (w - text_width) // 2)
                    text_right = text_left + text_width
                    gap = 6
                    bx = text_right + gap
                    # asegurar que el botón no salga de la sección
                    max_bx = x + w - btn.width() - 4
                    if bx > max_bx:
                        bx = max_bx
                    by = y + (h - btn.height()) // 2
                    btn.move(bx, by)
                except Exception:
                    pass
        except Exception as e:
            self.logger.exception("Error posicionando botones de filtro: %s", e)

    def eventFilter(self, obj, event):
        try:
            vp = self.table.viewport()
            if vp is not None and obj is vp and event.type() in (QEvent.Type.Resize, QEvent.Type.UpdateRequest):
                self.position_filter_buttons()
        except Exception:
            pass
        return super().eventFilter(obj, event)

    def apply_all_filters(self):
        """Aplica todos los filtros almacenados en `self.filter_texts` (conjunción entre columnas)."""
        try:
            icon_active = "Iconos/FiltroActivo.png"
            icon_inactive = "Iconos/FiltroInactivo.png"
            total_rows = self.table.rowCount()
            cols = self.table.columnCount()

            for r in range(total_rows):
                visible = True
                for c in range(cols):
                    ft = self.filter_texts[c] if c < len(self.filter_texts) else ""
                    if ft:
                        item = self.table.item(r, c)
                        cell_text = item.text() if (item is not None and item.text() is not None) else ""
                        ft_lower = ft.lower()
                        if ft_lower == "vacio":
                            # Show only rows where the cell is empty
                            if cell_text.strip() != "":
                                visible = False
                                break
                        else:
                            if ft_lower not in str(cell_text).lower():
                                visible = False
                                break
                self.table.setRowHidden(r, not visible)

            # Actualizar iconos de botones de filtro según si hay texto
            for i, btn in enumerate(self.filter_buttons):
                try:
                    if i < len(self.filter_texts) and self.filter_texts[i].strip():
                        btn.setIcon(QIcon(icon_active))
                    else:
                        btn.setIcon(QIcon(icon_inactive))
                except Exception:
                    pass
        except Exception as e:
            self.logger.exception("Error aplicando filtros: %s", e)

    def adjust_column_widths(self, padding: int = 24):
        """Ajusta el ancho de cada columna al máximo entre el texto del encabezado y los valores."""
        try:
            cols = self.table.columnCount()
            if cols == 0:
                return
            header = cast(QHeaderView, self.table.horizontalHeader())
            header_fm = header.fontMetrics()
            table_fm = self.table.fontMetrics()
            for c in range(cols):
                header_item = self.table.horizontalHeaderItem(c)
                header_text = header_item.text() if header_item is not None else ""
                # If there's an active filter for this column, include it in header width
                try:
                    ftext = self.filter_texts[c].strip() if c < len(self.filter_texts) else ""
                except Exception:
                    ftext = ""
                display_header = header_text
                if ftext:
                    display_header = f"{header_text} {ftext}"
                max_w = header_fm.horizontalAdvance(display_header)
                for r in range(self.table.rowCount()):
                    item = self.table.item(r, c)
                    cell_text = item.text() if (item is not None and item.text() is not None) else ""
                    w = table_fm.horizontalAdvance(cell_text)
                    if w > max_w:
                        max_w = w
                # Account for reserved filter icon area in the header if present on header object
                try:
                    filter_icon_w = getattr(header, 'filter_icon_width', None)
                    if filter_icon_w is None:
                        # fallback to a reasonable icon width (button width + margin)
                        filter_icon_w = 28
                except Exception:
                    filter_icon_w = 28
                total_w = max_w + padding + int(filter_icon_w)
                try:
                    self.table.setColumnWidth(c, total_w)
                except Exception:
                    pass
            # Reposicionar botones tras cambios
            try:
                self.position_filter_buttons()
            except Exception:
                pass
        except Exception as e:
            self.logger.exception("Error ajustando anchos de columnas: %s", e)

    def _on_table_context_menu(self, pos: QtCore.QPoint) -> None:
        """Context menu: allow pasting clipboard text into the MODELO column for selected rows."""
        try:
            menu = QMenu(self)
            paste_model_action = menu.addAction("Pegar en MODELO")
            paste_brand_action = menu.addAction("Pegar en MARCA")
            action = menu.exec(self.table.mapToGlobal(pos))
            if action is None:
                return
            cb = QApplication.clipboard()
            if cb is None:
                QMessageBox.information(self, "Portapapeles", "No se puede acceder al portapapeles.")
                return
            clipboard_text = cb.text()
            if clipboard_text is None or clipboard_text == "":
                QMessageBox.information(self, "Portapapeles vacío", "El portapapeles está vacío.")
                return
            # localizar índices de las columnas MODELO y MARCA
            model_idx = None
            brand_idx = None
            for i in range(self.table.columnCount()):
                hi = self.table.horizontalHeaderItem(i)
                if hi is None:
                    continue
                txt = hi.text().strip().upper()
                if txt == 'MODELO':
                    model_idx = i
                elif txt == 'MARCA':
                    brand_idx = i
            # decidir acción según la selección del menú
            if action == paste_model_action:
                target_idx = model_idx
                if target_idx is None:
                    QMessageBox.warning(self, "Columna no encontrada", "No se encontró la columna 'MODELO'.")
                    return
            elif action == paste_brand_action:
                target_idx = brand_idx
                if target_idx is None:
                    QMessageBox.warning(self, "Columna no encontrada", "No se encontró la columna 'MARCA'.")
                    return
            else:
                return

            sel_ranges = self.table.selectedRanges()
            if not sel_ranges:
                QMessageBox.information(self, "Sin selección", "No hay filas seleccionadas.")
                return
            count = 0
            for rng in sel_ranges:
                for r in range(rng.topRow(), rng.bottomRow() + 1):
                    if self.table.isRowHidden(r):
                        continue
                    try:
                        item = self.table.item(r, target_idx)
                        if item is None:
                            item = QTableWidgetItem(clipboard_text)
                            self.table.setItem(r, target_idx, item)
                        else:
                            item.setText(clipboard_text)
                        count += 1
                    except Exception:
                        continue
            # Ajustes visuales
            try:
                self.adjust_column_widths()
            except Exception:
                pass
            try:
                self.position_filter_buttons()
            except Exception:
                pass
            try:
                # actualizar marcadores por si se pegó un modelo manualmente
                self.update_row_markers()
            except Exception:
                pass
            # mensaje más genérico indicando la columna objetivo
            try:
                col_name = 'MODELO' if action == paste_model_action else 'MARCA'
            except Exception:
                col_name = 'columna'
            QMessageBox.information(self, "Pegado", f"Se pegaron {count} celdas en {col_name}.")
        except Exception as e:
            self.logger.exception("Error en menú contextual: %s", e)

    def identify_models(self, db_key: str = 'compras_internacionales', query: Optional[str] = None, models_list: Optional[list] = None):
        """Identifica y rellena la columna `MODELO` buscando coincidencias en
        `UNIDAD DE MEDIDA` y `UNIDAD DE MEDIDA2` usando una lista de modelos.
        Si `models_list` es None, intenta cargar desde la BD usando `db_key` y `query`.
        """
        # Guard: si se recibe un booleano (por ejemplo la señal clicked), usar el valor por defecto
        if isinstance(db_key, bool):
            self.logger.debug("identify_models recibió un booleano para db_key; usando valor por defecto 'compras_internacionales'")
            db_key = 'compras_internacionales'
        self.logger.info("Iniciando identificación de modelos (db_key=%s)", db_key)
        # Mostrar diálogo de carga (no bloqueante)
        try:
            self._show_loading("Detectando modelos...")
        except Exception:
            pass
        try:
            if models_list is None:
                models = self.load_models_from_db(db_key=db_key, query=query)
            else:
                models = models_list
            if not models:
                self.logger.warning("No se encontraron modelos para identificar")
                QMessageBox.warning(self, "Modelos", "No se encontraron modelos en la base de datos.")
                return

            # Normalizar modelos y ordenar por longitud descendente para priorizar coincidencias largas
            norm_models = [(m, self._normalize(m)) for m in models]
            norm_models.sort(key=lambda x: len(x[1]), reverse=True)

            # Encontrar índices de columnas relevantes
            col_names = []
            for i in range(self.table.columnCount()):
                header_item = self.table.horizontalHeaderItem(i)
                col_names.append(header_item.text() if header_item is not None else "")
            try:
                idx_unidad1 = col_names.index('UNIDAD DE MEDIDA')
            except ValueError:
                idx_unidad1 = None
            try:
                idx_unidad2 = col_names.index('UNIDAD DE MEDIDA2')
            except ValueError:
                idx_unidad2 = None
            try:
                idx_modelo = col_names.index('MODELO')
            except ValueError:
                idx_modelo = None

            if idx_modelo is None:
                self.logger.error("Columna 'MODELO' no encontrada en la tabla; abortando identificación")
                QMessageBox.critical(self, "Error", "Columna 'MODELO' no encontrada en la tabla.")
                return

            matched = 0
            total = self.table.rowCount()
            for r in range(total):
                unidad_vals = []
                if idx_unidad1 is not None:
                    item = self.table.item(r, idx_unidad1)
                    unidad_vals.append(item.text() if item is not None else "")
                if idx_unidad2 is not None:
                    item = self.table.item(r, idx_unidad2)
                    unidad_vals.append(item.text() if item is not None else "")

                combined = ' '.join([self._normalize(v) for v in unidad_vals if v])
                found_model = None
                for orig, norm in norm_models:
                    if not norm:
                        continue
                    # match whole words only to avoid partial matches (e.g. 'RIO' in 'SUPERIOR')
                    try:
                        if re.search(r"\b" + re.escape(norm) + r"\b", combined):
                            found_model = orig
                            break
                    except Exception:
                        # fallback to substring if regex fails for any reason
                        if norm in combined:
                            found_model = orig
                            break

                if found_model:
                    # escribir en la columna MODELO
                    try:
                        self.table.setItem(r, idx_modelo, QTableWidgetItem(str(found_model)))
                        matched += 1
                    except Exception as e:
                        self.logger.exception("Error escribiendo modelo en fila %s: %s", r, e)

            self.logger.info("Identificación de modelos completada: %s/%s filas con modelo asignado", matched, total)
            # Segunda fase: completar modelos vacíos usando agrupación por PESO_BRUTO + VALOR_CIF
            try:
                # localizar índices de PESO_BRUTO y VALOR_CIF
                try:
                    idx_peso = col_names.index('PESO_BRUTO')
                except ValueError:
                    idx_peso = None
                try:
                    idx_val = col_names.index('VALOR_CIF')
                except ValueError:
                    idx_val = None

                def _norm_val(x: Any) -> str:
                    if x is None:
                        return ""
                    # si es numérico, normalizar formato
                    try:
                        if isinstance(x, (int, float)):
                            # eliminar .0 para enteros
                            fx = float(x)
                            if fx.is_integer():
                                return str(int(fx))
                            return f"{fx:.6f}".rstrip('0').rstrip('.')
                    except Exception:
                        pass
                    s = str(x).strip()
                    return s

                # construir map de (peso, valor) -> Counter(modelo) para filas con modelo
                pv_to_models: dict[tuple[str, str], Counter] = {}
                for r in range(total):
                    try:
                        m_item = self.table.item(r, idx_modelo) if idx_modelo is not None else None
                        m_text = m_item.text().strip() if (m_item is not None and m_item.text() is not None) else ""
                        if not m_text:
                            continue
                        peso_item = self.table.item(r, idx_peso) if idx_peso is not None else None
                        peso = _norm_val(peso_item.text() if (peso_item is not None and peso_item.text() is not None) else None)
                        valor_item = self.table.item(r, idx_val) if idx_val is not None else None
                        valor = _norm_val(valor_item.text() if (valor_item is not None and valor_item.text() is not None) else None)
                        key = (peso, valor)
                        pv_to_models.setdefault(key, Counter())[m_text] += 1
                    except Exception:
                        continue

                # ahora asignar a filas con modelo vacío si existe coincidencia exacta en pv_to_models
                filled = 0
                for r in range(total):
                    try:
                        m_item = self.table.item(r, idx_modelo) if idx_modelo is not None else None
                        m_text = m_item.text().strip() if (m_item is not None and m_item.text() is not None) else ""
                        if m_text:
                            continue
                        peso_item = self.table.item(r, idx_peso) if idx_peso is not None else None
                        peso = _norm_val(peso_item.text() if (peso_item is not None and peso_item.text() is not None) else None)
                        valor_item = self.table.item(r, idx_val) if idx_val is not None else None
                        valor = _norm_val(valor_item.text() if (valor_item is not None and valor_item.text() is not None) else None)
                        key = (peso, valor)
                        cnt = pv_to_models.get(key)
                        if cnt:
                            # elegir el modelo más frecuente
                            model_choice, _ = cnt.most_common(1)[0]
                            try:
                                self.table.setItem(r, idx_modelo, QTableWidgetItem(str(model_choice)))
                                filled += 1
                                try:
                                    self.filled_rows.add(r)
                                except Exception:
                                    pass
                            except Exception:
                                pass
                    except Exception:
                        continue
                if filled:
                    self.logger.info("Completados %s modelos vacíos usando PESO_BRUTO+VALOR_CIF", filled)
                # marcar filas que fueron completadas
                try:
                    # filled_rows fue poblado en el bucle anterior
                    self.update_row_markers()
                except Exception:
                    pass

                # Tercera fase: inferir MARCA para filas con MODELO pero MARCA vacía
                try:
                    # localizar índice de MARCA si existe
                    try:
                        idx_marca = col_names.index('MARCA')
                    except ValueError:
                        idx_marca = None

                    # construir mapeo modelo_normalizado -> Counter(marca) desde la propia tabla
                    model_to_brands: dict[str, Counter] = {}
                    for r in range(total):
                        try:
                            m_item = self.table.item(r, idx_modelo) if idx_modelo is not None else None
                            marca_item = self.table.item(r, idx_marca) if idx_marca is not None else None
                            modelo_txt = m_item.text().strip() if (m_item is not None and m_item.text() is not None) else ""
                            marca_txt = marca_item.text().strip() if (marca_item is not None and marca_item.text() is not None) else ""
                            if not modelo_txt or not marca_txt:
                                continue
                            norm_mod = self._normalize(modelo_txt)
                            model_to_brands.setdefault(norm_mod, Counter())[marca_txt] += 1
                        except Exception:
                            continue

                    # complementar con datos de BD (si es posible) para enriquecer el mapeo
                    try:
                        conn2 = connect_db(db_key)
                        cur2 = conn2.cursor()
                        try:
                            cur2.execute("SELECT [MARCA], [MODELO] FROM [ComprasInternacionales].[kdx].[MarcaModeloMarketshare]")
                            rows = cur2.fetchall()
                            for r in rows:
                                try:
                                    marca_db = str(r[0]).strip() if r and r[0] is not None else ""
                                    modelo_db = str(r[1]).strip() if r and r[1] is not None else ""
                                    if not modelo_db or not marca_db:
                                        continue
                                    norm_mod_db = self._normalize(modelo_db)
                                    model_to_brands.setdefault(norm_mod_db, Counter())[marca_db] += 1
                                except Exception:
                                    continue
                        except Exception:
                            pass
                        try:
                            cur2.close()
                            conn2.close()
                        except Exception:
                            pass
                    except Exception:
                        # si falla la conexión, continuar con lo que haya en la tabla
                        pass

                    # ahora rellenar MARCA donde esté vacía y MODELO presente
                    marca_filled = 0
                    if idx_marca is not None:
                        for r in range(total):
                            try:
                                m_item = self.table.item(r, idx_modelo)
                                marca_item = self.table.item(r, idx_marca)
                                modelo_txt = m_item.text().strip() if (m_item is not None and m_item.text() is not None) else ""
                                marca_txt = marca_item.text().strip() if (marca_item is not None and marca_item.text() is not None) else ""
                                if not modelo_txt or marca_txt:
                                    continue
                                norm_mod = self._normalize(modelo_txt)
                                cnt = model_to_brands.get(norm_mod)
                                if cnt:
                                    brand_choice, _ = cnt.most_common(1)[0]
                                    try:
                                        if marca_item is None:
                                            self.table.setItem(r, idx_marca, QTableWidgetItem(brand_choice))
                                        else:
                                            marca_item.setText(brand_choice)
                                        marca_filled += 1
                                    except Exception:
                                        continue
                            except Exception:
                                continue
                    if marca_filled:
                        self.logger.info("Completadas %s marcas vacías basadas en MODELO", marca_filled)
                except Exception as e:
                    self.logger.exception("Error completando MARCA por MODELO: %s", e)
                # Cuarta fase: usar tabla DATOS para detectar pares MARCA/MODELO por palabra
                try:
                    token_map: dict[str, Counter] = {}
                    try:
                        conn3 = connect_db(db_key)
                        cur3 = conn3.cursor()
                        try:
                            cur3.execute("SELECT [MARCA], [MODELO], [DATOS] FROM [ComprasInternacionales].[kdx].[MarcaModeloDatosMarketshareVehículos]")
                            rows = cur3.fetchall()
                            for rr in rows:
                                try:
                                    marca_db = str(rr[0]).strip() if rr and rr[0] is not None else ""
                                    modelo_db = str(rr[1]).strip() if rr and rr[1] is not None else ""
                                    datos_db = str(rr[2]).strip() if rr and rr[2] is not None else ""
                                    if not datos_db:
                                        continue
                                    norm = self._normalize(datos_db)
                                    parts = re.findall(r"\w+", norm)
                                    for tok in parts:
                                        token_map.setdefault(tok, Counter())[(marca_db, modelo_db)] += 1
                                except Exception:
                                    continue
                        except Exception as e:
                            self.logger.debug("No se pudo leer tabla DATOS: %s", e)
                        try:
                            cur3.close()
                            conn3.close()
                        except Exception:
                            pass
                    except Exception:
                        # no hay BD o fallo; continuar
                        pass

                    # ahora intentar asignar usando tokens si hay mapa
                    assigned_from_datos = 0
                    if token_map:
                        for r in range(total):
                            try:
                                # asignar solo si MODELO vacío
                                m_item = self.table.item(r, idx_modelo) if idx_modelo is not None else None
                                m_text = m_item.text().strip() if (m_item is not None and m_item.text() is not None) else ""
                                if m_text:
                                    continue
                                # construir tokens desde UNIDAD DE MEDIDA y UNIDAD DE MEDIDA2
                                unit_texts = []
                                if idx_unidad1 is not None:
                                    it1 = self.table.item(r, idx_unidad1)
                                    unit_texts.append(it1.text() if it1 is not None and it1.text() is not None else "")
                                if idx_unidad2 is not None:
                                    it2 = self.table.item(r, idx_unidad2)
                                    unit_texts.append(it2.text() if it2 is not None and it2.text() is not None else "")
                                combined = ' '.join([self._normalize(u) for u in unit_texts if u])
                                if not combined:
                                    continue
                                toks = re.findall(r"\w+", combined)
                                cand = Counter()
                                for t in toks:
                                    if t in token_map:
                                        cand.update(token_map[t])
                                if not cand:
                                    continue
                                (brand_choice, model_choice), _ = cand.most_common(1)[0]
                                # escribir MODELO y MARCA
                                try:
                                    if idx_modelo is not None:
                                        self.table.setItem(r, idx_modelo, QTableWidgetItem(model_choice))
                                    if idx_marca is not None:
                                        mi = self.table.item(r, idx_marca)
                                        if mi is None:
                                            self.table.setItem(r, idx_marca, QTableWidgetItem(brand_choice))
                                        else:
                                            mi.setText(brand_choice)
                                    assigned_from_datos += 1
                                    try:
                                        self.filled_rows.add(r)
                                    except Exception:
                                        pass
                                except Exception:
                                    continue
                            except Exception:
                                continue
                    if assigned_from_datos:
                        self.logger.info("Asignados %s modelos/marcas desde tabla DATOS por coincidencia de palabra", assigned_from_datos)
                except Exception as e:
                    self.logger.exception("Error asignando desde tabla DATOS: %s", e)
            except Exception as e:
                self.logger.exception("Error completando modelos vacíos por agrupación: %s", e)

            QMessageBox.information(self, "Identificación completa", f"Modelos identificados: {matched} de {total} filas. Modelos completados por agrupación: {filled if 'filled' in locals() else 0}.")
        except Exception as e:
            self.logger.exception("Error en identify_models: %s", e)
        finally:
            try:
                self._hide_loading()
            except Exception:
                pass

    def _make_dot_icon(self, size: int = 10, color: str = '#80deea') -> QIcon:
        """Crea un QIcon con un punto circular del color dado."""
        try:
            from PyQt6.QtGui import QPixmap, QPainter, QBrush
            pix = QPixmap(size, size)
            pix.fill(QtCore.Qt.GlobalColor.transparent)
            p = QPainter(pix)
            try:
                p.setRenderHint(QPainter.RenderHint.Antialiasing)
                brush = QBrush(QColor(color))
                p.setBrush(brush)
                p.setPen(QtCore.Qt.PenStyle.NoPen)
                p.drawEllipse(0, 0, size, size)
            finally:
                p.end()
            return QIcon(pix)
        except Exception:
            return QIcon()

    def update_row_markers(self) -> None:
        """Aplica icono circular celeste en el encabezado vertical para filas en `self.filled_rows`."""
        try:
            # crear y cachear icono si no existe
            if self._dot_icon is None:
                self._dot_icon = self._make_dot_icon(size=10, color='#80deea')
            icon = self._dot_icon
            # actualizar solo filas cuyo estado cambió
            to_set = set(self.filled_rows) - set(self._last_filled_rows)
            to_clear = set(self._last_filled_rows) - set(self.filled_rows)

            for i in to_set:
                if i < 0 or i >= self.table.rowCount():
                    continue
                try:
                    vitem = self.table.verticalHeaderItem(i)
                    if vitem is None:
                        vitem = QTableWidgetItem(str(i + 1))
                        self.table.setVerticalHeaderItem(i, vitem)
                    vitem.setIcon(icon)
                except Exception:
                    continue

            for i in to_clear:
                if i < 0 or i >= self.table.rowCount():
                    continue
                try:
                    vitem = self.table.verticalHeaderItem(i)
                    if vitem is not None:
                        vitem.setIcon(QIcon())
                except Exception:
                    continue

            # actualizar registro de último estado
            self._last_filled_rows = set(self.filled_rows)
        except Exception as e:
            self.logger.exception("Error actualizando marcadores de fila: %s", e)

    def _show_loading(self, text: str = "Cargando...", gif_path: Optional[str] = None) -> None:
        """Muestra un diálogo centrado con `loading.gif` (si existe) y texto opcional."""
        try:
            # si ya existe, solo actualizar texto y mostrar
            if self._loading_dialog is not None:
                try:
                    self._loading_dialog.show()
                    return
                except Exception:
                    self._loading_dialog = None

            dlg = QDialog(self)
            dlg.setWindowFlags(QtCore.Qt.WindowType.FramelessWindowHint | QtCore.Qt.WindowType.Dialog)
            dlg.setModal(False)
            layout = QVBoxLayout(dlg)
            layout.setContentsMargins(12, 12, 12, 12)
            # determine gif path: prefer explicit gif_path, otherwise look for loading.gif next to this module
            if not gif_path:
                try:
                    base_dir = os.path.dirname(__file__)
                    candidate = os.path.join(base_dir, 'loading.gif')
                    if os.path.exists(candidate):
                        gif_path = candidate
                    else:
                        # fallback to parent 'Iconos' folder if present
                        candidate2 = os.path.join(os.path.dirname(base_dir), 'Iconos', 'loading.gif')
                        if os.path.exists(candidate2):
                            gif_path = candidate2
                except Exception:
                    gif_path = None

            # attempt to load gif
            label = QLabel(dlg)
            movie = None
            try:
                if gif_path and os.path.exists(gif_path):
                    movie = QMovie(gif_path)
                    label.setMovie(movie)
                    movie.start()
                else:
                    label.setText(text)
                    label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            except Exception:
                label.setText(text)
                label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(label)
            # size and center relative to parent
            dlg.adjustSize()
            parent_rect = self.geometry()
            global_pos = self.mapToGlobal(parent_rect.topLeft())
            x = global_pos.x() + (parent_rect.width() - dlg.width()) // 2
            y = global_pos.y() + (parent_rect.height() - dlg.height()) // 2
            dlg.move(int(x), int(y))
            dlg.show()
            # ensure the dialog is on top and process events so it renders immediately
            try:
                dlg.raise_()
                dlg.activateWindow()
            except Exception:
                pass
            try:
                QApplication.processEvents()
            except Exception:
                pass
            # keep reference to movie to avoid GC stopping animation
            try:
                self._loading_movie = movie
            except Exception:
                self._loading_movie = None
            self._loading_dialog = dlg
        except Exception as e:
            self.logger.exception("Error mostrando diálogo de carga: %s", e)

    def _hide_loading(self) -> None:
        try:
            if self._loading_dialog is not None:
                try:
                    self._loading_dialog.close()
                except Exception:
                    pass
                # stop movie if any
                try:
                    if hasattr(self, '_loading_movie') and self._loading_movie is not None:
                        try:
                            self._loading_movie.stop()
                        except Exception:
                            pass
                        try:
                            del self._loading_movie
                        except Exception:
                            pass
                except Exception:
                    pass
                self._loading_dialog = None
        except Exception:
            pass

    def load_from_dataframe(self, df: pd.DataFrame):
        """Carga datos desde un DataFrame en la tabla y registra la operación."""
        try:
            self.clear_table()
            if df is None or df.empty:
                self.logger.info("DataFrame vacío, no se cargó nada en MarketshareVehiculosTab")
                return
            # Normalizar y extraer MES y AÑO desde FECHA si está presente
            if 'FECHA' in df.columns:
                # Vectorized parsing: primero intentar parseo estándar, luego convertir valores numéricos (fechas Excel)
                s = pd.to_datetime(df['FECHA'], errors='coerce', dayfirst=True)
                mask_numeric = s.isna() & df['FECHA'].apply(lambda x: isinstance(x, (int, float)))
                if mask_numeric.any():
                    try:
                        s.loc[mask_numeric] = pd.to_datetime(df.loc[mask_numeric, 'FECHA'], unit='D', origin='1899-12-30', errors='coerce')
                    except Exception:
                        # ignore and keep NaT where parsing fails
                        pass
                df['FECHA'] = s
                # Extraer MES y AÑO; si no hay fecha válida, dejar cadena vacía
                df['MES'] = df['FECHA'].dt.month.fillna("")
                df['AÑO'] = df['FECHA'].dt.year.fillna("")
            expected_cols = []
            for i in range(self.table.columnCount()):
                header_item = self.table.horizontalHeaderItem(i)
                expected_cols.append(header_item.text() if header_item is not None else "")
            # Alinear columnas si vienen con nombres diferentes
            for _, row in df.iterrows():
                values = [row.get(col, "") if hasattr(row, 'get') else row[i] for i, col in enumerate(expected_cols)]
                self.add_row(values)
            # Ajustar anchos de columnas tras carga masiva
            try:
                self.adjust_column_widths()
            except Exception:
                pass
            try:
                # solo actualizar marcadores optimizados (no iterar todas las filas)
                self.update_row_markers()
            except Exception:
                pass
            self.logger.info("Cargadas %s filas en MarketshareVehiculosTab desde DataFrame", len(df))
        except Exception as e:
            self.logger.exception("Error cargando DataFrame en MarketshareVehiculosTab: %s", e)

    def load_excel_files(self):
        """Permite seleccionar múltiples archivos Excel y carga sus filas en la tabla.

        Cada archivo se procesa por separado; los registros se añaden (no se sobreescribe la tabla).
        """
        try:
            files, _ = QFileDialog.getOpenFileNames(self, "Seleccionar archivos Excel", "", "Archivos Excel (*.xlsx *.xls)")
            if not files:
                self.logger.debug("Diálogo de selección de archivos cancelado o sin selección")
                return

            total_loaded = 0
            for file_path in files:
                try:
                    df = pd.read_excel(file_path)
                    # Normalizar nombres de columnas eliminando espacios en los extremos
                    try:
                        df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c, inplace=True)
                    except Exception:
                        pass

                    # Extraer MES y AÑO desde FECHA si está presente (soporta fechas numéricas de Excel)
                    if 'FECHA' in df.columns:
                        s = pd.to_datetime(df['FECHA'], errors='coerce', dayfirst=True)
                        mask_numeric = s.isna() & df['FECHA'].apply(lambda x: isinstance(x, (int, float)))
                        if mask_numeric.any():
                            try:
                                s.loc[mask_numeric] = pd.to_datetime(df.loc[mask_numeric, 'FECHA'], unit='D', origin='1899-12-30', errors='coerce')
                            except Exception:
                                pass
                        df['FECHA'] = s
                        df['MES'] = df['FECHA'].dt.month.fillna("")
                        df['AÑO'] = df['FECHA'].dt.year.fillna("")
                    if df is None or df.empty:
                        self.logger.info("Archivo vacío o sin datos: %s", file_path)
                        continue

                    # Obtener nombres de columnas esperadas desde la tabla
                    expected_cols = []
                    for i in range(self.table.columnCount()):
                        header_item = self.table.horizontalHeaderItem(i)
                        expected_cols.append(header_item.text() if header_item is not None else "")
                    # Determinar estado por nombre de archivo
                    fname = file_path.split("\\")[-1].upper()
                    estado_value = ""
                    if "NUEVO" in fname or "NUEVOS" in fname:
                        estado_value = "NUEVOS"
                    elif "USADO" in fname or "USADOS" in fname:
                        estado_value = "USADO"
                    if estado_value:
                        self.logger.info("Archivo %s identificado como estado '%s'", file_path, estado_value)

                    file_count = 0
                    for _, row in df.iterrows():
                        # Obtener valor por columna esperada y recortar strings
                        values = []
                        for i, col in enumerate(expected_cols):
                            try:
                                val = row.get(col, "") if hasattr(row, 'get') else row[i]
                            except Exception:
                                val = ""
                            if isinstance(val, str):
                                val = val.strip()
                            # Si la columna es ESTADO y se determinó por archivo, sobrescribir
                            if col.upper() == 'ESTADO' and estado_value:
                                val = estado_value
                            values.append(val)
                        self.add_row(values)
                        file_count += 1

                    total_loaded += file_count
                except Exception as file_exc:
                    self.logger.exception("Error procesando archivo %s: %s", file_path, file_exc)
            # Ajustar anchos de columnas una vez terminada la carga de archivos
            try:
                self.adjust_column_widths()
            except Exception:
                pass
            try:
                # solo actualizar marcadores optimizados (no iterar todas las filas)
                self.update_row_markers()
            except Exception:
                pass

            QMessageBox.information(self, "Carga completa", f"Se cargaron {total_loaded} filas desde {len(files)} archivo(s).")
            self.logger.info("Carga múltiple completada: archivos=%s, filas_totales=%s", len(files), total_loaded)
        except Exception as e:
            self.logger.exception("Error en carga múltiple de Excel: %s", e)
