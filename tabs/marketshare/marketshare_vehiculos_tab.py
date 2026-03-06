from typing import cast, Iterable, Any, Callable
import logging
from typing import Optional
from collections import Counter

import pandas as pd
import unicodedata
import re

from db_config import connect_db

from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel, QTableWidget, QHeaderView, QTableWidgetItem, QPushButton, QHBoxLayout, QFileDialog, QMessageBox, QMenu, QInputDialog, QLineEdit, QListWidget, QListWidgetItem, QWidgetAction, QDialog
from PyQt6.QtGui import QFont, QIcon, QAction, QColor, QBrush
from PyQt6.QtCore import QSize, QEvent
from PyQt6 import QtCore
from PyQt6.QtGui import QGuiApplication
from PyQt6.QtWidgets import QApplication
import os
try:
    from ..shared.loading_dialog import LoadingDialog
except Exception:
    try:
        from tabs.shared.loading_dialog import LoadingDialog
    except Exception:
        LoadingDialog = None


# Importar la lógica modulada para el botón 'Guardar Detalles'
try:
    from .Logica_Vehiculos import save_details as lv_save_details
except Exception:
    try:
        from tabs.marketshare.Logica_Vehiculos import save_details as lv_save_details
    except Exception:
        lv_save_details = None


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
        self.load_files_button.clicked.connect(self.load_excel_files)
        btn_layout.addWidget(self.load_files_button)

        self.detect_models_button = QPushButton("Detectar Modelos")
        self.detect_models_button.clicked.connect(self._wrap_with_loading(self.identify_models, "Detectando modelos..."))
        btn_layout.addWidget(self.detect_models_button)

        self.save_models_button = QPushButton("Guardar Modelos")
        self.save_models_button.clicked.connect(self._wrap_with_loading(self.save_models_to_db, "Guardando modelos..."))
        btn_layout.addWidget(self.save_models_button)

        self.save_details_button = QPushButton("Guardar Detalles")
        self.save_details_button.clicked.connect(self._wrap_with_loading(self.save_details, "Guardando detalles..."))
        btn_layout.addWidget(self.save_details_button)

        self.save_sql_button = QPushButton("Guardar en SQL")
        self.save_sql_button.clicked.connect(self._wrap_with_loading(self.save_table_to_sql, "Guardando en SQL..."))
        btn_layout.addWidget(self.save_sql_button)

        self.clear_button = QPushButton("Limpiar tabla")
        self.clear_button.clicked.connect(self._wrap_with_loading(self.clear_table, "Limpiando tabla..."))
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
        # loading dialog reutilizable (instancia compartida)
        try:
            if LoadingDialog is not None:
                try:
                    from ..shared.loading_dialog import get_loading_dialog
                except Exception:
                    try:
                        from tabs.shared.loading_dialog import get_loading_dialog
                    except Exception:
                        get_loading_dialog = None
                if get_loading_dialog is not None:
                    self.loading = get_loading_dialog(self)
                else:
                    self.loading = LoadingDialog(self)
            else:
                self.loading = None
        except Exception:
            self.loading = None
        # referencias a worker/hilo de detección (inicializadas para que Pylance las conozca)
        self._detect_worker: Optional[object] = None
        self._detect_thread: Optional[object] = None

    def _wrap_with_loading(self, func: Callable[..., Any], text: Optional[str] = None) -> Callable[[], None]:
        """Devuelve un callable que muestra el diálogo de loading, ejecuta `func` y lo oculta.

        `func` puede ser un método que acepte cero o más argumentos; el wrapper asume ninguno
        (porque lo usaremos con señales `clicked`). Maneja excepciones y siempre oculta.
        """
        def _wrapped() -> None:
            try:
                try:
                    self._show_loading(text or "Procesando...")
                except Exception:
                    pass
                try:
                    func()
                except Exception as e:
                    try:
                        self.logger.exception("Error en acción de botón: %s", e)
                    except Exception:
                        pass
                    try:
                        QMessageBox.critical(self, "Error", f"Ocurrió un error: {e}")
                    except Exception:
                        pass
            finally:
                try:
                    self._hide_loading()
                except Exception:
                    pass

        return _wrapped

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

    def save_models_to_db(self, db_key: str = 'compras_internacionales') -> None:
        """Guarda en la BD los pares únicos (MARCA, MODELO) presentes en la tabla pero ausentes en la tabla de destino.

        Inserta en [ComprasInternacionales].[kdx].[MarcaModeloMarketshare] columnas [MARCA],[MODELO].
        """
        # permitir señales booleanas
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
                    # Omitir modelos marcados como N/A (y variantes como N.A. o N A)
                    try:
                        modelo_check = re.sub(r'[^A-Z0-9]', '', modelo.upper())
                        if modelo_check == 'NA':
                            continue
                    except Exception:
                        pass
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
                max_show = 50
                if inserted <= max_show:
                    details = '\n'.join([f"{m} | {mo}" for m, mo in inserted_pairs])
                    QMessageBox.information(self, "Guardar Modelos", f"Insertados {inserted} nuevos modelos (candidatos: {len(to_insert)}):\n\n{details}")
                else:
                    details = '\n'.join([f"{m} | {mo}" for m, mo in inserted_pairs[:max_show]])
                    QMessageBox.information(self, "Guardar Modelos", f"Insertados {inserted} nuevos modelos (candidatos: {len(to_insert)}). Mostrando primeros {max_show}:\n\n{details}")
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

    def save_details(self, db_key: str = 'compras_internacionales') -> None:
        """Alterna la visualización de un panel lateral derecho para 'Detalles'.

        El panel se crea la primera vez y luego se muestra/oculta. Esta
        implementación no persiste datos; solo muestra el panel lateral.
        """
        # permitir señales booleanas
        if isinstance(db_key, bool):
            db_key = 'compras_internacionales'
        # Delegar a la lógica modularizada (sin fallback local)
        try:
            self.logger.debug("save_details llamado; lv_save_details disponible=%s", lv_save_details is not None)
            if lv_save_details is None:
                QMessageBox.information(self, "Detalles", "La lógica de 'Guardar Detalles' no está disponible.")
                return
            try:
                lv_save_details.handle_save_details(self, db_key)
            except Exception as e:
                self.logger.exception("Error delegando a lv_save_details: %s", e)
                QMessageBox.critical(self, "Error", "Ocurrió un error mostrando el panel de detalles. Revisa el log.")
        except Exception as e:
            try:
                self.logger.exception("Error en save_details: %s", e)
            except Exception:
                pass

    def save_table_to_sql(self, db_key: str = 'compras_internacionales') -> None:
        """Guarda las filas de la tabla en la tabla SQL [ComprasInternacionales].[CI].[MarketshareVehiculo].

        Se intenta mapear las columnas de la UI a los nombres de columnas SQL listados.
        """
        # permitir señales booleanas
        if isinstance(db_key, bool):
            db_key = 'compras_internacionales'
        try:
            # columnas objetivo tal como fueron provistas por el usuario
            sql_cols = [
                "NRO_RUC",
                "NOMBRE DEL IMPORTADOR",
                "DESCRIPCIÓN",
                "ADUANA ",
                "PAIS DE ORIGEN",
                "PESO BRUTO",
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
                "Selecc",
                "ESTADO",
            ]

            # preparar mapeo columna UI -> índice
            header_norms = {}
            for i in range(self.table.columnCount()):
                hi = self.table.horizontalHeaderItem(i)
                if hi is None:
                    continue
                header_norms[i] = self._normalize(hi.text())

            import re as _re
            col_to_idx: dict[str, Optional[int]] = {}
            for col in sql_cols:
                target_norm = self._normalize(col)
                # clean: remove spaces, underscores and hyphens for loose matching
                tclean = _re.sub(r"[\s_\-]", "", target_norm)
                found = None
                for idx, hnorm in header_norms.items():
                    hclean = _re.sub(r"[\s_\-]", "", hnorm)
                    if hnorm == target_norm or hclean == tclean:
                        found = idx
                        break
                col_to_idx[col] = found

            # construir query con columnas entre corchetes
            cols_bracketed = ','.join([f"[{c}]" for c in sql_cols])
            placeholders = ','.join(['?'] * len(sql_cols))
            insert_sql = f"INSERT INTO [ComprasInternacionales].[CI].[MarketshareVehiculo] ({cols_bracketed}) VALUES ({placeholders})"

            conn = connect_db(db_key)
            cursor = conn.cursor()
            inserted = 0
            failed = 0
            errors: list[str] = []

            total_rows = self.table.rowCount()
            if total_rows == 0:
                QMessageBox.information(self, "Guardar en SQL", "No hay filas en la tabla para guardar.")
                try:
                    cursor.close()
                    conn.close()
                except Exception:
                    pass
                return

            def _parse_date(val):
                # intenta convertir varios formatos comunes a datetime, devuelve None si no es posible
                try:
                    if val is None:
                        return None
                    # si ya es datetime
                    import datetime as _dt
                    if isinstance(val, _dt.datetime) or isinstance(val, _dt.date):
                        # pyodbc acepta datetime.datetime; si es date, convertir a datetime
                        if isinstance(val, _dt.date) and not isinstance(val, _dt.datetime):
                            return _dt.datetime(val.year, val.month, val.day)
                        return val
                    # si es string con patrón ISO (YYYY-MM-DD...) forzar dayfirst=False para evitar warning
                    try:
                        if isinstance(val, str):
                            s = val.strip()
                            if re.match(r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}', s):
                                parsed = pd.to_datetime(s, errors='coerce', dayfirst=False)
                                if pd.isna(parsed):
                                    # intentar formatos explícitos comunes
                                    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d'):
                                        try:
                                            parsed = pd.to_datetime(s, format=fmt, errors='coerce')
                                            if not pd.isna(parsed):
                                                break
                                        except Exception:
                                            continue
                                if pd.isna(parsed):
                                    parsed = None
                                else:
                                    return parsed.to_pydatetime()

                        # usar pandas para parsing con dayfirst heurístico
                        parsed = pd.to_datetime(val, errors='coerce', dayfirst=True)
                        if pd.isna(parsed):
                            parsed = pd.to_datetime(val, errors='coerce', dayfirst=False)
                        if pd.isna(parsed):
                            # intento adicional para valores numéricos tipo Excel serial
                            if isinstance(val, (int, float)):
                                try:
                                    parsed = pd.to_datetime(val, unit='D', origin='1899-12-30', errors='coerce')
                                except Exception:
                                    parsed = pd.NaT
                        if pd.isna(parsed):
                            return None
                        return parsed.to_pydatetime()
                    except Exception:
                        return None
                except Exception:
                    return None

            def _parse_float(val):
                try:
                    if val is None:
                        return None
                    if isinstance(val, (int, float)):
                        return float(val)
                    s = str(val).strip()
                    if s == "":
                        return None
                    # eliminar símbolos comunes
                    for ch in ['$', 'USD', 'EUR', '€']:
                        s = s.replace(ch, '')
                    s = s.replace('\u00A0', '')  # no-break space
                    s = s.strip()
                    # decide separadores: si contiene ambos '.' y ','
                    if '.' in s and ',' in s:
                        # si la última coma está después del último punto, tratar la coma como decimal
                        if s.rfind(',') > s.rfind('.'):
                            s = s.replace('.', '')
                            s = s.replace(',', '.')
                        else:
                            s = s.replace(',', '')
                    else:
                        # solo coma -> coma decimal
                        if ',' in s:
                            s = s.replace(',', '.')
                    # quitar espacios remanentes
                    s = s.replace(' ', '')
                    return float(s)
                except Exception:
                    return None

            for r in range(total_rows):
                try:
                    values = []
                    for col in sql_cols:
                        idx = col_to_idx.get(col)
                        if idx is None:
                            values.append(None)
                            continue
                        try:
                            it = self.table.item(r, idx)
                            raw = it.text().strip() if (it is not None and it.text() is not None) else None
                        except Exception:
                            raw = None

                        # Normalizaciones por tipo de columna
                        if col == 'FECHA':
                            parsed_date = _parse_date(raw)
                            values.append(parsed_date)
                            continue
                        if col in ('MES', 'AÑO', 'PESO BRUTO', 'VALOR_CIF', 'CANTIDAD', 'POLIZA', 'SAC'):
                            # columnas numéricas en la tabla (float)
                            num = _parse_float(raw)
                            values.append(num)
                            continue
                        # default: insertar texto (None si vacío)
                        v = raw if (raw is not None and raw != "") else None
                        values.append(v)
                    try:
                        cursor.execute(insert_sql, tuple(values))
                        inserted += 1
                    except Exception as e:
                        failed += 1
                        errors.append(f"Fila {r+1}: {e}")
                        self.logger.exception("Error insertando fila %s: %s", r+1, e)
                        continue
                except Exception as e:
                    failed += 1
                    errors.append(f"Fila {r+1}: {e}")
                    self.logger.exception("Error procesando fila %s para inserción: %s", r+1, e)
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

            # Reporte al usuario
            if inserted == 0:
                QMessageBox.information(self, "Guardar en SQL", f"No se insertaron filas. Errores: {failed}.")
            else:
                msg = f"Insertadas {inserted} filas. Errores: {failed}."
                if failed > 0:
                    # si hay muchos errores, no mostrar todos en un diálogo simple
                    sample = '\n'.join(errors[:20])
                    msg = msg + "\nEjemplos de errores:\n" + sample
                QMessageBox.information(self, "Guardar en SQL", msg)
            self.logger.info("save_table_to_sql: insertadas=%s fallidas=%s (total filas=%s)", inserted, failed, total_rows)
        except Exception as e:
            self.logger.exception("Error guardando tabla en SQL: %s", e)
            QMessageBox.critical(self, "Error", "Ocurrió un error guardando la tabla en SQL. Revisa el log.")

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
                    # reservar área a la derecha para el icono de filtro
                    gap = 6
                    reserved_right = btn.width() + gap + 4
                    # Si el texto ocupa gran parte de la sección, ampliar la sección
                    try:
                        if text_width + reserved_right > w:
                            new_w = int(text_width + reserved_right + 8)
                            try:
                                # Preferir ajustar directamente el ancho de la columna en la tabla
                                self.table.setColumnWidth(i, new_w)
                                w = new_w
                            except Exception:
                                try:
                                    header.resizeSection(i, new_w)
                                    w = new_w
                                except Exception:
                                    # fallback silencioso
                                    pass
                    except Exception:
                        pass
                    # calcular posición del texto (centrado o con margen mínimo)
                    min_margin = 6
                    text_left = x + max(min_margin, (w - text_width) // 2)
                    text_right = text_left + text_width
                    # colocar el botón siempre en la zona derecha reservada
                    max_bx = x + w - btn.width() - 4
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
        """Inicia la detección (lanza Fase 1). El encadenamiento a Fase 2 lo maneja `detect_models` internamente."""
        # permitir señales booleanas
        if isinstance(db_key, bool):
            db_key = 'compras_internacionales'
        try:
            from .Logica_Vehiculos.model_detection import detect_models
        except Exception:
            try:
                from tabs.marketshare.Logica_Vehiculos.model_detection import detect_models
            except Exception:
                detect_models = None
        if detect_models is None:
            QMessageBox.critical(self, "Error", "No se pudo cargar el módulo de detección de modelos.")
            return
        return detect_models(self, db_key=db_key, query=query, models_list=models_list, phase=1)

    # removed interactive phase chooser; detection phases are chained in the detection module

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

    def _set_ui_locked(self, locked: bool) -> None:
        """Deshabilita/rehabilita controles interactivos durante operaciones en background.

        `locked=True` desactiva la tabla, botones principales y los botones de filtro.
        """
        try:
            # tabla
            try:
                self.table.setDisabled(locked)
            except Exception:
                pass
            # botones principales
            try:
                for n in ('load_files_button', 'detect_models_button', 'save_models_button', 'clear_button'):
                    btn = getattr(self, n, None)
                    if btn is not None:
                        btn.setDisabled(locked)
            except Exception:
                pass
            # botones de filtro (en header)
            try:
                for b in self.filter_buttons:
                    try:
                        b.setDisabled(locked)
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            pass

    # Handlers para la detección en background (QThread)
    def _on_detection_row_assigned(self, row: int, model: str) -> None:
        """Maneja la señal del worker indicando que una fila debe recibir MODELO."""
        try:
            # localizar índice MODELO cada vez por si cambió el esquema
            idx_modelo = None
            for i in range(self.table.columnCount()):
                hi = self.table.horizontalHeaderItem(i)
                if hi is not None and hi.text().strip().upper() == 'MODELO':
                    idx_modelo = i
                    break
            if idx_modelo is None:
                self.logger.error("_on_detection_row_assigned: columna MODELO no encontrada")
                return
            item = QTableWidgetItem(str(model))
            self.table.setItem(row, idx_modelo, item)
        except Exception:
            self.logger.exception("Error asignando modelo en fila %s", row)

    def _on_detection_row_assigned_vin(self, row: int, marca: str, modelo: str) -> None:
        """Maneja la señal del worker de VIN indicando que una fila debe recibir MARCA y MODELO."""
        try:
            # localizar índices MODELO y MARCA
            idx_modelo = None
            idx_marca = None
            for i in range(self.table.columnCount()):
                hi = self.table.horizontalHeaderItem(i)
                if hi is None:
                    continue
                txt = hi.text().strip().upper()
                if txt == 'MODELO':
                    idx_modelo = i
                elif txt == 'MARCA':
                    idx_marca = i
                if idx_modelo is not None and idx_marca is not None:
                    break
            if idx_modelo is None and idx_marca is None:
                self.logger.error("_on_detection_row_assigned_vin: columnas MARCA/MODELO no encontradas")
                return
            if idx_marca is not None:
                try:
                    self.table.setItem(row, idx_marca, QTableWidgetItem(str(marca)))
                except Exception:
                    pass
            if idx_modelo is not None:
                try:
                    self.table.setItem(row, idx_modelo, QTableWidgetItem(str(modelo)))
                except Exception:
                    pass
        except Exception as e:
            self.logger.exception("Error asignando MARCA/MODELO en fila %s: %s", row, e)

    def _on_detection_progress(self, value: int) -> None:
        # Intencionalmente silencioso: evitar logs de progreso por fila
        return

    def _on_detection_log(self, text: str) -> None:
        # Intencionalmente silencioso: evitar logs detallados por fila
        return

    def _on_detection_error(self, message: str) -> None:
        # Registrar y notificar al usuario
        try:
            self.logger.error("Error en DetectWorker: %s", message)
        except Exception:
            pass
        try:
            QMessageBox.critical(self, "Error en detección", str(message))
        except Exception:
            pass
        # Ocultar diálogo y limpiar hilo/worker si existen
        try:
            # re-enable UI interactions
            try:
                self._set_ui_locked(False)
            except Exception:
                pass
            self._hide_loading()
        except Exception:
            pass
        try:
            wk = getattr(self, '_detect_worker', None)
            if wk is not None:
                try:
                    wk.stop()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            th = getattr(self, '_detect_thread', None)
            if th is not None:
                try:
                    th.quit()
                    th.wait(2000)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            del self._detect_worker
        except Exception:
            pass
        try:
            del self._detect_thread
        except Exception:
            pass

    def _on_detection_finished(self, matched: int) -> None:
        try:
            self.logger.info("DetectWorker finalizó: %s coincidencias asignadas", matched)
        except Exception:
            pass
        try:
            # re-enable UI
            try:
                self._set_ui_locked(False)
            except Exception:
                pass
            self._hide_loading()
        except Exception:
            pass
        try:
            # actualizar marcadores visuales si se completaron filas
            self.update_row_markers()
        except Exception:
            pass
        # limpiar hilo/worker si existen
        try:
            wk = getattr(self, '_detect_worker', None)
            if wk is not None:
                try:
                    wk.stop()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            th = getattr(self, '_detect_thread', None)
            if th is not None:
                try:
                    th.quit()
                    th.wait(2000)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            del self._detect_worker
        except Exception:
            pass
        try:
            del self._detect_thread
        except Exception:
            pass

    def _show_loading(self, text: str = "Cargando...", gif_path: Optional[str] = None) -> None:
        """Muestra un diálogo centrado con `loading.gif` (si existe) y texto opcional."""
        try:
            ld = getattr(self, 'loading', None)
            if LoadingDialog is not None and isinstance(ld, LoadingDialog):
                try:
                    ld.show(text=text, gif_path=gif_path)
                    return
                except Exception:
                    self.logger.debug("LoadingDialog disponible pero falló show()")
            else:
                self.logger.debug("LoadingDialog no disponible; omitiendo diálogo de carga")
        except Exception as e:
            self.logger.exception("Error mostrando diálogo de carga: %s", e)

    # Nota: la lógica del panel de detalles ahora está totalmente delegada a
    # `tabs.marketshare.Logica_Vehiculos.save_details`. Las funciones locales
    # de creación/mostrar/ocultar fueron eliminadas para evitar duplicidad.

    def _hide_loading(self) -> None:
        try:
            ld = getattr(self, 'loading', None)
            if LoadingDialog is not None and isinstance(ld, LoadingDialog):
                try:
                    ld.hide()
                    return
                except Exception:
                    self.logger.debug("LoadingDialog disponible pero falló hide()")
            else:
                self.logger.debug("LoadingDialog no disponible; nothing to hide")
        except Exception as e:
            self.logger.exception("Error ocultando diálogo de carga: %s", e)

    def _find_loading_gif(self) -> Optional[str]:
        """Busca `loading.gif` en ubicaciones probables (módulo, Iconos en padres).

        Recorre hacia arriba hasta 6 niveles buscando una carpeta `Iconos` con `loading.gif`,
        y también verifica si hay `loading.gif` junto al módulo.
        """
        # delegar a LoadingDialog si está disponible
        try:
            if LoadingDialog is not None:
                try:
                    return LoadingDialog.find_loading_gif()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            cur = os.path.dirname(__file__)
            candidate = os.path.join(cur, 'loading.gif')
            if os.path.exists(candidate):
                return os.path.abspath(candidate)
            for _ in range(6):
                candidate = os.path.join(cur, 'Iconos', 'loading.gif')
                if os.path.exists(candidate):
                    return os.path.abspath(candidate)
                candidate2 = os.path.join(cur, 'loading.gif')
                if os.path.exists(candidate2):
                    return os.path.abspath(candidate2)
                parent = os.path.dirname(cur)
                if not parent or parent == cur:
                    break
                cur = parent
        except Exception:
            pass
        return None

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

            # Mostrar diálogo de carga inmediatamente después de elegir archivos
            try:
                base_dir = os.path.dirname(__file__)
                gif_path = os.path.abspath(os.path.join(base_dir, '..', 'Iconos', 'loading.gif'))
                if not os.path.exists(gif_path):
                    gif_path = os.path.abspath(os.path.join(os.path.dirname(base_dir), 'Iconos', 'loading.gif'))
                try:
                    self._show_loading("Cargando archivos...", gif_path=gif_path if os.path.exists(gif_path) else None)
                except Exception:
                    pass
            except Exception:
                pass

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
                        # permitir que la UI procese eventos para mantener la animación del GIF
                        try:
                            if file_count % 100 == 0:
                                QApplication.processEvents()
                        except Exception:
                            pass

                    total_loaded += file_count
                    # procesar eventos también después de cada archivo para evitar que la UI se congele
                    try:
                        QApplication.processEvents()
                    except Exception:
                        pass
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
        finally:
            try:
                self._hide_loading()
            except Exception:
                pass
