from typing import TYPE_CHECKING, Optional, Callable, Any

from PyQt6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QLabel,
    QPushButton,
    QListWidget,
    QFileDialog,
    QHBoxLayout,
    QMessageBox,
)
import os
import pandas as pd

if TYPE_CHECKING:
    from PyQt6.QtWidgets import QTabWidget
    from logging import Logger

# Reutilizamos el widget de arrastrar definido en inventory_tab
try:
    from .inventory_tab import FileDropWidget
except Exception:
    FileDropWidget = None


class ImportacionesTabMixin:
    """Mixin que añade la subpestaña 'Importaciones' con soporte básico
    de arrastrar/soltar y selección múltiple de archivos Excel.
    """

    # Anotaciones esperadas por Pylance / checkers: estos atributos/métodos
    # se proporcionan por la clase que usa el mixin (`ReportesApp`).
    if TYPE_CHECKING:
        from PyQt6.QtWidgets import QTabWidget

    tab_widget: 'QTabWidget'
    logger: 'Logger'

    def _connect_db(self, key: str) -> Any:  # pragma: no cover - provided by host
        raise NotImplementedError

    def setup_importaciones_tab(self):
        self.importaciones_tab = QWidget()
        layout = QVBoxLayout(self.importaciones_tab)

        layout.addWidget(QLabel("Importaciones"))

        # Área de arrastrar/soltar (si está disponible)
        if FileDropWidget is not None:
            self.file_drop_widget = FileDropWidget(self.importaciones_tab)
            self.file_drop_widget.setFixedHeight(140)
            layout.addWidget(self.file_drop_widget)
            # conectar señal (usar nombre único para evitar conflicto MRO con InventoryTabMixin)
            try:
                self.file_drop_widget.files_dropped.connect(self.handle_importaciones_files_selected)
            except Exception:
                pass
        else:
            layout.addWidget(QLabel("(Arrastrar y soltar no disponible)"))

        # Botón para seleccionar archivos manualmente
        hl = QHBoxLayout()
        self.select_files_btn = QPushButton("Seleccionar archivos")
        # conectar al método específico de Importaciones (no al open_file_dialog genérico)
        self.select_files_btn.clicked.connect(lambda checked=False: self.open_importaciones_dialog(checked))
        hl.addWidget(self.select_files_btn)
        hl.addStretch()
        layout.addLayout(hl)

        # Lista de archivos seleccionados
        self.files_list = QListWidget()
        layout.addWidget(self.files_list)

        # Lista interna de rutas
        self.import_files = []

        # Botón para guardar en SQL
        self.save_sql_btn = QPushButton("Guardar en SQL")
        self.save_sql_btn.setEnabled(False)
        self.save_sql_btn.clicked.connect(self.insert_import_files_to_sql)
        hl2 = QHBoxLayout()
        hl2.addStretch()
        hl2.addWidget(self.save_sql_btn)
        hl2.addStretch()
        layout.addLayout(hl2)

        # Añadir la subpestaña al QTabWidget actual si existe
        try:
            self.tab_widget.addTab(self.importaciones_tab, "Importaciones")
            # Exponer un atributo `open_file_dialog` en el widget de la pestaña
            # para que FileDropWidget lo encuentre mediante _find_opener().
            try:
                # Usar setattr para evitar avisos de Pylance al añadir atributos dinámicos
                setattr(self.importaciones_tab, 'open_file_dialog', lambda checked=False: self.open_importaciones_dialog(checked))
            except Exception:
                pass
        except Exception:
            pass

    def open_importaciones_dialog(self, checked: bool = False):
        # `clicked` puede enviar un bool; aceptar opcionalmente para evitar errores
        parent = getattr(self, 'importaciones_tab', None)
        files, _ = QFileDialog.getOpenFileNames(parent, "Seleccionar archivos Excel", "", "Archivos Excel (*.xls *.xlsx)")
        try:
            self._log_activity(f"Dialogo selección abierto, archivos seleccionados: {len(files) if files else 0}")
        except Exception:
            pass
        if files:
            self.handle_importaciones_files_selected(files)

    # Helper de logging local al mixin
    def _append_to_app_log(self, text: str):
        try:
            root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
            path = os.path.join(root, 'app.log')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(text + '\n')
        except Exception:
            pass

    def _log_activity(self, message: str):
        # Intentar logger de la aplicación, si existe
        try:
            if hasattr(self, 'logger') and getattr(self, 'logger') is not None:
                try:
                    self.logger.info(message)
                except Exception:
                    pass
        except Exception:
            pass
        # Siempre persistir en app.log
        try:
            from datetime import datetime
            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._append_to_app_log(f"[{ts}] {message}")
        except Exception:
            pass

    def handle_importaciones_files_selected(self, files):
        # files: lista de rutas (manejador específico de la pestaña Importaciones)
        try:
            self._log_activity(f"handle_importaciones_files_selected llamado con {len(files)} archivo(s)")
        except Exception:
            pass
        added = 0
        for f in files:
            try:
                if f not in self.import_files:
                    self.import_files.append(f)
                    name = os.path.basename(f)
                    self.files_list.addItem(name)
                    try:
                        self._log_activity(f"Archivo añadido a Importaciones: {name}")
                    except Exception:
                        pass
                    added += 1
            except Exception as e:
                try:
                    self._log_activity(f"ERROR añadiendo archivo {f}: {e}")
                except Exception:
                    pass
        if added:
            try:
                # Hacer scroll a fin de lista para mostrar los últimos añadidos
                self.files_list.scrollToBottom()
            except Exception:
                pass
        # actualizar estado del botón guardar
        try:
            self.save_sql_btn.setEnabled(len(self.import_files) > 0)
            try:
                self._log_activity(f"Botón Guardar en SQL {'habilitado' if len(self.import_files)>0 else 'deshabilitado'} (archivos={len(self.import_files)})")
            except Exception:
                pass
        except Exception:
            pass

    def _normalize(self, s: str) -> str:
        if s is None:
            return ""
        return ''.join(c for c in str(s).lower() if c.isalnum())

    def insert_import_files_to_sql(self):
        if not self.import_files:
            QMessageBox.information(self.importaciones_tab, "Sin archivos", "No hay archivos seleccionados para importar.")
            return
        try:
            self._log_activity(f"Iniciando importación de {len(self.import_files)} archivo(s) desde Importaciones tab")
        except Exception:
            pass
        # Ejecutar borrado previo en SQL Server (solicitado): limpiar tablas de destino
        try:
            try:
                conn_del = self._connect_db('compras_internacionales')
                cursor_del = conn_del.cursor()
                cursor_del.execute("DELETE FROM [ComprasInternacionales].[CC].[Importacion]")
                cursor_del.execute("DELETE FROM [ComprasInternacionales].[LGD].[Importacion]")
                try:
                    conn_del.commit()
                except Exception:
                    pass
                try:
                    self._log_activity("Tablas [CC].[Importacion] y [LGD].[Importacion] limpiadas antes de insertar")
                except Exception:
                    pass
            except Exception as e:
                try:
                    self._log_activity(f"ERROR ejecutando DELETE previo: {e}")
                except Exception:
                    pass
        finally:
            try:
                if cursor_del is not None:
                    cursor_del.close()
            except Exception:
                pass
            try:
                if conn_del is not None:
                    conn_del.close()
            except Exception:
                pass
        # columnas objetivo (mismo esquema para CC y LGD)
        cols = [
            "Consecutivo","Factura","Nombre Proveedor","Ruc Proveedor","Codigo Alterno","Codigo Original","Poliza",
            "FOB","%DAI","%ISC","Flete$","Seguro$","OG$","CIF$","CIF_C$","DAI","ISC","TSIM","SPE","SSA","IVA",
            "AGEN","ALMAC","TRAMCNTR","EPNMOV","EPNSCAN","OTROS GASTOS","HONORARIOS","DEMORAJE","TotalServicios",
            "CostoTotal","CostoUnitCord","CostoUnitDolar","Cantidad","Costo Total Factura","Costo Unitario","Descuento",
            "Factor Descuento","Factor Impuesto","Fecha Compra","Fecha Ingreso","Id Compra","Impuesto","Linea","Moneda",
            "Nombre","Nombre Marca","Rubro","Sub Total","Total",
        ]

        total_inserted = 0
        errors = []
        for path in list(self.import_files):
            name = os.path.basename(path)
            is_cc = 'CC' in name.upper()
            target = '[ComprasInternacionales].[CC].[Importacion]' if is_cc else '[ComprasInternacionales].[LGD].[Importacion]'
            try:
                self._log_activity(f"Leyendo archivo: {name}")
            except Exception:
                pass
            try:
                # Elegir motor según extensión: openpyxl para .xlsx, xlrd para .xls
                ext = os.path.splitext(path)[1].lower()
                engine = None
                if ext in ('.xlsx', '.xlsm', '.xltx', '.xltm'):
                    engine = 'openpyxl'
                elif ext == '.xls':
                    engine = 'xlrd'

                if engine:
                    xls = pd.read_excel(path, sheet_name=None, engine=engine)
                else:
                    xls = pd.read_excel(path, sheet_name=None)

                try:
                    self._log_activity(f"Leído correctamente: {name} (hojas={len(xls)})")
                except Exception:
                    pass
            except Exception as e:
                errors.append(f"{name}: error leyendo Excel: {e}")
                try:
                    # Sugerencia explícita si falla por falta de motor para .xls
                    if ext == '.xls' and (isinstance(e, ImportError) or 'No module named xlrd' in str(e) or 'xlrd' in str(e).lower()):
                        self._log_activity(f"ERROR leyendo {name}: {e} -- para archivos .xls instale 'xlrd' (pip install xlrd)")
                    else:
                        self._log_activity(f"ERROR leyendo {name}: {e}")
                except Exception:
                    pass
                continue
            # concatenar hojas
            try:
                df = pd.concat(xls.values(), ignore_index=True)
                try:
                    self._log_activity(f"Hojas concatenadas en DataFrame: {name} (filas={len(df)})")
                except Exception:
                    pass
            except Exception:
                # si solo hay una hoja o concat falla, intentar leer la primera
                try:
                    df = list(xls.values())[0]
                    try:
                        self._log_activity(f"Usando primera hoja para {name} (filas={len(df)})")
                    except Exception:
                        pass
                except Exception as e:
                    errors.append(f"{name}: error procesando hojas: {e}")
                    continue

            # preparar registros
            records = []
            df_cols_norm = {self._normalize(c): c for c in df.columns}
            for _, row in df.iterrows():
                vals = []
                for c in cols:
                    # buscar columna equivalente en df por nombre normalizado
                    key = self._normalize(c)
                    found = None
                    if key in df_cols_norm:
                        found = df_cols_norm[key]
                    else:
                        # intentar variantes: quitar simbolos del nombre original
                        for k2, orig in df_cols_norm.items():
                            if key in k2 or k2 in key:
                                found = orig
                                break
                    if found is not None and found in row:
                        vals.append(None if pd.isna(row[found]) else row[found])
                    else:
                        vals.append(None)
                records.append(tuple(vals))
            try:
                self._log_activity(f"Registros preparados para {name}: {len(records)}")
            except Exception:
                pass

            if not records:
                errors.append(f"{name}: no se encontraron registros válidos")
                continue

            placeholders = ','.join(['?'] * len(cols))
            columns_clause = ','.join([f'[{c}]' for c in cols])
            insert_query = f"INSERT INTO {target} ({columns_clause}) VALUES ({placeholders})"

            conn = None
            cursor = None
            try:
                conn = self._connect_db('compras_internacionales')
                conn.autocommit = False
                cursor = conn.cursor()
                try:
                    cursor.fast_executemany = True
                except Exception:
                    pass
                cursor.executemany(insert_query, records)
                conn.commit()
                total_inserted += len(records)
                try:
                    self._log_activity(f"Insertadas {len(records)} filas en {target} desde {name}")
                except Exception:
                    pass
            except Exception as e:
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                errors.append(f"{name}: error insertando en {target}: {e}")
                try:
                    self._log_activity(f"ERROR insertando {name} en {target}: {e}")
                except Exception:
                    pass
            finally:
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

        # mostrar resultado
        msg = f"Insertadas: {total_inserted}."
        if errors:
            msg += "\nErrores:\n" + "\n".join(errors[:10])
        # Ejecutar procedimiento de consolidación solicitado
        sp_conn = None
        sp_cursor = None
        try:
            try:
                self._log_activity("Ejecutando SP_03_Consolidar_Ingresos en SQL Server")
            except Exception:
                pass
            sp_conn = self._connect_db('compras_internacionales')
            sp_cursor = sp_conn.cursor()
            sp_cursor.execute('EXEC SP_03_Consolidar_Ingresos')
            try:
                sp_conn.commit()
            except Exception:
                pass
            try:
                self._log_activity("SP_03_Consolidar_Ingresos ejecutado correctamente")
            except Exception:
                pass
        except Exception as e:
            errors.append(f"Error ejecutando SP_03_Consolidar_Ingresos: {e}")
            try:
                self._log_activity(f"ERROR ejecutando SP_03_Consolidar_Ingresos: {e}")
            except Exception:
                pass
        finally:
            try:
                if sp_cursor is not None:
                    sp_cursor.close()
            except Exception:
                pass
            try:
                if sp_conn is not None:
                    sp_conn.close()
            except Exception:
                pass

        QMessageBox.information(self.importaciones_tab, "Importación finalizada", msg)
