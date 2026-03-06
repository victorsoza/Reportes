from typing import TYPE_CHECKING, Any

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

# Reutilizamos FileDropWidget del inventory_tab si está disponible
try:
    from .inventory_tab import FileDropWidget
except Exception:
    FileDropWidget = None


class ComprasLocalTabMixin:
    """Mixin para la subpestaña 'Compras Local' — similar a Importaciones pero
    insertando en las tablas CompraLocales de CC o LGD según el nombre del archivo.
    """

    if TYPE_CHECKING:
        from PyQt6.QtWidgets import QTabWidget

    tab_widget: 'QTabWidget'
    logger: 'Logger'

    def _connect_db(self, key: str) -> Any:  # pragma: no cover - provided by host
        raise NotImplementedError

    def setup_compras_local_tab(self):
        self.compras_local_tab = QWidget()
        layout = QVBoxLayout(self.compras_local_tab)

        layout.addWidget(QLabel("Compras Local"))

        if FileDropWidget is not None:
            self.cl_file_drop = FileDropWidget(self.compras_local_tab)
            self.cl_file_drop.setFixedHeight(140)
            layout.addWidget(self.cl_file_drop)
            try:
                self.cl_file_drop.files_dropped.connect(self.handle_compras_local_files_selected)
            except Exception:
                pass
        else:
            layout.addWidget(QLabel("(Arrastrar y soltar no disponible)"))

        hl = QHBoxLayout()
        self.cl_select_btn = QPushButton("Seleccionar archivos")
        self.cl_select_btn.clicked.connect(lambda checked=False: self.open_compras_local_dialog(checked))
        hl.addWidget(self.cl_select_btn)
        hl.addStretch()
        layout.addLayout(hl)

        self.cl_files_list = QListWidget()
        layout.addWidget(self.cl_files_list)

        self.cl_import_files = []

        self.cl_save_btn = QPushButton("Guardar en SQL")
        self.cl_save_btn.setEnabled(False)
        self.cl_save_btn.clicked.connect(self.insert_compras_local_files_to_sql)
        hl2 = QHBoxLayout()
        hl2.addStretch()
        hl2.addWidget(self.cl_save_btn)
        hl2.addStretch()
        layout.addLayout(hl2)

        try:
            self.tab_widget.addTab(self.compras_local_tab, "Compras Local")
            try:
                setattr(self.compras_local_tab, 'open_file_dialog', lambda checked=False: self.open_compras_local_dialog(checked))
            except Exception:
                pass
        except Exception:
            pass

    def open_compras_local_dialog(self, checked: bool = False):
        parent = getattr(self, 'compras_local_tab', None)
        files, _ = QFileDialog.getOpenFileNames(parent, "Seleccionar archivos Excel", "", "Archivos Excel (*.xls *.xlsx)")
        try:
            self._cl_log_activity(f"Dialogo selección Compras Local abierto, archivos seleccionados: {len(files) if files else 0}")
        except Exception:
            pass
        if files:
            self.handle_compras_local_files_selected(files)

    # Logging helpers (locales al mixin)
    def _cl_append_to_app_log(self, text: str):
        try:
            root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
            path = os.path.join(root, 'app.log')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(text + '\n')
        except Exception:
            pass

    def _cl_log_activity(self, message: str):
        try:
            if hasattr(self, 'logger') and getattr(self, 'logger') is not None:
                try:
                    self.logger.info(message)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            from datetime import datetime
            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._cl_append_to_app_log(f"[{ts}] {message}")
        except Exception:
            pass

    def handle_compras_local_files_selected(self, files):
        try:
            self._cl_log_activity(f"handle_compras_local_files_selected llamado con {len(files)} archivo(s)")
        except Exception:
            pass
        added = 0
        for f in files:
            try:
                if f not in self.cl_import_files:
                    self.cl_import_files.append(f)
                    name = os.path.basename(f)
                    self.cl_files_list.addItem(name)
                    try:
                        self._cl_log_activity(f"Archivo añadido a Compras Local: {name}")
                    except Exception:
                        pass
                    added += 1
            except Exception as e:
                try:
                    self._cl_log_activity(f"ERROR añadiendo archivo {f}: {e}")
                except Exception:
                    pass
        if added:
            try:
                self.cl_files_list.scrollToBottom()
            except Exception:
                pass
        try:
            self.cl_save_btn.setEnabled(len(self.cl_import_files) > 0)
            try:
                self._cl_log_activity(f"Botón Guardar en SQL {'habilitado' if len(self.cl_import_files)>0 else 'deshabilitado'} (archivos={len(self.cl_import_files)})")
            except Exception:
                pass
        except Exception:
            pass

    def _cl_normalize(self, s: str) -> str:
        if s is None:
            return ""
        return ''.join(c for c in str(s).lower() if c.isalnum())

    def insert_compras_local_files_to_sql(self):
        if not self.cl_import_files:
            QMessageBox.information(self.compras_local_tab, "Sin archivos", "No hay archivos seleccionados para importar.")
            return
        try:
            self._cl_log_activity(f"Iniciando importación Compras Local de {len(self.cl_import_files)} archivo(s)")
        except Exception:
            pass

        # Ejecutar borrado previo en SQL Server: limpiar tablas CompraLocales (CC y LGD)
        try:
            try:
                conn_del = self._connect_db('compras_internacionales')
                cursor_del = conn_del.cursor()
                cursor_del.execute("DELETE FROM [ComprasInternacionales].[CC].[CompraLocales]")
                cursor_del.execute("DELETE FROM [ComprasInternacionales].[LGD].[CompraLocales]")
                try:
                    conn_del.commit()
                except Exception:
                    pass
                try:
                    self._cl_log_activity("Tablas [CC].[CompraLocales] y [LGD].[CompraLocales] limpiadas antes de insertar")
                except Exception:
                    pass
            except Exception as e:
                try:
                    self._cl_log_activity(f"ERROR ejecutando DELETE previo CompraLocales: {e}")
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

        cols = [
            "Consecutivo","Factura","Fecha Ingreso","Fecha Compra","Nombre Proveedor","Ruc Proveedor","Tipo pago",
            "Codigo Alterno","Codigo Original","Nombre","Rubro","Nombre Marca","Linea","Moneda","Cantidad",
            "Costo Unitario","Costo Total","Factor Descuento","Descuento","Sub Total","Factor Impuesto","Impuesto","Total",
        ]

        total_inserted = 0
        errors = []
        for path in list(self.cl_import_files):
            name = os.path.basename(path)
            is_cc = 'CC' in name.upper()
            target = '[ComprasInternacionales].[CC].[CompraLocales]' if is_cc else '[ComprasInternacionales].[LGD].[CompraLocales]'
            try:
                self._cl_log_activity(f"Leyendo archivo: {name}")
            except Exception:
                pass
            try:
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
                    self._cl_log_activity(f"Leído correctamente: {name} (hojas={len(xls)})")
                except Exception:
                    pass
            except Exception as e:
                errors.append(f"{name}: error leyendo Excel: {e}")
                try:
                    self._cl_log_activity(f"ERROR leyendo {name}: {e}")
                except Exception:
                    pass
                continue

            try:
                df = pd.concat(xls.values(), ignore_index=True)
                try:
                    self._cl_log_activity(f"Hojas concatenadas en DataFrame: {name} (filas={len(df)})")
                except Exception:
                    pass
            except Exception:
                try:
                    df = list(xls.values())[0]
                except Exception as e:
                    errors.append(f"{name}: error procesando hojas: {e}")
                    continue

            records = []
            df_cols_norm = {self._cl_normalize(c): c for c in df.columns}
            for _, row in df.iterrows():
                vals = []
                for c in cols:
                    key = self._cl_normalize(c)
                    found = None
                    if key in df_cols_norm:
                        found = df_cols_norm[key]
                    else:
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
                self._cl_log_activity(f"Registros preparados para {name}: {len(records)}")
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
                    self._cl_log_activity(f"Insertadas {len(records)} filas en {target} desde {name}")
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
                    self._cl_log_activity(f"ERROR insertando {name} en {target}: {e}")
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

        msg = f"Insertadas: {total_inserted}."
        if errors:
            msg += "\nErrores:\n" + "\n".join(errors[:10])
        # Ejecutar procedimiento de consolidación solicitado
        sp_conn = None
        sp_cursor = None
        try:
            try:
                self._cl_log_activity("Ejecutando SP_02_Consolidar_ComprasLocales en SQL Server")
            except Exception:
                pass
            sp_conn = self._connect_db('compras_internacionales')
            sp_cursor = sp_conn.cursor()
            sp_cursor.execute('EXEC SP_02_Consolidar_ComprasLocales')
            try:
                sp_conn.commit()
            except Exception:
                pass
            try:
                self._cl_log_activity("SP_02_Consolidar_ComprasLocales ejecutado correctamente")
            except Exception:
                pass
        except Exception as e:
            errors.append(f"Error ejecutando SP_02_Consolidar_ComprasLocales: {e}")
            try:
                self._cl_log_activity(f"ERROR ejecutando SP_02_Consolidar_ComprasLocales: {e}")
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

        QMessageBox.information(self.compras_local_tab, "Importación finalizada", msg)
