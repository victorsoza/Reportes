from typing import Optional, Callable
import logging
import pandas as pd
import db_config
from PyQt6.QtWidgets import QWidget, QVBoxLayout, QMessageBox, QFileDialog, QInputDialog, QTableWidgetItem


class DevolucionesEspecialesTab(QWidget):
    def __init__(
        self,
        parent=None,
        logger: logging.Logger | None = None,
        column_names: Optional[list] = None,
        left_aligned_columns: Optional[set] = None,
        complete_from_sql: Optional[Callable] = None,
        **kwargs,
    ):
        super().__init__(parent)
        self.logger = logger or logging.getLogger("DevolucionesEspecialesTab")
        # Props passed from ReportesApp
        self.column_names = column_names
        self.left_aligned_columns = left_aligned_columns
        self.complete_from_sql = complete_from_sql

        self.current_df: Optional[pd.DataFrame] = None
        self.updating_table = False

        # UI setup
        layout = QVBoxLayout(self)

        from PyQt6.QtWidgets import QPushButton, QTableWidget, QHBoxLayout

        btn_layout = QHBoxLayout()
        self.load_button = QPushButton("Cargar Excel")
        self.load_button.clicked.connect(self.load_excel)
        btn_layout.addWidget(self.load_button)

        self.export_button = QPushButton("Exportar")
        self.export_button.clicked.connect(self.export_table)
        btn_layout.addWidget(self.export_button)

        self.update_sql_button = QPushButton("Actualizar SQL")
        self.update_sql_button.clicked.connect(self.confirm_update_sql)
        btn_layout.addWidget(self.update_sql_button)

        layout.addLayout(btn_layout)

        self.table = QTableWidget()
        layout.addWidget(self.table)

    def load_excel(self):
        from PyQt6.QtWidgets import QFileDialog, QMessageBox
        import pandas as pd
        file_path, _ = QFileDialog.getOpenFileName(self, "Seleccionar Archivo Excel", "", "Archivos Excel (*.xlsx)")
        if not file_path:
            return
        try:
            df = pd.read_excel(file_path)
            if self.column_names:
                for col in self.column_names:
                    if col not in df.columns:
                        df[col] = ""
                df = df[self.column_names]
            self.current_df = df.copy()
            self.table.setRowCount(len(df))
            self.table.setColumnCount(len(df.columns))
            self.table.setHorizontalHeaderLabels(list(df.columns))
            for row in range(len(df)):
                for col in range(len(df.columns)):
                    val = df.iloc[row, col]
                    item = QTableWidgetItem(str(val) if not pd.isna(val) else "")
                    self.table.setItem(row, col, item)
            self.table.update()
            QMessageBox.information(self, "Éxito", f"Archivo cargado con {len(df)} filas.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al cargar el archivo: {str(e)}")

    def export_table(self):
        if self.current_df is None or self.current_df.empty:
            QMessageBox.warning(self, "Sin datos", "Carga un archivo antes de exportar la tabla.")
            return
        file_path, selected_filter = QFileDialog.getSaveFileName(
            self,
            "Exportar Tabla",
            "",
            "Archivos Excel (*.xlsx);;Archivos CSV (*.csv)"
        )
        if not file_path:
            return
        try:
            export_path = file_path
            if selected_filter.startswith("Archivos CSV") or file_path.lower().endswith(".csv"):
                if not file_path.lower().endswith(".csv"):
                    export_path = f"{file_path}.csv"
                self.current_df.to_csv(export_path, index=False)
            else:
                if not file_path.lower().endswith(".xlsx"):
                    export_path = f"{file_path}.xlsx"
                self.current_df.to_excel(export_path, index=False)
            QMessageBox.information(self, "Éxito", f"Tabla exportada correctamente")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo exportar la tabla: {str(e)}")

    def confirm_update_sql(self):
        if self.current_df is None or self.current_df.empty:
            QMessageBox.warning(self, "Sin datos", "Carga un archivo antes de actualizar la base de datos.")
            return
        table_name, ok = QInputDialog.getText(self, "Tabla destino", "Nombre completo de la tabla (schema.tabla):", text="PV.CostosFactoresPorSegmento")
        if not ok:
            return
        resp = QMessageBox.question(self, "Confirmar", f"¿Actualizar Costo y FactorMaximo en la tabla '{table_name}' para las filas mostradas?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if resp == QMessageBox.StandardButton.Yes:
            try:
                updated = self.update_sql_rows(db_key="centro_distribucion", table=table_name)
                QMessageBox.information(self, "Éxito", f"Filas actualizadas: {updated}")
                try:
                    self.recalculate_all_dependent_columns()
                except Exception:
                    pass
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al actualizar la base de datos: {str(e)}")

    def update_sql_rows(self, db_key: str = "sig_web", table: Optional[str] = None) -> int:
        if self.current_df is None or self.current_df.empty:
            return 0
        table_str: str = table if table is not None else "PV.CostosFactoresPorSegmento"
        conn = db_config.connect_db(db_key)
        try:
            cursor = conn.cursor()
            cursor.fast_executemany = True
            sql = f"UPDATE {table_str} SET [Costo]=?, [FactorMaximo]=? WHERE CodigoAlterno=?"
            params = []
            df_costo_col = None
            df_factor_col = None
            for candidate in ("Costo Dolares", "Costo", "Costo_USD"):
                if candidate in self.current_df.columns:
                    df_costo_col = candidate
                    break
            for candidate in ("Factor", "FactorMaximo", "Factor Maximo", "FactorMax"):
                if candidate in self.current_df.columns:
                    df_factor_col = candidate
                    break
            if df_costo_col is None:
                df_costo_col = "Costo"
            if df_factor_col is None:
                df_factor_col = "FactorMaximo"
            for _, row in self.current_df.iterrows():
                codigo = row.get("CodigoAlterno")
                if pd.isna(codigo):
                    continue
                raw_costo = row.get(df_costo_col)
                raw_factor = row.get(df_factor_col)
                costo_val = None if pd.isna(raw_costo) else self._parse_numeric(raw_costo)
                factor_val = None if pd.isna(raw_factor) else self._parse_numeric(raw_factor)
                params.append((None if costo_val is None else float(costo_val), None if factor_val is None else float(factor_val), str(codigo)))
            if not params:
                return 0
            cursor.executemany(sql, params)
            conn.commit()
            return len(params)
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def recalculate_all_dependent_columns(self):
        if self.current_df is None:
            return
        self.updating_table = True
        try:
            for row in range(len(self.current_df)):
                try:
                    self.update_dependent_columns(row)
                except Exception:
                    if self.logger:
                        self.logger.exception("Error al recalcular fila %s", row)
            if self.table is not None:
                try:
                    self.table.update()
                except Exception:
                    pass
        finally:
            self.updating_table = False

    def update_dependent_columns(self, row: int) -> None:
        """Recalcula las columnas dependientes para la fila `row`.

        Actualiza valores como 'Costo Cordobas', 'Precio Full Cordobas',
        'Precio Full+IVA Cordobas', 'Precio Full Dolares' y
        'Precio Full+IVA Dolares' en `self.current_df` cuando sea posible.
        """
        if self.current_df is None:
            return
        if row < 0 or row >= len(self.current_df):
            return
        try:
            # Determinar columnas candidatas
            df = self.current_df
            costo_col = next((c for c in ("Costo Dolares", "Costo", "Costo_USD") if c in df.columns), None)
            factor_col = next((c for c in ("Factor", "FactorMaximo", "Factor Maximo", "FactorMax") if c in df.columns), None)
            descuento_col = next((c for c in ("Descuento",) if c in df.columns), None)

            raw_costo = df.at[row, costo_col] if costo_col is not None else None
            raw_factor = df.at[row, factor_col] if factor_col is not None else None
            raw_desc = df.at[row, descuento_col] if descuento_col is not None else None

            costo_usd = None if pd.isna(raw_costo) else self._parse_numeric(raw_costo)
            factor = None if pd.isna(raw_factor) else self._parse_numeric(raw_factor)
            descuento = None if pd.isna(raw_desc) else self._parse_numeric(raw_desc)

            if costo_usd is None:
                costo_usd = 0.0
            if factor is None:
                factor = 1.0
            # Si el descuento viene en porcentaje (0-100), normalizar a 0-1
            if descuento is not None and descuento > 1:
                try:
                    descuento = float(descuento) / 100.0
                except Exception:
                    descuento = descuento

            # Parámetros económicos (valores por defecto razonables)
            cordoba_rate = 36.6243
            iva_multiplier = 1.15

            costo_cordoba = float(costo_usd) * cordoba_rate
            precio_full_dolares = float(costo_usd) * float(factor)
            precio_full_iva_dolares = precio_full_dolares * iva_multiplier
            precio_full_cordobas = costo_cordoba * float(factor)
            precio_full_iva_cordobas = precio_full_cordobas * iva_multiplier

            # Asignar en el DataFrame si existen las columnas destino, o crearlas
            df.at[row, 'Costo Cordobas'] = costo_cordoba
            df.at[row, 'Precio Full Dolares'] = precio_full_dolares
            df.at[row, 'Precio Full+IVA Dolares'] = precio_full_iva_dolares
            df.at[row, 'Precio Full Cordobas'] = precio_full_cordobas
            df.at[row, 'Precio Full+IVA Cordobas'] = precio_full_iva_cordobas

            # Si hay una tabla visual asociada, no hacer refresh intensivo cuando está en actualización masiva
            if self.table is not None and not self.updating_table:
                try:
                    self.table.update()
                except Exception:
                    pass
        except Exception:
            if self.logger:
                self.logger.exception("Error actualizando columnas dependientes para fila %s", row)

    def _parse_numeric(self, value):
        try:
            return float(value)
        except Exception:
            try:
                s = str(value).replace('$','').replace(',','').strip()
                return float(s)
            except Exception:
                return None
