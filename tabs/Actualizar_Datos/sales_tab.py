import logging
import threading
import time
from typing import cast

import pandas as pd
from PyQt6.QtCore import QDate, Qt
from PyQt6.QtGui import QFont, QPainter, QColor
from PyQt6.QtWidgets import (
    QDateEdit,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QHeaderView,
    QPushButton,
    QTabWidget,
)
# ...existing code...

class SalesTabMixin:
    logger: logging.Logger
    tab_widget: QTabWidget
    sales_columns: list
    sales_df: pd.DataFrame | None

    def _connect_db(self, key):
        raise NotImplementedError

    def format_currency(self, value, currency):
        raise NotImplementedError

    def _parent_widget(self):
        return cast(QWidget, self)

    def setup_sales_tab(self):
        self.sales_tab = QWidget()
        sales_layout = QVBoxLayout(self.sales_tab)

        filters_layout = QHBoxLayout()
        filters_layout.addWidget(QLabel("Fecha inicio:"))
        yesterday = QDate.currentDate().addDays(-1)
        self.sales_start_date_edit = QDateEdit()
        self.sales_start_date_edit.setCalendarPopup(True)
        self.sales_start_date_edit.setDate(yesterday)
        filters_layout.addWidget(self.sales_start_date_edit)

        filters_layout.addWidget(QLabel("Fecha fin:"))
        self.sales_end_date_edit = QDateEdit()
        self.sales_end_date_edit.setCalendarPopup(True)
        self.sales_end_date_edit.setDate(yesterday)
        filters_layout.addWidget(self.sales_end_date_edit)

        filters_layout.addWidget(QLabel("Id Empresa:"))
        self.sales_company_spin = QSpinBox()
        self.sales_company_spin.setMinimum(1)
        self.sales_company_spin.setMaximum(9999)
        self.sales_company_spin.setValue(1)
        filters_layout.addWidget(self.sales_company_spin)

        self.load_sales_button = QPushButton("Consultar ventas")
        self.load_sales_button.clicked.connect(self.load_sales)
        filters_layout.addWidget(self.load_sales_button)

        self.insert_sales_button = QPushButton("Insertar ventas")
        self.insert_sales_button.clicked.connect(self.insert_sales)
        filters_layout.addWidget(self.insert_sales_button)

        filters_layout.addStretch()
        sales_layout.addLayout(filters_layout)

        self.sales_table = QTableWidget()
        self.sales_table.setRowCount(0)
        self.sales_table.setColumnCount(len(self.sales_columns))
        self.sales_table.setHorizontalHeaderLabels(self.sales_columns)
        sales_header = cast(QHeaderView, self.sales_table.horizontalHeader())
        sales_header.setStretchLastSection(True)
        sales_header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        widened_size = max(120, int(sales_header.defaultSectionSize() * 1.5))
        sales_header.setDefaultSectionSize(widened_size)
        sales_header.setMinimumSectionSize(int(widened_size * 0.6))
        self.sales_table.setAlternatingRowColors(True)
        self.sales_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.sales_table.setFont(QFont("Arial", 10))

        sales_layout.addWidget(self.sales_table)
        self.tab_widget.addTab(self.sales_tab, "Reporte de Ventas")
        self.load_sales(show_feedback=False)

    def load_sales(self, show_feedback=True):
        start_date = self.sales_start_date_edit.date()
        end_date = self.sales_end_date_edit.date()
        if start_date > end_date:
            QMessageBox.warning(self._parent_widget(), "Rango inválido", "La fecha de inicio no puede ser posterior a la fecha fin.")
            return

        start_str = start_date.toString("yyyy-MM-dd")
        end_str = end_date.toString("yyyy-MM-dd")
        company_id = self.sales_company_spin.value()
        query = "SET NOCOUNT ON; EXEC dbo.spReporteGeneralVentasWEB @IdEmpresa = ?, @FechaInicio = ?, @FechaFin = ?"

        try:
            self.logger.info(
                "Consultando ventas IdEmpresa=%s, rango %s a %s", company_id, start_str, end_str
            )
            with self._connect_db("sig_web") as conn:
                cursor = conn.cursor()
                cursor.execute(query, (company_id, start_str, end_str))

                rows = []
                columns = []
                while True:
                    if cursor.description:
                        columns = [col[0] for col in cursor.description]
                        rows = cursor.fetchall()
                        break
                    if not cursor.nextset():
                        break

                if not columns:
                    df = pd.DataFrame(columns=self.sales_columns)
                else:
                    records = [tuple(row) for row in rows]
                    df = pd.DataFrame.from_records(records, columns=columns)

                cursor.close()
        except Exception as e:
            self.logger.exception("Error consultando ventas")
            QMessageBox.critical(self._parent_widget(), "Error", f"No se pudieron obtener las ventas: {str(e)}")
            return

        if df.empty:
            self.sales_df = df
            self.sales_table.setRowCount(0)
            self.logger.info("Consulta de ventas sin resultados")
            if show_feedback:
                QMessageBox.information(self._parent_widget(), "Sin resultados", "No se encontraron ventas para el rango seleccionado.")
            return

        for col in self.sales_columns:
            if col not in df.columns:
                df[col] = ""
        df = df[self.sales_columns]
        if 'Origen' in df.columns:
            sales_origin_map = {"CASA CROSS": "Casa Cross", "GIGANTES D": "Los Gigantes Dos"}
            df['Origen'] = df['Origen'].replace(sales_origin_map)
        self.sales_df = df.copy()
        self.logger.info("Ventas consultadas: %s filas", len(df))

        currency_columns = {"Precio", "Venta", "Costo", "Iva", "Total", "Utilidad"}
        self.sales_table.setRowCount(len(df))
        for row_idx in range(len(df)):
            for col_idx, column_name in enumerate(self.sales_columns):
                value = df.iloc[row_idx, col_idx]
                if column_name in currency_columns:
                    display_value = self.format_currency(value, "usd")
                    alignment = Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight
                else:
                    display_value = "" if pd.isna(value) else str(value)
                    alignment = Qt.AlignmentFlag.AlignCenter
                item = QTableWidgetItem(display_value)
                item.setTextAlignment(alignment)
                self.sales_table.setItem(row_idx, col_idx, item)

        self.sales_table.update()
        if show_feedback:
            QMessageBox.information(self._parent_widget(), "Éxito", f"Se cargaron {len(df)} registros de ventas.")

    def insert_sales(self):
        t0 = time.perf_counter()
        if self.sales_df is None or self.sales_df.empty:
            QMessageBox.warning(self._parent_widget(), "Sin datos", "No hay ventas cargadas para insertar.")
            return

        df = self.sales_df.copy()
        df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
        self.logger.info("Preparando %s ventas para inserción", len(df))

        def safe_int(value):
            if pd.isna(value):
                return None
            try:
                return int(round(float(value)))
            except (ValueError, TypeError):
                return None

        def safe_float(value, decimals=2):
            if pd.isna(value):
                return None
            try:
                return round(float(value), decimals)
            except (ValueError, TypeError):
                return None

        def safe_value(value):
            return None if pd.isna(value) else value

        records = []
        total_rows = len(df)
        t_prep_start = time.perf_counter()
        for _, row in df.iterrows():
            fecha = row['Fecha']
            fecha_value = fecha.to_pydatetime() if pd.notna(fecha) else None
            record = (
                safe_float(row['IdFactura'], decimals=4),
                safe_value(row['Consecutivo']),
                safe_value(row['TipoDocumento']),
                safe_value(row['CodVendedor']),
                safe_value(row['Vendedor']),
                safe_value(row['Segmento']),
                safe_value(row['Cars']),
                fecha_value,
                safe_int(row['Dia']),
                safe_int(row['Mes']),
                safe_int(row['Año']),
                safe_value(row['CodCliente']),
                safe_value(row['Cliente']),
                safe_value(row['Cedula']),
                safe_value(row['Departamento']),
                safe_value(row['Municipio']),
                safe_value(row['Tipo']),
                safe_value(row['CodAlterno']),
                safe_value(row['CodOriginal']),
                safe_value(row['Descripcion']),
                safe_value(row['Aplicacion']),
                safe_value(row['Marca']),
                safe_float(row['CodLinea'], decimals=4),
                safe_value(row['Linea']),
                safe_value(row['Rubro']),
                safe_value(row['Sistema']),
                safe_value(row['UnidadMedida']),
                safe_float(row['Precio']),
                safe_int(row['Cantidad']),
                safe_float(row['Venta']),
                safe_float(row['Costo']),
                safe_float(row['Iva']),
                safe_float(row['Total']),
                safe_float(row['Utilidad']),
                safe_float(row['Margen']),
                safe_value(row['TipoPago']),
                safe_value(row['Origen']),
                None,
            )
            records.append(record)
        t_prep_end = time.perf_counter()
        if not records:
            QMessageBox.warning(self._parent_widget(), "Sin datos", "No se encontraron registros válidos para insertar.")
            self.logger.warning("Preparación de ventas sin registros válidos")
            return

        sales_insert_columns = self.sales_columns + ['IdProducto']
        placeholders = ", ".join(["?"] * len(sales_insert_columns))
        columns_clause = ", ".join(sales_insert_columns)
        insert_query = (
            "INSERT INTO [ComprasInternacionales].[dbo].[ReporteVentasWeb] "
            f"({columns_clause}) VALUES ({placeholders})"
        )

        conn = None
        cursor = None
        t_insert_start = time.perf_counter()
        t_sp1_start = None
        t_sp1_end = None
        try:
            conn = self._connect_db("compras_internacionales")
            conn.autocommit = False
            cursor = conn.cursor()
            cursor.fast_executemany = True
            cursor.executemany(insert_query, records)
            conn.commit()
            t_insert_end = time.perf_counter()
            t_sp1_start = time.perf_counter()
            # Ejecutar SP_05_Insert_Id_Nuevos
            try:
                cursor.execute("EXEC SP_05_Insert_Id_Nuevos")
                conn.commit()
                t_sp1_end = time.perf_counter()
                self.logger.info("SP_05_Insert_Id_Nuevos ejecutado correctamente después de insertar ventas.")
                # Ejecutar SP_04_Actualizar_Id_vtas en segundo plano
                def run_sp2_in_background(conn_params):
                    try:
                        bg_conn = self._connect_db("compras_internacionales")
                        bg_cursor = bg_conn.cursor()
                        bg_cursor.execute("EXEC SP_04_Actualizar_Id_vtas")
                        bg_conn.commit()
                        bg_cursor.close()
                        bg_conn.close()
                        self.logger.info("SP_04_Actualizar_Id_vtas ejecutado correctamente en segundo plano.")
                    except Exception as sp2_exc:
                        self.logger.error(f"Error ejecutando SP_04_Actualizar_Id_vtas en segundo plano: {sp2_exc}")
                threading.Thread(target=run_sp2_in_background, args=(None,), daemon=True).start()
                QMessageBox.information(self._parent_widget(), "Éxito", f"Se insertaron {len(records)} ventas y se ejecutó SP_05_Insert_Id_Nuevos. SP_04_Actualizar_Id_vtas se está ejecutando en segundo plano.")
            except Exception as sp1_exc:
                t_sp1_end = time.perf_counter()
                self.logger.error(f"Error ejecutando SP_05_Insert_Id_Nuevos: {sp1_exc}")
                QMessageBox.warning(self._parent_widget(), "Advertencia", f"Las ventas se insertaron, pero hubo un error al ejecutar SP_05_Insert_Id_Nuevos: {sp1_exc}")
            self.logger.info("Se insertaron %s registros en ReporteVentasWeb", len(records))
        except Exception as e:
            t_insert_end = time.perf_counter()
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            self.logger.exception("Error insertando ventas en ReporteVentasWeb")
            QMessageBox.critical(self._parent_widget(), "Error", f"No se pudieron insertar las ventas: {str(e)}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        # Medición de tiempos
        t1 = time.perf_counter()
        prep_time = t_prep_end - t_prep_start
        insert_time = t_insert_end - t_insert_start
        sp1_time = (t_sp1_end - t_sp1_start) if (t_sp1_start is not None and t_sp1_end is not None) else 0
        total_time = t1 - t0
        self.logger.info(f"Tiempo preparación datos: {prep_time:.2f}s | Inserción: {insert_time:.2f}s | SP_05_Insert_Id_Nuevos: {sp1_time:.2f}s | Total: {total_time:.2f}s")
