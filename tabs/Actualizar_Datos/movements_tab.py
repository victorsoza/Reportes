import logging
from typing import cast

import pandas as pd
from PyQt6.QtCore import QDate, Qt
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QDateEdit,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QHeaderView,
    QTabWidget,
)


class MovementsTabMixin:
    INSERT_TARGET_TABLE = "KardexMovimientoEntreBodegas"
    POST_INSERT_SP_NAME = "SP_Insert_Id_Prod_Linea_Descrip"

    logger: logging.Logger
    tab_widget: QTabWidget
    movement_columns: list
    movements_df: pd.DataFrame | None

    def _connect_db(self, key):
        raise NotImplementedError

    def _parent_widget(self):
        return cast(QWidget, self)

    def setup_movements_tab(self):
        self.movements_tab = QWidget()
        movements_layout = QVBoxLayout(self.movements_tab)

        filters_layout = QHBoxLayout()
        filters_layout.addWidget(QLabel("Fecha inicio:"))
        yesterday = QDate.currentDate().addDays(-1)
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDate(yesterday)
        filters_layout.addWidget(self.start_date_edit)

        filters_layout.addWidget(QLabel("Fecha fin:"))
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setCalendarPopup(True)
        self.end_date_edit.setDate(yesterday)
        filters_layout.addWidget(self.end_date_edit)

        self.load_movements_button = QPushButton("Consultar movimientos")
        self.load_movements_button.clicked.connect(self.load_movements)
        filters_layout.addWidget(self.load_movements_button)

        self.insert_movements_button = QPushButton("Insertar movimientos")
        self.insert_movements_button.clicked.connect(self.insert_movements)
        filters_layout.addWidget(self.insert_movements_button)

        filters_layout.addStretch()
        movements_layout.addLayout(filters_layout)

        self.movements_table = QTableWidget()
        self.movements_table.setRowCount(0)
        self.movements_table.setColumnCount(len(self.movement_columns))
        self.movements_table.setHorizontalHeaderLabels(self.movement_columns)
        movements_header = cast(QHeaderView, self.movements_table.horizontalHeader())
        movements_header.setStretchLastSection(True)
        movements_header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.movements_table.setAlternatingRowColors(True)
        self.movements_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.movements_table.setFont(QFont("Arial", 10))

        movements_layout.addWidget(self.movements_table)
        self.tab_widget.addTab(self.movements_tab, "Movimientos Kardex")
        self.load_movements(show_feedback=False)

    def load_movements(self, show_feedback=True):
        start_date = self.start_date_edit.date()
        end_date = self.end_date_edit.date()
        if start_date > end_date:
            QMessageBox.warning(self._parent_widget(), "Rango inválido", "La fecha de inicio no puede ser posterior a la fecha fin.")
            return

        selected_start = start_date.toPyDate()
        selected_end = end_date.toPyDate()

        start_str = f"{start_date.toString('yyyy-MM-dd')} 00:00:00"
        query_end_str = f"{end_date.addDays(1).toString('yyyy-MM-dd')} 00:00:00"
        query = "SET NOCOUNT ON; EXEC dbo.spReporteMovimientoGeneralKardexWEB @FechaInicio = ?, @FechaFin = ?"

        try:
            self.logger.info(
                "Consultando movimientos Kardex (seleccionado: %s a %s, consulta: %s a %s [fin exclusivo])",
                selected_start,
                selected_end,
                start_str,
                query_end_str,
            )
            with self._connect_db("sig_web") as conn:
                cursor = conn.cursor()
                cursor.execute(query, (start_str, query_end_str))

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
                    df = pd.DataFrame(columns=self.movement_columns)
                else:
                    records = [tuple(row) for row in rows]
                    df = pd.DataFrame.from_records(records, columns=columns)

                cursor.close()
        except Exception as e:
            self.logger.exception("Error consultando movimientos Kardex")
            QMessageBox.critical(self._parent_widget(), "Error", f"No se pudieron obtener los movimientos: {str(e)}")
            return

        if df.empty:
            self.movements_df = df
            self.movements_table.setRowCount(0)
            self.logger.info("Consulta de movimientos sin resultados")
            if show_feedback:
                QMessageBox.information(self._parent_widget(), "Sin resultados", "No se encontraron movimientos para el rango seleccionado.")
            return

        for col in self.movement_columns:
            if col not in df.columns:
                df[col] = ""

        if 'FechaMovimiento' in df.columns:
            movement_dates = pd.to_datetime(df['FechaMovimiento'], errors='coerce')
            date_mask = movement_dates.dt.date.between(selected_start, selected_end)
            df = df[date_mask.fillna(False)]

        df = df[self.movement_columns]

        if 'Origen' in df.columns:
            origin_map = {"CC": "Casa Cross", "GD": "Los Gigantes Dos"}
            df['Origen'] = df['Origen'].replace(origin_map)
        self.movements_df = df.copy()
        self.logger.info("Movimientos consultados: %s filas", len(df))

        self.movements_table.setRowCount(len(df))
        for row_idx in range(len(df)):
            for col_idx, column_name in enumerate(self.movement_columns):
                value = df.iloc[row_idx, col_idx]
                display_value = "" if pd.isna(value) else str(value)
                item = QTableWidgetItem(display_value)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.movements_table.setItem(row_idx, col_idx, item)

        self.movements_table.update()
        if show_feedback:
            QMessageBox.information(self._parent_widget(), "Éxito", f"Se cargaron {len(df)} movimientos del Kardex.")

    def insert_movements(self):
        if self.movements_df is None or self.movements_df.empty:
            QMessageBox.warning(self._parent_widget(), "Sin datos", "No hay movimientos cargados para insertar.")
            return

        df = self.movements_df.copy()
        df['FechaMovimiento'] = pd.to_datetime(df['FechaMovimiento'], errors='coerce')
        self.logger.info("Preparando %s movimientos para inserción", len(df))

        def safe_value(value):
            return None if pd.isna(value) else value

        def safe_int(value):
            if pd.isna(value):
                return None
            try:
                return int(round(float(value)))
            except (ValueError, TypeError):
                return None

        def safe_float(value):
            if pd.isna(value):
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None

        records = []
        for _, row in df.iterrows():
            fecha = row['FechaMovimiento']
            fecha_value = fecha.to_pydatetime() if pd.notna(fecha) else None
            record = (
                safe_value(row['CodigoAlterno']),
                safe_value(row['CodigoOriginal']),
                safe_value(row['Nombre']),
                safe_value(row['IdBodega']),
                safe_value(row['CodigoBodega']),
                safe_value(row['NombreBodega']),
                safe_value(row['TipoMovimiento']),
                safe_value(row['Consecutivo']),
                safe_int(row['CantidadEntrada']),
                safe_int(row['CantidadSalida']),
                fecha_value,
                safe_float(row['Costo']),
                safe_value(row['Origen']),
                None,
                None,
            )
            records.append(record)

        if not records:
            QMessageBox.warning(self._parent_widget(), "Sin datos", "No se encontraron registros válidos para insertar.")
            self.logger.warning("Preparación de inserción sin registros válidos")
            return

        insert_query = (
            f"INSERT INTO [CentroDistribucion].[dbo].[{self.INSERT_TARGET_TABLE}] "
            "(CodigoAlterno, CodigoOriginal, Nombre, IdBodega, CodigoBodega, NombreBodega, TipoMovimiento, Consecutivo, "
            "CantidadEntrada, CantidadSalida, FechaMovimiento, Costo, Origen, IdProducto, Estado) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )

        conn = None
        cursor = None
        try:
            conn = self._connect_db("centro_distribucion")
            conn.autocommit = False
            cursor = conn.cursor()
            cursor.fast_executemany = True
            cursor.executemany(insert_query, records)
            cursor.execute(f"EXEC {self.POST_INSERT_SP_NAME}")
            conn.commit()
            self.logger.info(
                "Inserción completada en %s: %s registros y %s ejecutado",
                self.INSERT_TARGET_TABLE,
                len(records),
                self.POST_INSERT_SP_NAME,
            )
            QMessageBox.information(
                self._parent_widget(),
                "Éxito",
                f"Se insertaron {len(records)} movimientos en {self.INSERT_TARGET_TABLE} y se ejecutó {self.POST_INSERT_SP_NAME}.",
            )
        except Exception as e:
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            self.logger.exception("Error insertando movimientos en %s", self.INSERT_TARGET_TABLE)
            QMessageBox.critical(self._parent_widget(), "Error", f"No se pudieron insertar los movimientos: {str(e)}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
