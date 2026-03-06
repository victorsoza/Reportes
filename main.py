import sys
import os
import logging
from typing import cast
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QVBoxLayout,
    QWidget,
    QTabWidget,
    QFileDialog,
    QMessageBox,
    QTableWidgetItem,
)
from PyQt6.QtCore import Qt, QDate
from PyQt6.QtGui import QFont
import pandas as pd
import pyodbc

from tabs.Actualizar_Datos.inventory_tab import InventoryTabMixin
from tabs.Actualizar_Datos.sales_tab import SalesTabMixin
from tabs.Actualizar_Datos.movements_tab import MovementsTabMixin
from tabs.Actualizar_Datos.importaciones_tab import ImportacionesTabMixin
from tabs.Actualizar_Datos.compras_local_tab import ComprasLocalTabMixin
from tabs.Analisis_Inventario.analisis_inventario_tab import AnalisisInventarioTab
from tabs.devoluciones_especiales_tab import DevolucionesEspecialesTab
from tabs.reportes.reportes_tab import ReportesTab
from tabs.marketshare.marketshare_tab import MarketshareTab
from db_config import build_connection_string, DB_CONNECTIONS, connect_db

LOG_FILE = os.path.join(os.path.dirname(__file__), "app.log")

# Importar configuración y funciones de conexión

def get_logger():
    logger = logging.getLogger("ReportesApp")
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    # Intentar crear el directorio del log y usar FileHandler; si falla, usar StreamHandler
    try:
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
    except Exception:
        pass

    try:
        handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.debug("Logger initialized; writing to %s", LOG_FILE)
    except Exception:
        stream = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        stream.setFormatter(formatter)
        logger.addHandler(stream)
        logger.debug("Logger initialized with StreamHandler (FileHandler failed)")
    return logger


class ReportesApp(InventoryTabMixin, SalesTabMixin, MovementsTabMixin, ImportacionesTabMixin, ComprasLocalTabMixin, QMainWindow):
    def __init__(self):
        super().__init__()
        self.logger = get_logger()
        self.logger.info("Iniciando aplicación de reportes")
        self.setWindowTitle("Aplicación de Reportes")
        self.setGeometry(100, 100, 800, 600)
        self.column_names = ['CodigoAlterno', 'Descripcion', 'Marca','Bodega', 'Cantidad', 'Costo Dolares', 'Factor', 'Precio Full Dolares', 'Precio Full+IVA Dolares', 'Descuento', 'Costo Cordobas', 'Precio Full Cordobas', 'Precio Full+IVA Cordobas']
        self.left_aligned_columns = {'CodigoAlterno', 'Descripcion', 'Marca', 'Bodega'}
        self.movement_columns = ['CodigoAlterno', 'CodigoOriginal', 'Nombre', 'IdBodega', 'CodigoBodega', 'NombreBodega', 'TipoMovimiento', 'Consecutivo', 'CantidadEntrada', 'CantidadSalida', 'FechaMovimiento', 'Costo', 'Origen']
        self.sales_columns = ['IdFactura', 'Consecutivo', 'TipoDocumento', 'CodVendedor', 'Vendedor', 'Segmento', 'Cars', 'Fecha', 'Dia', 'Mes', 'Año', 'CodCliente', 'Cliente', 'Cedula', 'Departamento', 'Municipio', 'Tipo', 'CodAlterno', 'CodOriginal', 'Descripcion', 'Aplicacion', 'Marca', 'CodLinea', 'Linea', 'Rubro', 'Sistema', 'UnidadMedida', 'Precio', 'Cantidad', 'Venta', 'Costo', 'Iva', 'Total', 'Utilidad', 'Margen', 'TipoPago', 'Origen']
        self.current_df = None
        self.movements_df = None
        self.sales_df = None
        self.inventory_files = []
        self.inventory_insert_thread = None
        self.inventory_insert_worker = None

        # Crear widget central
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # Layout principal con pestañas
        layout = QVBoxLayout(central_widget)
        self.tab_widget = QTabWidget()
        layout.addWidget(self.tab_widget)

        # Crear la tabla principal y agregarla al layout
        from PyQt6.QtWidgets import QTableWidget
        self.table = QTableWidget()
        # No añadir la tabla principal al layout para evitar el cuadro vacío
        # layout.addWidget(self.table)


        # Agregar pestaña principal/padre "Analisis Inventario" usando el nuevo módulo
        self.analisis_inventario_tab = AnalisisInventarioTab()
        self.tab_widget.addTab(self.analisis_inventario_tab, "Analisis Inventario")

        # Agregar nueva pestaña principal "Reportes"
        self.reportes_tab = ReportesTab()
        self.tab_widget.addTab(self.reportes_tab, "Reportes")

        # Agregar nueva pestaña principal "Marketshare"
        self.marketshare_tab = MarketshareTab()
        self.tab_widget.addTab(self.marketshare_tab, "Marketshare")

        self.setup_devoluciones_tab()

        self.update_data_tab = QWidget()
        update_data_layout = QVBoxLayout(self.update_data_tab)
        self.update_data_tab_widget = QTabWidget()
        update_data_layout.addWidget(self.update_data_tab_widget)
        self.tab_widget.addTab(self.update_data_tab, "Actualizacion de datos")

        main_tab_widget = self.tab_widget
        self.tab_widget = self.update_data_tab_widget
        self.setup_sales_tab()
        self.setup_inventory_import_tab()
        self.setup_movements_tab()  # Ahora la pestaña de movimientos se agrega como subpestaña de Actualizacion de datos
        # Nuevas subpestañas solicitadas
        try:
            self.setup_importaciones_tab()
        except Exception:
            pass
        try:
            self.setup_compras_local_tab()
        except Exception:
            pass

        # Volver a la pestaña principal y mostrar 'Actualizacion de datos' como predeterminada
        self.tab_widget = main_tab_widget
        self.tab_widget.setCurrentIndex(self.tab_widget.indexOf(self.update_data_tab))

    def _build_connection_string(self, key):
        return build_connection_string(key)

    def _connect_db(self, key):
        return connect_db(key)

    def setup_devoluciones_tab(self):
        self.devoluciones_tab = DevolucionesEspecialesTab(
            logger=self.logger,
            column_names=self.column_names,
            left_aligned_columns=self.left_aligned_columns,
            complete_from_sql=self.complete_from_sql
        )
        self.tab_widget.addTab(self.devoluciones_tab, "Devoluciones Especiales")

    def format_currency(self, value, currency):
        if pd.isna(value):
            return ""
        try:
            amount = float(value)
        except (TypeError, ValueError):
            return ""
        symbol = "$" if currency == "usd" else "C$"
        formatted = f"{symbol}{abs(amount):,.2f}"
        if amount < 0:
            formatted = f"({formatted})"
        return formatted

    def format_percentage(self, value):
        if pd.isna(value):
            return ""
        try:
            percentage = float(value) * 100
        except (TypeError, ValueError):
            return ""
        return f"{percentage:.2f}%"

    def format_cell_value(self, column_name, value):
        dollar_columns = {"Costo Dolares", "Precio Full Dolares", "Precio Full+IVA Dolares"}
        cordoba_columns = {"Costo Cordobas", "Precio Full Cordobas", "Precio Full+IVA Cordobas"}
        percentage_columns = {"Descuento"}
        if column_name in dollar_columns:
            return self.format_currency(value, "usd")
        if column_name in cordoba_columns:
            return self.format_currency(value, "nio")
        if column_name in percentage_columns:
            return self.format_percentage(value)
        return "" if pd.isna(value) else str(value)

    def load_excel(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Seleccionar Archivo Excel", "", "Archivos Excel (*.xlsx)")
        if not file_path:
            return
        try:
            self.logger.info("Iniciando carga de Excel: %s", file_path)
            df = pd.read_excel(file_path)
            expected_cols = self.column_names
            mandatory = ['CodigoAlterno', 'Bodega', 'Cantidad']
            if not all(col in df.columns for col in mandatory):
                QMessageBox.warning(self, "Error", "El archivo debe contener las columnas obligatorias: CodigoAlterno, Bodega, Cantidad")
                return
            # Fill missing optional with empty
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = ""
            # Reorder to expected
            df = df[expected_cols]
            
            # Completar datos desde SQL Server
            df = self.complete_from_sql(df)
            self.current_df = df.copy()
            
            # Set table
            self.table.setRowCount(len(df))
            self.table.setColumnCount(len(self.column_names))
            self.table.setHorizontalHeaderLabels(self.column_names)
            cantidad_index = self.column_names.index("Cantidad")
            for row in range(len(df)):
                for col in range(len(self.column_names)):
                    column_name = self.column_names[col]
                    value = self.format_cell_value(column_name, df.iloc[row, col])
                    item = QTableWidgetItem(value)
                    if column_name in self.left_aligned_columns:
                        item.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
                    else:
                        item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    self.table.setItem(row, col, item)
            self.table.update()  # Force refresh of the table
            has_rows = not df.empty
            self.logger.info("Excel cargado correctamente con %s filas", len(df))
            QMessageBox.information(self, "Éxito", f"Archivo cargado y datos completados desde SQL Server con {len(df)} filas.")
        except Exception as e:
            has_previous_data = self.current_df is not None and not self.current_df.empty
            self.logger.exception("Error al cargar Excel %s", file_path)
            QMessageBox.critical(self, "Error", f"Error al cargar el archivo: {str(e)}")

    

    def complete_from_sql(self, df):
        expected_cols = self.column_names
        try:
            self.logger.info("Completando datos desde SQL Server para %s filas", len(df))
            conn = self._connect_db("centro_distribucion")
            cursor = conn.cursor()
            
            # Query the table for relevant columns
            cursor.execute("SELECT CodigoAlterno, Origen, Costo, FactorMaximo, Descuento FROM [PV].[CostosFactoresPorSegmento]")
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            records = [tuple(row) for row in rows]
            sql_df = pd.DataFrame.from_records(records, columns=columns)
            sql_df['Descuento'] = pd.to_numeric(sql_df['Descuento'], errors='coerce') / 100.0
            
            # Merge with df on CodigoAlterno and Bodega (df) with Origen (sql)
            df = df.merge(sql_df, left_on=['CodigoAlterno', 'Bodega'], right_on=['CodigoAlterno', 'Origen'], how='left', suffixes=('', '_sql'))
            
            # Convert to numeric for safe fillna
            df['Costo Dolares'] = pd.to_numeric(df['Costo Dolares'], errors='coerce')
            df['Factor'] = pd.to_numeric(df['Factor'], errors='coerce')
            df['Descuento'] = pd.to_numeric(df['Descuento'], errors='coerce')
            df['Costo Cordobas'] = pd.to_numeric(df['Costo Cordobas'], errors='coerce')
            df['Precio Full Cordobas'] = pd.to_numeric(df['Precio Full Cordobas'], errors='coerce')
            df['Precio Full+IVA Cordobas'] = pd.to_numeric(df['Precio Full+IVA Cordobas'], errors='coerce')
            df['Precio Full Dolares'] = pd.to_numeric(df['Precio Full Dolares'], errors='coerce')
            df['Precio Full+IVA Dolares'] = pd.to_numeric(df['Precio Full+IVA Dolares'], errors='coerce')
            df['Costo'] = pd.to_numeric(df['Costo'], errors='coerce')
            df['FactorMaximo'] = pd.to_numeric(df['FactorMaximo'], errors='coerce')
            if 'Descuento_sql' in df.columns:
                df['Descuento_sql'] = pd.to_numeric(df['Descuento_sql'], errors='coerce')
            
            # Fill missing values
            df['Costo Dolares'] = df['Costo Dolares'].fillna(df['Costo'])
            df['Factor'] = df['Factor'].fillna(df['FactorMaximo'])
            df['Descuento'] = df['Descuento'].fillna(df['Descuento_sql'])

            # Recalculate dependent pricing columns
            cordoba_rate = 36.6243
            iva_multiplier = 1.15
            df['Costo Cordobas'] = df['Costo Dolares'] * cordoba_rate
            df['Precio Full Cordobas'] = df['Costo Cordobas'] * df['Factor']
            df['Precio Full+IVA Cordobas'] = df['Precio Full Cordobas'] * iva_multiplier
            df['Precio Full Dolares'] = df['Costo Dolares'] * df['Factor']
            df['Precio Full+IVA Dolares'] = df['Precio Full Dolares'] * iva_multiplier
            
            # Drop extra columns
            df = df.drop(columns=['Costo', 'Origen', 'FactorMaximo', 'Descuento_sql'], errors='ignore')
            # Ensure column order
            df = df[expected_cols]
            
            # Check matches
            matches = len(df[df['Costo Dolares'].notna() | df['Factor'].notna() | df['Descuento'].notna()])
            self.logger.info("Completado desde SQL Server; coincidencias: %s", matches)
            QMessageBox.information(self, "Completado", f"Datos completados desde SQL Server. Coincidencias encontradas: {matches}")
            
            cursor.close()
            conn.close()
            return df
        except Exception as e:
            self.logger.exception("Error completando datos desde SQL Server")
            QMessageBox.warning(self, "Advertencia", f"Error al completar datos desde SQL Server: {str(e)}. Los datos se cargarán sin completar.")
            return df

if __name__ == "__main__":
    app = QApplication(sys.argv)
    # Establecer icono de la aplicación usando ruta compatible con bundle PyInstaller
    try:
        from PyQt6.QtGui import QIcon
        if getattr(sys, 'frozen', False):
            base_path = getattr(sys, '_MEIPASS', os.path.dirname(__file__))
        else:
            base_path = os.path.dirname(__file__)
        icon_path = os.path.join(base_path, 'Iconos', 'ComprasInternacionales.ico')
        if os.path.exists(icon_path):
            app.setWindowIcon(QIcon(icon_path))
    except Exception:
        pass

    window = ReportesApp()
    window.show()
    sys.exit(app.exec())