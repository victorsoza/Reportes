import logging
import os
# importaciones necesarias
import pandas as pd
import pyodbc
from PyQt6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem, QMessageBox, QHeaderView, QLabel, QComboBox, QProgressBar
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QMovie

class AnalisisInventarioTab(QWidget):
	def get_logger(self):
		log_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "app.log")
		logger = logging.getLogger("AnalisisInventarioTab")
		if not logger.handlers:
			handler = logging.FileHandler(log_path, encoding="utf-8")
			formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
			handler.setFormatter(formatter)
			logger.addHandler(handler)
		logger.setLevel(logging.INFO)
		return logger
	def __init__(self, parent=None):
		super().__init__(parent)
		self.logger = self.get_logger()
		self.vlayout = QVBoxLayout(self)

		# Filtros
		self.filter_layout = QHBoxLayout()
		self.filter_layout.addWidget(QLabel("Categoría:"))
		self.category_combo = QComboBox()
		self.category_combo.currentIndexChanged.connect(self.apply_filters)
		self.filter_layout.addWidget(self.category_combo)

		self.filter_layout.addWidget(QLabel("Línea:"))
		self.line_combo = QComboBox()
		self.line_combo.currentIndexChanged.connect(self.apply_filters)
		self.filter_layout.addWidget(self.line_combo)

		self.vlayout.addLayout(self.filter_layout)

		# Icono de carga flotante
		self.loading_overlay = QLabel(self)
		self.loading_overlay.setVisible(False)
		self.loading_overlay.setStyleSheet("background: transparent;")
		self.loading_overlay.setAlignment(Qt.AlignmentFlag.AlignCenter)
		self.loading_overlay.setFixedSize(120, 120)
		self.loading_overlay.move(
			(self.width() - self.loading_overlay.width()) // 2,
			(self.height() - self.loading_overlay.height()) // 2
		)
		self.loading_movie = QMovie(os.path.join(os.path.dirname(__file__), "loading.gif"))
		self.loading_overlay.setMovie(self.loading_movie)

		self.table = QTableWidget()
		self.vlayout.addWidget(self.table)

		self.df = None  # DataFrame completo
		self.data_loaded = False

		# Detectar cuando la pestaña se muestra
		self.installEventFilter(self)

	def start_data_loading(self):
		from PyQt6.QtCore import QThread, pyqtSignal, QObject

		class DataLoaderWorker(QObject):
			finished = pyqtSignal(object)
			error = pyqtSignal(str)
			def run(self):
				try:
					import pandas as pd
					import pyodbc
					conn = pyodbc.connect(
						"DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.42.34;DATABASE=ComprasInternacionales;Trusted_Connection=yes;TrustServerCertificate=yes;Encrypt=no;Connection Timeout=5;"
					)
					cursor = conn.cursor()
					cursor.execute("EXEC SP_00_Analisis_Inv")
					rows = cursor.fetchall()
					columns = [col[0] for col in cursor.description]
					records = [tuple(row) for row in rows]
					df = pd.DataFrame.from_records(records, columns=columns)
					cursor.execute("SELECT [Código Original], [Categoria] FROM [ComprasInternacionales].[dbo].[CodCategoria]")
					cat_rows = cursor.fetchall()
					cat_columns = [col[0] for col in cursor.description]
					cat_records = [tuple(row) for row in cat_rows]
					df_cat = pd.DataFrame.from_records(cat_records, columns=cat_columns)
					if 'Código Original' in df.columns and 'Código Original' in df_cat.columns:
						df = df.merge(df_cat, on='Código Original', how='left')
					# Eliminar duplicados por combinación de 'Código Original' y 'Producto'
					if 'Código Original' in df.columns and 'Producto' in df.columns:
						df = df.drop_duplicates(subset=['Código Original', 'Producto'])
					cursor.close()
					conn.close()
					self.finished.emit(df)
				except Exception as e:
					self.error.emit(str(e))

		self.data_thread = QThread()
		self.worker = DataLoaderWorker()
		self.worker.moveToThread(self.data_thread)
		self.data_thread.started.connect(self.worker.run)
		self.worker.finished.connect(self.on_data_loaded)
		self.worker.error.connect(self.on_data_error)
		self.worker.finished.connect(self.data_thread.quit)
		self.worker.error.connect(self.data_thread.quit)
		self.data_thread.start()

	def eventFilter(self, obj, event):
		from PyQt6.QtCore import QEvent
		if event.type() == QEvent.Type.Show:
			if not self.data_loaded:
				self.show_loading_overlay()
				# Solo cargar datos la primera vez que se muestra la pestaña
				if not hasattr(self, '_data_loading_started'):
					self._data_loading_started = True
					self.start_data_loading()
			else:
				self.hide_loading_overlay()
		return super().eventFilter(obj, event)
	def show_loading_overlay(self):
		# Centrar el overlay en el widget principal
		parent_rect = self.rect()
		self.loading_overlay.resize(120, 120)
		self.loading_overlay.move(
			(parent_rect.width() - self.loading_overlay.width()) // 2,
			(parent_rect.height() - self.loading_overlay.height()) // 2
		)
		self.loading_overlay.raise_()
		self.loading_overlay.setVisible(True)
		self.loading_movie.start()
		from PyQt6.QtWidgets import QApplication
		QApplication.processEvents()

	def hide_loading_overlay(self):
		self.loading_movie.stop()
		self.loading_overlay.setVisible(False)

	def on_data_loaded(self, df):
		self.df = df
		self.data_loaded = True
		self.hide_loading_overlay()
		self.populate_filters(df)
		self.apply_filters()

	def on_data_error(self, error_msg):
		self.data_loaded = True
		self.hide_loading_overlay()
		QMessageBox.critical(self, "Error", f"No se pudo cargar el análisis de inventario:\n{error_msg}")

	def load_data(self):
		try:
			self.logger.info("Iniciando consulta de SP_00_Analisis_Inv en ComprasInternacionales")
			conn = pyodbc.connect(
				"DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.42.34;DATABASE=ComprasInternacionales;Trusted_Connection=yes;TrustServerCertificate=yes;Encrypt=no;Connection Timeout=5;"
			)
			cursor = conn.cursor()
			# Consulta principal
			cursor.execute("EXEC SP_00_Analisis_Inv")
			rows = cursor.fetchall()
			columns = [col[0] for col in cursor.description]
			records = [tuple(row) for row in rows]
			df = pd.DataFrame.from_records(records, columns=columns)
			self.logger.info(f"Consulta exitosa. Filas obtenidas: {len(df)}")

			# Consulta de categorías
			cursor.execute("SELECT [Código Original], [Categoria] FROM [ComprasInternacionales].[dbo].[CodCategoria]")
			cat_rows = cursor.fetchall()
			cat_columns = [col[0] for col in cursor.description]
			cat_records = [tuple(row) for row in cat_rows]
			df_cat = pd.DataFrame.from_records(cat_records, columns=cat_columns)
			self.logger.info(f"Consulta de categorías exitosa. Filas obtenidas: {len(df_cat)}")

			# Unir por Código Original
			if 'Código Original' in df.columns and 'Código Original' in df_cat.columns:
				df = df.merge(df_cat, on='Código Original', how='left')
				self.logger.info("Unión con categorías realizada correctamente.")
			else:
				self.logger.warning("No se encontró la columna 'Código Original' para unir categorías.")

			self.df = df
			self.populate_filters(df)
			self.apply_filters()
			cursor.close()
			conn.close()
		except Exception as e:
			self.logger.error(f"Error al consultar SP_00_Analisis_Inv: {e}")
			QMessageBox.critical(self, "Error", f"No se pudo cargar el análisis de inventario:\n{e}")

	def populate_filters(self, df: pd.DataFrame):
		# Llenar combos de categoría y línea
		# Buscar columnas que contengan "Categoría" y "Línea" (case-insensitive)
		cat_col = next((c for c in df.columns if "categ" in c.lower()), None)
		line_col = next((c for c in df.columns if "linea" in c.lower()), None)
		self.logger.info(f"Columnas detectadas: {list(df.columns)}")
		self.logger.info(f"Columna de categoría detectada: {cat_col}")
		self.logger.info(f"Columna de línea detectada: {line_col}")
		self.category_combo.blockSignals(True)
		self.line_combo.blockSignals(True)
		self.category_combo.clear()
		self.line_combo.clear()
		self.category_combo.addItem("(Todas)")
		self.line_combo.addItem("(Todas)")
		if cat_col:
			cats = sorted(df[cat_col].dropna().unique())
			self.logger.info(f"Valores únicos de categoría: {cats}")
			self.category_combo.addItems([str(c) for c in cats])
		else:
			self.logger.warning("No se encontró columna de categoría en el DataFrame.")
		if line_col:
			lines = sorted(df[line_col].dropna().unique())
			self.logger.info(f"Valores únicos de línea: {lines}")
			self.line_combo.addItems([str(l) for l in lines])
		else:
			self.logger.warning("No se encontró columna de línea en el DataFrame.")
		self.category_combo.blockSignals(False)
		self.line_combo.blockSignals(False)
		self.cat_col = cat_col
		self.line_col = line_col

	def apply_filters(self):
		if self.df is None:
			return
		self.show_loading_overlay()
		df = self.df
		# Filtrar por categoría
		selected_cat = None
		if hasattr(self, 'cat_col') and self.cat_col and self.category_combo.currentIndex() > 0:
			selected_cat = self.category_combo.currentText()
			df = df[df[self.cat_col] == selected_cat]
		# Filtrar por línea
		selected_line = None
		if hasattr(self, 'line_col') and self.line_col and self.line_combo.currentIndex() > 0:
			selected_line = self.line_combo.currentText()
			df = df[df[self.line_col] == selected_line]
		# Determinar columnas de línea a mostrar
		columns_to_show = list(df.columns)
		if selected_cat:
			# Las columnas de línea son todas las que estaban en el pivot (las que no son fijas ni Categoria)
			fixed_cols = {'Código Original', 'Producto', 'TotalStock', 'TotalCosto', 'Categoria'}
			# Si la columna Categoria tiene otro nombre, agregarlo
			if self.cat_col:
				fixed_cols.add(self.cat_col)
			# Columnas de línea presentes en el DataFrame
			line_columns = [col for col in df.columns if col not in fixed_cols]
			# De las filas filtradas, obtener las líneas presentes en la categoría seleccionada
			# Pero como no hay relación directa, mostramos solo las columnas de línea donde haya algún valor distinto de cero en la categoría filtrada
			nonzero_lines = set()
			for col in line_columns:
				if df[col].abs().sum() > 0:
					nonzero_lines.add(col)
			columns_to_show = [col for col in df.columns if col in fixed_cols or col in nonzero_lines]
		self.populate_table(df, columns_to_show)
		self.hide_loading_overlay()

	def populate_table(self, df: pd.DataFrame, columns_to_show=None):
		self.table.clear()
		if columns_to_show is None:
			columns_to_show = list(df.columns)
		df = df[columns_to_show]
		self.table.setRowCount(len(df))
		self.table.setColumnCount(len(df.columns))
		self.table.setHorizontalHeaderLabels(df.columns.tolist())
		for i, (_, row) in enumerate(df.iterrows()):
			for j, value in enumerate(row.values):
				col_name = df.columns[j]
				# Formato moneda para TotalCosto
				if col_name.lower() == "totalcosto" or col_name.lower() == "total_costo":
					try:
						value_float = float(value)
						display_value = f"${value_float:,.2f}"
					except Exception:
						display_value = str(value)
				else:
					display_value = str(value)
				item = QTableWidgetItem(display_value)
				# Centrar todos los valores
				item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
				# Colorear de gris los ceros
				if (isinstance(value, (int, float)) and float(value) == 0) or (isinstance(value, str) and value.strip() in ("0", "0.0")):
					item.setForeground(Qt.GlobalColor.gray)
				self.table.setItem(i, j, item)
		header = self.table.horizontalHeader()
		if header is not None:
			for col in range(self.table.columnCount()):
				self.table.resizeColumnToContents(col)
