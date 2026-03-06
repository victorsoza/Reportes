import logging
import os
from typing import cast, Any, Optional
import time
import json

import pandas as pd
from PyQt6.QtCore import Qt, pyqtSignal, QDate, QTimer, QObject, QThread
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
	QWidget,
	QVBoxLayout,
	QHBoxLayout,
	QLabel,
	QPushButton,
	QFileDialog,
	QFrame,
	QMessageBox,
	QTableWidget,
	QTableWidgetItem,
	QHeaderView,
	QDateEdit,
	QTextEdit,
	QProgressBar,
	QTabWidget,
)


class FileDropWidget(QFrame):
	files_dropped = pyqtSignal(list)

	def __init__(self, parent=None):
		super().__init__(parent)
		self.setAcceptDrops(True)
		# Estilos más neutros y modernos que el borde punteado
		self._normal_style = (
			"QFrame{"
			"border: 1px solid #e6eef8;"
			"border-radius: 12px;"
			"background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #ffffff, stop:1 #f7fbff);"
			"padding: 16px;"
			"}"
		)
		self._highlight_style = (
			"QFrame{"
			"border: 2px solid #2b81f7;"
			"border-radius: 12px;"
			"background: #f0f7ff;"
			"padding: 16px;"
			"}"
		)
		self.setStyleSheet(self._normal_style)
		layout = QVBoxLayout(self)
		layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

		# Icono grande (usa un carácter como fallback si no hay recurso)
		self.icon_label = QLabel("\u21aa")
		self.icon_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
		icon_font = QFont("Arial", 36)
		self.icon_label.setFont(icon_font)
		self.icon_label.setStyleSheet("color: #2b81f7;")
		layout.addWidget(self.icon_label)

		# Título y subtítulo
		self.title_label = QLabel("Arrastra archivos XLSX aquí")
		self.title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
		title_font = QFont("Arial", 12)
		title_font.setBold(True)
		self.title_label.setFont(title_font)
		layout.addWidget(self.title_label)

		self.subtitle_label = QLabel("o haz clic para seleccionar")
		self.subtitle_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
		sub_font = QFont("Arial", 9)
		self.subtitle_label.setFont(sub_font)
		self.subtitle_label.setStyleSheet("color: #6b7280;")
		layout.addWidget(self.subtitle_label)

		# Botón estilizado
		self.inner_button = QPushButton("Seleccionar archivos")
		self.inner_button.setCursor(Qt.CursorShape.PointingHandCursor)
		self.inner_button.setFixedWidth(200)
		self.inner_button.setMinimumHeight(36)
		# Estilo claro y texto más grande para asegurar legibilidad
		self.inner_button.setStyleSheet(
			"QPushButton{"
			"background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #2673f2, stop:1 #1b62d6);"
			"color: white; border-radius: 10px; padding: 8px 14px; font-weight: 700; font-size: 12px;"
			"}"
			"QPushButton:hover{background: #1d62d6;}"
		)
		self.inner_button.clicked.connect(self._on_inner_button_clicked)
		layout.addWidget(self.inner_button)

	def dragEnterEvent(self, event):
		if event.mimeData().hasUrls():
			event.acceptProposedAction()
			# aplicar estilo resaltado mientras se arrastra
			try:
				self.setStyleSheet(self._highlight_style)
			except Exception:
				pass
		else:
			event.ignore()

	def dragLeaveEvent(self, event):
		# restaurar estilo normal cuando el usuario sale con el cursor
		try:
			self.setStyleSheet(self._normal_style)
		except Exception:
			pass

	def dropEvent(self, event):
		urls = event.mimeData().urls()
		paths = [u.toLocalFile() for u in urls if u.isLocalFile()]
		excel_paths = [p for p in paths if os.path.splitext(p)[1].lower() in (".xls", ".xlsx")]
		if excel_paths:
			self.files_dropped.emit(excel_paths)
			event.acceptProposedAction()
		else:
			event.ignore()
		# restaurar estilo normal después del drop
		try:
			self.setStyleSheet(self._normal_style)
		except Exception:
			pass

	def mousePressEvent(self, event):
		opener = self._find_opener()
		if callable(opener):
			opener()

	def _on_inner_button_clicked(self):
		opener = self._find_opener()
		if callable(opener):
			opener()

	def _find_opener(self):
		"""Recorre los padres hasta encontrar un objeto que implemente `open_file_dialog`.
		Devuelve la función o `None` si no la encuentra."""
		w = self
		while w is not None:
			w = cast(Any, w.parent())
			if w is None:
				break
			opener = getattr(w, "open_file_dialog", None)
			if callable(opener):
				return opener
		return None


class InsertWorker(QObject):
	"""Worker que ejecuta la inserción en un hilo separado y emite señales de progreso y log."""
	progress = pyqtSignal(int)
	log = pyqtSignal(str)
	file_summary = pyqtSignal(str)
	finished = pyqtSignal(int, int, list)

	def __init__(self, inventory_files, connect_db_callable):
		super().__init__()
		self.inventory_files = inventory_files
		self.connect_db = connect_db_callable

	def run(self):
		import pandas as pd
		import time

		# Ejecutar stored procedure de limpieza antes de insertar
		try:
			sp_conn = self.connect_db('compras_internacionales')
			sp_cursor = sp_conn.cursor()
			sp_cursor.execute('EXEC SP_00_Eliminar_Inventario')
			try:
				sp_conn.commit()
			except Exception:
				pass
			self.log.emit('Procedimiento SP_00_Eliminar_Inventario ejecutado')
		except Exception as e:
			self.log.emit(f'ERROR ejecutando SP_00_Eliminar_Inventario: {e}')
		finally:
			try:
				if sp_cursor:
					sp_cursor.close()
			except Exception:
				pass
			try:
				if sp_conn:
					sp_conn.close()
			except Exception:
				pass

		total_files = len(self.inventory_files)
		inserted_total = 0
		error_count = 0
		per_file_durations = []

		# try to load previous times to estimate, but worker only emits progress by files
		for idx, entry in enumerate(self.inventory_files, start=1):
			file_start = time.perf_counter()
			name = entry.get('name')
			file_type = entry.get('type')
			self.log.emit(f"Procesando '{name}' -> tabla {file_type}")
			try:
				xlsx = pd.read_excel(entry['path'], sheet_name=None, engine='openpyxl')
			except Exception as e:
				self.log.emit(f"ERROR lectura {name}: {e}")
				error_count += 1
				per_file_durations.append(0.0)
				# update progress by file
				pct = int((idx / total_files) * 100)
				self.progress.emit(pct)
				continue

			frames = []
			for sheet, df in xlsx.items():
				frames.append(df)
			if not frames:
				per_file_durations.append(0.0)
				pct = int((idx / total_files) * 100)
				self.progress.emit(pct)
				continue

			df = pd.concat(frames, ignore_index=True)
			records = []
			for _, r in df.iterrows():
				# Mapear campos con tolerancia; usar nombres directos si están
				def mapv(keys):
					for k in keys:
						if k in r and pd.notna(r[k]):
							return r[k]
					return None

				codigo_alterno = mapv(['Codigo Alterno', 'Codigo_Alterno', 'CodigoAlterno'])
				codigo_original = mapv(['Código Original', 'Codigo Original', 'Codigo_Original'])
				nombre = mapv(['Producto', 'Nombre', 'Nombre Producto'])
				descripcion = mapv(['Descripcion', 'Descripción', 'Detalle'])
				rubro = mapv(['Rubro'])
				linea = mapv(['Linea', 'Línea'])
				marca = mapv(['Marca'])
				categoria = mapv(['Categoria', 'Categoría'])
				nombre_bodega = mapv(['Nombre Bodega', 'Nombre_Bodega'])
				stock = mapv(['Stock', 'Cantidad'])
				costo_cord = mapv(['Costo Cordobas', 'Costo_Cordobas', 'Costo'])
				costo_dol = mapv(['Costo Dolares', 'Costo_Dolares'])
				fecha_trans = mapv(['Fecha Transaccion', 'Fecha_Transaccion', 'Fecha'])

				tienda = entry.get('store')
				fecha_corte = entry.get('cutoff')
				if hasattr(fecha_corte, 'toPyDate'):
					fecha_corte_val = fecha_corte.toPyDate()
				else:
					fecha_corte_val = fecha_corte

				record = (
					None if pd.isna(codigo_alterno) else str(codigo_alterno),
					None if pd.isna(codigo_original) else str(codigo_original),
					None if pd.isna(nombre) else str(nombre),
					None if pd.isna(descripcion) else str(descripcion),
					None if pd.isna(rubro) else str(rubro),
					None if pd.isna(linea) else str(linea),
					None if pd.isna(marca) else str(marca),
					None if pd.isna(categoria) else str(categoria),
					None if pd.isna(nombre_bodega) else str(nombre_bodega),
					None if pd.isna(stock) else float(stock),
					None if pd.isna(costo_cord) else float(costo_cord),
					None if pd.isna(costo_dol) else float(costo_dol),
					None if pd.isna(fecha_trans) else (pd.to_datetime(fecha_trans).date() if not isinstance(fecha_trans, pd.Timestamp) else fecha_trans.date()),
					tienda,
					fecha_corte_val,
				)
				records.append(record)

			if not records:
				per_file_durations.append(0.0)
				pct = int((idx / total_files) * 100)
				self.progress.emit(pct)
				continue

			if file_type == 'CC':
				target_table = '[ComprasInternacionales].[CC].[InventarioCasaCross]'
			else:
				target_table = '[ComprasInternacionales].[LGD].[InventarioLosGigantesDos]'

			columns = (
				"[Codigo Alterno], [Código Original], [Producto], [Descripcion], [Rubro], [Linea], [Marca],"
				" [Categoria], [Nombre Bodega], [Stock], [Costo Cordobas], [Costo Dolares], [Fecha Transaccion], [Tienda], [FECHA_CORTE]"
			)
			placeholders = ','.join(['?'] * 15)
			insert_query = f"INSERT INTO {target_table} ({columns}) VALUES ({placeholders})"

			conn = None
			cursor = None
			try:
				conn = self.connect_db('compras_internacionales')
				conn.autocommit = False
				cursor = conn.cursor()
				cursor.fast_executemany = True
				cursor.executemany(insert_query, records)
				conn.commit()
				inserted_total += len(records)
			except Exception as e:
				if conn:
					try:
						conn.rollback()
					except Exception:
						pass
				self.log.emit(f"ERROR insertando {name}: {e}")
				error_count += 1
			finally:
				if cursor:
					cursor.close()
				if conn:
					conn.close()

			file_end = time.perf_counter()
			duration = file_end - file_start
			per_file_durations.append(duration)
			# emitir resumen por archivo con duración
			if error_count == 0:
				self.file_summary.emit(f"{name}: insertadas {len(records)} filas ({duration:.1f}s)")
			else:
				self.log.emit(f"{name}: error durante inserción ({duration:.1f}s)")
			pct = int((idx / total_files) * 100)
			self.progress.emit(pct)

		# finished
		self.finished.emit(inserted_total, error_count, per_file_durations)



class LoadWorker(QObject):
	"""Worker que carga/lee archivos en background para calcular filas y metadatos."""
	progress = pyqtSignal(int)
	log = pyqtSignal(str)
	finished = pyqtSignal(list)

	def __init__(self, paths: list):
		super().__init__()
		self.paths = paths

	def run(self):
		import pandas as pd
		from PyQt6.QtCore import QDate
		results = []
		total = len(self.paths)
		for idx, p in enumerate(self.paths, start=1):
			name = os.path.basename(p)
			self.log.emit(f"Cargando: {name}")
			file_start = time.perf_counter()
			rows = 0
			try:
				xls = pd.read_excel(p, sheet_name=None, engine='openpyxl')
				for s, df in xls.items():
					rows += len(df)
			except Exception as e:
				self.log.emit(f"Error leyendo {name}: {e}")
				rows = 0

			upper = name.upper()
			if 'CC' in upper:
				type_ = 'CC'
				store = 'Casa Cross'
			else:
				type_ = 'LGD'
				store = 'Los Gigantes Dos'

			file_end = time.perf_counter()
			duration = file_end - file_start
			entry = {
				'path': p,
				'name': name,
				'type': type_,
				'store': store,
				'cutoff': QDate.currentDate().addDays(-1),
				'rows': rows,
				'duration': duration,
			}
			results.append(entry)
			# emitir log de finalización con duración
			self.log.emit(f"Cargando: {name} ({duration:.1f}s)")
			pct = int((idx / total) * 100)
			self.progress.emit(pct)

		self.finished.emit(results)



class ConsolidationWorker(QObject):
	"""Worker que ejecuta la stored procedure de consolidación en background."""
	log = pyqtSignal(str)
	finished = pyqtSignal(float, bool, str)

	def __init__(self, connect_db_callable):
		super().__init__()
		self.connect_db = connect_db_callable

	def run(self):
		start = time.perf_counter()
		success = False
		msg = ''
		try:
			conn = self.connect_db('compras_internacionales')
			cur = conn.cursor()
			cur.execute('EXEC SP_01_ConsolidacionInventario')
			try:
				conn.commit()
			except Exception:
				pass
			success = True
			msg = 'SP_01_ConsolidacionInventario ejecutado'
		except Exception as e:
			success = False
			msg = str(e)
		finally:
			try:
				cur.close()
			except Exception:
				pass
			try:
				conn.close()
			except Exception:
				pass
		end = time.perf_counter()
		duration = end - start
		self.log.emit(msg)
		self.finished.emit(duration, success, msg)


class InventoryTabMixin:
	"""Mixin para añadir la pestaña de Inventario con soporte de arrastrar/seleccionar archivos Excel."""

	logger: logging.Logger
	tab_widget: QTabWidget
	inventory_files: list

	# Atributos creados dinámicamente por los workers/threads y timers.
	_progress_timer: Optional[QTimer] = None
	_insert_thread: Optional[QThread] = None
	_insert_worker: Optional[InsertWorker] = None
	_load_thread: Optional[QThread] = None
	_load_worker: Optional[LoadWorker] = None
	_consol_timer: Optional[QTimer] = None
	_consol_thread: Optional[QThread] = None
	_consol_worker: Optional[ConsolidationWorker] = None

	def _connect_db(self, key):
		raise NotImplementedError

	def _parent_widget(self):
		return cast(QWidget, self)

	def setup_inventory_tab(self):
		self.inventory_tab = QWidget()
		inventory_layout = QVBoxLayout(self.inventory_tab)

		# Área de arrastrar y soltar
		self.file_drop_widget = FileDropWidget(self.inventory_tab)
		self.file_drop_widget.setFixedHeight(180)
		inventory_layout.addWidget(self.file_drop_widget)


		# El botón de selección está dentro del cuadro (inner_button). Mantener pequeño espacio debajo.
		inventory_layout.addSpacing(8)

		# Conectar señales
		self.file_drop_widget.files_dropped.connect(self.handle_files_selected)

		# Etiqueta y tabla de archivos seleccionados (estilo similar al HTML)
		inventory_layout.addSpacing(12)
		self.selected_label = QLabel("Archivos seleccionados:")
		self.selected_label.setFont(QFont("Arial", 10))
		inventory_layout.addWidget(self.selected_label)

		self.files_table = QTableWidget()
		self.files_table.setColumnCount(6)
		self.files_table.setHorizontalHeaderLabels(["Archivo", "Tipo", "Tienda", "Fecha de corte", "Filas", "Acciones"])
		header = cast(QHeaderView, self.files_table.horizontalHeader())
		header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
		self.files_table.setAlternatingRowColors(True)
		self.files_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
		inventory_layout.addWidget(self.files_table)

		# Panel de actividades / log de acciones
		self.activity_panel = QTextEdit()
		self.activity_panel.setReadOnly(True)
		self.activity_panel.setFixedHeight(160)
		self.activity_panel.setStyleSheet("background:#ffffff; border:1px solid #e5e7eb; padding:6px;")
		inventory_layout.addWidget(self.activity_panel)

		# Mini barra de carga (progreso estimado basado en tiempos previos)
		self.progress_bar = QProgressBar()
		self.progress_bar.setRange(0, 100)
		self.progress_bar.setValue(0)
		self.progress_bar.setFixedHeight(14)
		inventory_layout.addWidget(self.progress_bar)

		# Botón guardar en SQL (inserta todos los archivos seleccionados)
		actions_layout = QHBoxLayout()
		actions_layout.addStretch()
		self.save_sql_button = QPushButton("Guardar en SQL")
		self.save_sql_button.setStyleSheet("background-color: #16a34a; color: white; padding: 8px 14px; border-radius: 8px;")
		self.save_sql_button.clicked.connect(self.insert_all_files_to_sql)
		self.save_sql_button.setEnabled(False)
		actions_layout.addWidget(self.save_sql_button)
		actions_layout.addStretch()
		inventory_layout.addLayout(actions_layout)

		self.inventory_files = []

		self.tab_widget.addTab(self.inventory_tab, "Inventario")

		# Log inicialización del tab
		try:
			self.log_activity("Pestaña 'Inventario' inicializada")
		except Exception:
			pass
		try:
			self.logger.info("Inventory tab initialized")
		except Exception:
			pass

	def setup_inventory_import_tab(self):
		"""Compatibilidad con versiones anteriores: alias a `setup_inventory_tab`."""
		return self.setup_inventory_tab()

	def open_file_dialog(self):
		files, _ = QFileDialog.getOpenFileNames(self._parent_widget(), "Seleccionar archivos Excel", "", "Archivos Excel (*.xls *.xlsx)")
		if files:
			self.handle_files_selected(files)

	def handle_files_selected(self, files):
		# files: lista de rutas (strings)
		if not files:
			return

		# Deshabilitar interacción mientras cargan
		try:
			self.file_drop_widget.setEnabled(False)
		except Exception:
			pass

		self.log_activity(f"Iniciando carga de {len(files)} archivo(s) en background...")

		# Crear worker y hilo para carga de metadatos (filas, tipo, tienda)
		self._load_thread = QThread()
		self._load_worker = LoadWorker(list(files))
		self._load_worker.moveToThread(self._load_thread)

		# Conexiones
		self._load_thread.started.connect(self._load_worker.run)
		self._load_worker.progress.connect(lambda v: self.progress_bar.setValue(v))
		self._load_worker.log.connect(lambda msg: self.log_activity(msg))

		def _on_loaded(entries: list):
			# añadir entradas y refrescar UI en hilo principal
			self.inventory_files.extend(entries)
			self._refresh_files_table()
			# calcular tiempo total de carga (sumatoria de duraciones por archivo)
			total_time = sum([e.get('duration', 0.0) for e in entries])
			self.log_activity(f"Tiempo de carga total: {total_time:.1f}s")
			self.log_activity(f"Carga completada: {len(entries)} archivo(s) añadidos ({total_time:.1f}s)")
			try:
				self.logger.info("Files loaded: %s", [e['path'] for e in entries])
			except Exception:
				pass
			QMessageBox.information(self._parent_widget(), "Carga completada", f"Se añadieron {len(entries)} archivo(s).")
			# re-habilitar interacción
			try:
				self.file_drop_widget.setEnabled(True)
			except Exception:
				pass
			# limpiar hilo (comprobar que existe antes de llamar a métodos)
			_load_thread = getattr(self, '_load_thread', None)
			if _load_thread is not None:
				try:
					_load_thread.quit()
					_load_thread.wait()
				except Exception:
					pass

		self._load_worker.finished.connect(_on_loaded)
		self._load_thread.start()


	def _determine_type_and_store(self, filename: str):
		upper = filename.upper()
		if 'CC' in upper:
			return 'CC', 'Casa Cross'
		return 'LGD', 'Los Gigantes Dos'

	def _count_excel_rows(self, path: str) -> int:
		try:
			# Intentar leer con pandas y openpyxl
			xls = pd.read_excel(path, sheet_name=None, engine='openpyxl')
			total = 0
			for sheet, df in xls.items():
				total += len(df)
			return total
		except Exception:
			return 0

	def _refresh_files_table(self):
		self.files_table.setRowCount(len(self.inventory_files))
		for row_idx, entry in enumerate(self.inventory_files):
			# Archivo
			item_name = QTableWidgetItem(entry['name'])
			self.files_table.setItem(row_idx, 0, item_name)
			# Tipo
			item_type = QTableWidgetItem(entry['type'])
			if entry['type'] == 'CC':
				item_type.setForeground(Qt.GlobalColor.blue)
			else:
				item_type.setForeground(Qt.GlobalColor.darkGreen)
			self.files_table.setItem(row_idx, 1, item_type)
			# Tienda
			self.files_table.setItem(row_idx, 2, QTableWidgetItem(entry['store']))
			# Fecha de corte (QDateEdit en celda)
			date_edit = QDateEdit()
			date_edit.setCalendarPopup(True)
			date_edit.setDate(entry['cutoff'])
			date_edit.setDisplayFormat('dd/MM/yyyy')
			date_edit.setProperty('row', row_idx)
			date_edit.dateChanged.connect(lambda d, r=row_idx: self._on_cutoff_changed(r, d))
			self.files_table.setCellWidget(row_idx, 3, date_edit)
			# Filas
			self.files_table.setItem(row_idx, 4, QTableWidgetItem(f"{entry['rows']:,}"))
			# Acciones (botón eliminar)
			btn = QPushButton('✖')
			btn.setStyleSheet('color: #e11d48; background: transparent;')
			btn.clicked.connect(lambda _, r=row_idx: self._remove_file_at(r))
			self.files_table.setCellWidget(row_idx, 5, btn)

		# Actualizar estado del botón guardar
		if hasattr(self, 'save_sql_button'):
			self.save_sql_button.setEnabled(len(self.inventory_files) > 0)
		# Log actualización de tabla
		try:
			self.log_activity(f"Tabla actualizada: {len(self.inventory_files)} archivo(s) en la lista")
		except Exception:
			pass
		try:
			self.logger.debug("Files table refreshed: %d entries", len(self.inventory_files))
		except Exception:
			pass


	def _on_cutoff_changed(self, row: int, date: QDate):
		if 0 <= row < len(self.inventory_files):
			self.inventory_files[row]['cutoff'] = date

	def log_activity(self, message: str):
		"""Añade una línea al panel de actividades con marca de tiempo."""
		from datetime import datetime
		ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		text = f"[{ts}] {message}\n"
		try:
			self.activity_panel.append(text)
		except Exception:
			pass

		# También persistir en app.log en la raíz del proyecto
		try:
			self._append_to_app_log(text)
		except Exception:
			pass

		# Intentar también enviar al logger configurado
		try:
			self.logger.info(message)
		except Exception:
			pass


	def _append_to_app_log(self, text: str):
		"""Añade texto al archivo app.log en la raíz del proyecto."""
		root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
		path = os.path.join(root, 'app.log')
		try:
			with open(path, 'a', encoding='utf-8') as f:
				f.write(text)
		except Exception:
			# no romper la ejecución si no se puede escribir el log
			pass
	def _remove_file_at(self, row: int):
		if 0 <= row < len(self.inventory_files):
			self.inventory_files.pop(row)
			self._refresh_files_table()
			# actualizada la tabla tras eliminar
			try:
				self.log_activity(f"Archivo eliminado en la fila {row}")
			except Exception:
				pass
			try:
				self.logger.info("Archivo eliminado (fila %s)", row)
			except Exception:
				pass

	def _map_value(self, row: dict, keys: list):
		"""Intentar obtener un valor de row probando una lista de nombres de columnas."""
		for k in keys:
			if k in row and pd.notna(row[k]):
				return row[k]
		# Probar versiones sin acentos/espacios
		for k in keys:
			k2 = k.replace(' ', '').replace('í', 'i').replace('ó', 'o').replace('é', 'e').replace('á', 'a').replace('ú','u')
			for rk in list(row.keys()):
				if rk.replace(' ', '').lower() == k2.lower() and pd.notna(row[rk]):
					return row[rk]
		return None

	def _times_file_path(self) -> str:
		root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
		return os.path.join(root, 'inventory_times.json')

	def _consolidation_times_file_path(self) -> str:
		root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
		return os.path.join(root, 'consolidation_times.json')

	def _load_times(self) -> list:
		path = self._times_file_path()
		if not os.path.exists(path):
			return []
		try:
			with open(path, 'r', encoding='utf-8') as f:
				data = json.load(f)
				if isinstance(data, list):
					return [float(x) for x in data]
				return []
		except Exception:
			return []

	def _save_times(self, new_durations: list):
		path = self._times_file_path()
		existing = self._load_times()
		combined = existing + [float(x) for x in new_durations]
		# mantener solo los últimos 5 registros
		combined = combined[-5:]
		try:
			with open(path, 'w', encoding='utf-8') as f:
				json.dump(combined, f)
		except Exception:
			pass

	def _load_consolidation_times(self) -> list:
		path = self._consolidation_times_file_path()
		if not os.path.exists(path):
			return []
		try:
			with open(path, 'r', encoding='utf-8') as f:
				data = json.load(f)
				if isinstance(data, list):
					return [float(x) for x in data]
				return []
		except Exception:
			return []

	def _save_consolidation_time(self, duration: float):
		path = self._consolidation_times_file_path()
		existing = self._load_consolidation_times()
		combined = existing + [float(duration)]
		combined = combined[-5:]
		try:
			with open(path, 'w', encoding='utf-8') as f:
				json.dump(combined, f)
		except Exception:
			pass

	def insert_all_files_to_sql(self):
			if not self.inventory_files:
				QMessageBox.warning(self._parent_widget(), 'Sin archivos', 'No hay archivos seleccionados para guardar en SQL.')
				return

			# Preparar estimación usando mediana de tiempos previos
			times = self._load_times()
			if times:
				sorted_times = sorted(times)
				mid = len(sorted_times) // 2
				if len(sorted_times) % 2 == 1:
					median_per_file = sorted_times[mid]
				else:
					median_per_file = (sorted_times[mid - 1] + sorted_times[mid]) / 2.0
			else:
				median_per_file = 1.0

			total_files = len(self.inventory_files)
			expected_total = median_per_file * total_files
			start_time = time.perf_counter()
			self.progress_bar.setValue(0)

			# temporizador para animar la barra
			self._progress_timer = QTimer(self.inventory_tab)
			self._progress_timer.setInterval(200)
			def _update_progress():
				elapsed = time.perf_counter() - start_time
				pct = int(min((elapsed / expected_total) * 100, 99)) if expected_total > 0 else 0
				self.progress_bar.setValue(pct)
			self._progress_timer.timeout.connect(_update_progress)
			self._progress_timer.start()

			self.log_activity('Iniciando inserción a SQL para todos los archivos seleccionados')
			try:
				self.logger.info('Starting batch insert for %d files', total_files)
			except Exception:
				pass

			# Deshabilitar botón mientras se ejecuta
			try:
				self.save_sql_button.setEnabled(False)
			except Exception:
				pass

			# Crear worker y hilo para procesar la inserción en background
			self._insert_thread = QThread()
			self._insert_worker = InsertWorker(list(self.inventory_files), self._connect_db)
			self._insert_worker.moveToThread(self._insert_thread)

			# Conectar señales
			self._insert_thread.started.connect(self._insert_worker.run)
			self._insert_worker.log.connect(lambda msg: self.log_activity(msg))
			self._insert_worker.file_summary.connect(lambda s: self.log_activity(s))
			self._insert_worker.progress.connect(lambda v: self.progress_bar.setValue(v))

			def _on_finished(inserted_total, error_count, durations):
				# detener timer y fijar progreso
				_pt = getattr(self, '_progress_timer', None)
				if _pt is not None:
					try:
						_pt.stop()
					except Exception:
						pass
				self.progress_bar.setValue(100)
				# guardar tiempos
				try:
					self._save_times(durations)
				except Exception:
					pass
				# mensaje intermedio
				self.log_activity('Inventario guardado (inserción completada). Iniciando consolidación...')
				try:
					self.logger.info('Batch insert finished: %d rows, %d errors', inserted_total, error_count)
				except Exception:
					pass

				# preparar y ejecutar consolidación en background
				# estimación por mediana de últimos 5 tiempos
				ctimes = self._load_consolidation_times()
				if ctimes:
					sorted_ct = sorted(ctimes)
					midc = len(sorted_ct) // 2
					if len(sorted_ct) % 2 == 1:
						median_consol = sorted_ct[midc]
					else:
						median_consol = (sorted_ct[midc - 1] + sorted_ct[midc]) / 2.0
				else:
					median_consol = 2.0

				expected_total_consol = median_consol
				start_time_consol = time.perf_counter()
				# animador de barra
				self._consol_timer = QTimer(self.inventory_tab)
				self._consol_timer.setInterval(200)
				def _update_consol_progress():
					elapsed = time.perf_counter() - start_time_consol
					pct = int(min((elapsed / expected_total_consol) * 100, 99)) if expected_total_consol > 0 else 0
					self.progress_bar.setValue(pct)
				self._consol_timer.timeout.connect(_update_consol_progress)
				self._consol_timer.start()

				# crear worker de consolidación
				self._consol_thread = QThread()
				self._consol_worker = ConsolidationWorker(self._connect_db)
				self._consol_worker.moveToThread(self._consol_thread)
				self._consol_worker.log.connect(lambda m: self.log_activity(m))

				def _on_consol_finished(duration, success, msg):
					# detener animador
					_ct = getattr(self, '_consol_timer', None)
					if _ct is not None:
						try:
							_ct.stop()
						except Exception:
							pass
					self.progress_bar.setValue(100)
					# guardar tiempo de consolidación
					try:
						self._save_consolidation_time(duration)
					except Exception:
						pass
					# log final
					if success:
						self.log_activity(f'Consolidación finalizada ({duration:.1f}s)')
					else:
						self.log_activity(f'Consolidación falló ({duration:.1f}s): {msg}')
					# informar al usuario con resumen completo
					QMessageBox.information(self._parent_widget(), 'Proceso completado', f"Inserción: {inserted_total} filas. Errores: {error_count}. Consolidación: {'OK' if success else 'ERROR'} ({duration:.1f}s).")
					# re-habilitar botón
					try:
						self.save_sql_button.setEnabled(True)
					except Exception:
						pass
					# limpiar hilo de consolidación (comprobar existencia)
					_consol_thread = getattr(self, '_consol_thread', None)
					if _consol_thread is not None:
						try:
							_consol_thread.quit()
							_consol_thread.wait()
						except Exception:
							pass

				self._consol_worker.finished.connect(_on_consol_finished)
				# iniciar hilo
				self._consol_thread.started.connect(self._consol_worker.run)
				self._consol_thread.start()

				# limpiar hilo de inserción (comprobar existencia)
				_insert_thread = getattr(self, '_insert_thread', None)
				if _insert_thread is not None:
					try:
						_insert_thread.quit()
						_insert_thread.wait()
					except Exception:
						pass

			self._insert_worker.finished.connect(_on_finished)
			self._insert_thread.start()




