from PyQt6.QtWidgets import (
 	QWidget,
 	QVBoxLayout,
 	QHBoxLayout,
 	QPushButton,
	QTableWidget,
 	QTableWidgetItem,
 	QMessageBox,
 	QMenu,
 	QHeaderView,
 	QLabel,
 	QApplication,
 	QLineEdit,
    QCompleter,
 	QListWidget,
 	QListWidgetItem,
 	QWidgetAction,
	QTabWidget,
)
from PyQt6.QtCore import Qt, QObject, QThread, pyqtSignal
from PyQt6.QtGui import QMovie, QGuiApplication, QPainter, QColor, QPen, QPolygon
from PyQt6 import QtCore
import os
from typing import cast, TYPE_CHECKING, Optional
if TYPE_CHECKING:
	# Help static type checkers resolve the symbol without executing runtime import
	from .reporte_inventario_seguro_resumen_tab import ReporteInventarioSeguroResumenTab  # type: ignore
else:
	try:
		from .reporte_inventario_seguro_resumen_tab import ReporteInventarioSeguroResumenTab
	except Exception:
		# Fallback for environments where package relative imports are resolved differently
		try:
			from tabs.reporte_inventario_seguro_resumen_tab import ReporteInventarioSeguroResumenTab
		except Exception:
			# If import fails at runtime, define a minimal placeholder to avoid NameError.
			class ReporteInventarioSeguroResumenTab:
				def __init__(self, *args, **kwargs):
					raise ImportError("Could not import ReporteInventarioSeguroResumenTab")

import pyodbc
from db_config import connect_db
import logging

# Logger configured to write to app.log (only adds handler if none present)
logger = logging.getLogger("app")
if not logger.handlers:
	fh = logging.FileHandler("app.log", encoding="utf-8")
	fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
	logger.addHandler(fh)
	logger.setLevel(logging.INFO)


class ReporteInventarioSeguroTab(QWidget):
	# Type stubs so static checkers know these methods exist on the class
	if TYPE_CHECKING:
		def _show_loading_overlay(self) -> None: ...
		def _hide_loading_overlay(self) -> None: ...
	def __init__(self, parent=None):
		super().__init__(parent)

		# Custom header that paints a subtle filter-area border on each section
		class FilterHeader(QHeaderView):
			def __init__(self, orient, parent=None):
				super().__init__(orient, parent)
				# width (pixels) reserved for the filter icon area at right of section
				# increased so the hit area is more inward (avoids conflicting with resize handle)
				self.filter_icon_width = 28

			def paintSection(self, painter: QPainter, rect, logicalIndex):
				# Paint default header
				super().paintSection(painter, rect, logicalIndex)
				# Draw a subtle rounded rectangle to indicate filter area on the right
				try:
					painter.save()
					pen = QPen(QColor(0, 0, 0, 40))
					pen.setWidth(1)
					painter.setPen(pen)
					# Slightly inset the rectangle so it doesn't hug the section divider
					inset_left = max(2, rect.width() - self.filter_icon_width - 6)
					r = rect.adjusted(inset_left, 4, -6, -4)
					painter.drawRoundedRect(r, 3, 3)
					# Draw a small funnel icon centered inside the filter area
					try:
						painter.save()
						# icon size: leave a small padding inside r
						pad = 4
						icon_w = max(8, min(16, r.width() - pad * 2))
						icon_h = max(6, int(icon_w * 0.6))
						cx = r.center().x()
						cy = r.center().y()
						left_x = int(cx - icon_w / 2)
						right_x = int(cx + icon_w / 2)
						top_y = int(cy - icon_h / 2)
						bottom_y = int(cy + icon_h / 2)
						# funnel polygon: wide top, narrow bottom
						pts = [QtCore.QPoint(left_x, top_y), QtCore.QPoint(right_x, top_y), QtCore.QPoint(cx, bottom_y)]
						poly = QPolygon(pts)
						painter.setPen(Qt.PenStyle.NoPen)
						painter.setBrush(QColor(0, 0, 0, 150))
						painter.drawPolygon(poly)
					finally:
						painter.restore()
				finally:
					painter.restore()
		# Main layout for this widget. We'll host two subtabs: "Detalle" and "Resumen".
		main_layout = QVBoxLayout(self)
		self._tabs = QTabWidget(self)
		# Detail tab: contains the existing actions and table
		detalle = QWidget(self)
		detalle_layout = QVBoxLayout(detalle)

		# Acciones
		actions = QHBoxLayout()
		self.load_button = QPushButton("Cargar")
		self.load_button.clicked.connect(self.load_data)
		actions.addWidget(self.load_button)
		actions.addStretch()
		detalle_layout.addLayout(actions)

		# Tabla
		self.table = QTableWidget()
		headers = [
			"Codigo Alterno",
			"Código Original",
			"Producto",
			"Descripcion",
			"Rubro",
			"Linea",
			"Marca",
			"Categoria",
			"Nombre Bodega",
			"Stock",
			"Costo Dolares",
			"No. Sig Pro",
			"No. Scm",
			"Clase",
		]
		self.table.setColumnCount(len(headers))
		self.table.setHorizontalHeaderLabels(headers)
		# create and install our custom header view
		header = FilterHeader(Qt.Orientation.Horizontal, self.table)
		self.table.setHorizontalHeader(header)
		# header is a FilterHeader instance; safe to cast
		header = cast(QHeaderView, header)
		header.setStretchLastSection(True)
		# Allow user to resize columns interactively; we'll size to contents after loading data
		for i in range(len(headers)):
			header.setSectionResizeMode(i, QHeaderView.ResizeMode.Interactive)
		# Install an event filter on the header viewport so we can distinguish
		# clicks on the small filter icon area (open filter menu) from general
		# header clicks (perform sorting). This avoids opening the filter menu
		# on every header click while keeping the existing sort-on-click UX.
		try:
			vp = header.viewport()
			if vp is not None:
				vp.installEventFilter(self)
		except Exception:
			logger.exception("Failed to install header event filter")

		# Active filters per column index
		self._active_filters: dict[int, str] = {}
		# Per-column unique-values cache used for header menus (col_index -> set(values))
		self._col_unique_cache: dict[int, set] = {}
		# runtime column name -> index map (populated after load)
		self._col_indices: dict[str, int] = {}
		# suppress itemChanged handler while programmatically populating
		self._suppress_item_changed = False
		# recent edits cache to avoid processing duplicate itemChanged signals
		self._recent_edits: dict[tuple[int, int], str] = {}
		# keep references to active worker threads so they are not destroyed
		# while still running (prevents 'QThread: Destroyed while thread is still running')
		self._active_threads: list[QThread] = []
		logger.debug("ReporteInventarioSeguroTab initialized")

		self.table.setAlternatingRowColors(True)
		self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
		# Enable custom context menu for multi-paste support
		self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
		self.table.customContextMenuRequested.connect(self._on_table_context_menu)
		# Double-click to open a completer-equipped editor for Clase
		self.table.cellDoubleClicked.connect(self._on_cell_double_clicked)
		detalle_layout.addWidget(self.table)

		# Loading GIF overlay
		loading_gif_path = os.path.join(os.path.dirname(__file__), "loading.gif")
		self.loading_label = QLabel(self)
		self.loading_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
		self.loading_label.setStyleSheet(
			"background: rgba(255,255,255,0.7); border-radius: 12px; padding: 8px;"
		)
		self.loading_label.setVisible(False)
		self.loading_movie = QMovie(loading_gif_path)
		self.loading_movie.setScaledSize(QtCore.QSize(150, 150))
		self.loading_label.setFixedSize(150, 150)
		self.loading_label.setMovie(self.loading_movie)

		# Add the two tabs: Detalle (existing UI) and Resumen (modularized)
		self._tabs.addTab(detalle, "Detalle")
		# instantiate modular resumen tab
		self._resumen_tab = ReporteInventarioSeguroResumenTab(self)
		self._tabs.addTab(self._resumen_tab, "Resumen")
		main_layout.addWidget(self._tabs)


	def load_data(self):
		# Query template: `{CLASE_EXPR}` será reemplazado en el worker
		query = """
	       SELECT
		       imh.[Codigo Alterno],
		       imh.[Código Original],
		       imh.[Producto],
		       imh.[Descripcion],
		       imh.[Rubro],
		       imh.[Linea],
			   imh.[Marca],
			   imh.[Categoria],
			   imh.[Nombre Bodega],
		       imh.[Stock],
		       imh.[Costo Dolares],
		       ISNULL(cb.[No_SigPro], '') AS [No. Sig Pro],
			   ISNULL(cb.[No_Scm], '') AS [No. Scm],
			   {CLASE_EXPR}
	       FROM [ComprasInternacionales].[dbo].[InventarioMensualHistorico] imh
	       LEFT JOIN [ComprasInternacionales].[kdx].[CodigoBodebaReporteInvSeguro] cb
		       ON imh.[Nombre Bodega] = cb.[NombreBodega]
			   LEFT JOIN [ComprasInternacionales].[kdx].[CategoríaProducto] cat
			       ON imh.[Codigo Alterno] = cat.[CodigoAlterno]
	       WHERE imh.[FECHA_CORTE] = (
		       SELECT MAX([FECHA_CORTE]) FROM [ComprasInternacionales].[dbo].[InventarioMensualHistorico]
	       )
	       AND imh.[Stock] > 0
	       ORDER BY imh.[STOCK] DESC;
	"""
		# Ejecutar carga en hilo para no bloquear la UI
		logger.info("Inicio de carga de inventario seguro")
		logger.debug("Query preview: %s", query.replace('\n', ' ')[:1000])
		self.load_button.setEnabled(False)
		self._show_loading_overlay()
		# Worker para ejecutar la consulta en background
		class _LoadWorker(QObject):
			finished = pyqtSignal(list, list)
			failed = pyqtSignal(str)

			def __init__(self, query):
				super().__init__()
				self.query = query

			def run(self):
				conn = None
				cursor = None
				try:
					conn = connect_db("compras_internacionales")
					cursor = conn.cursor()
					# Check whether imh.[Clase] exists; build query accordingly to avoid referencing a missing column
					try:
						cursor.execute("""
						SELECT 1
						FROM INFORMATION_SCHEMA.COLUMNS c
						WHERE c.TABLE_SCHEMA = 'dbo'
						  AND c.TABLE_NAME = 'InventarioMensualHistorico'
						  AND c.COLUMN_NAME = 'Clase'
						""")
						has_clase = cursor.fetchone() is not None
					except Exception:
						# If we can't check, assume column exists (we'll still handle errors)
						has_clase = True
					# Prepare final query replacing placeholder
					if has_clase:
						cl_expr = "ISNULL(cat.[Categoria], imh.[Clase]) AS [Clase]"
					else:
						cl_expr = "ISNULL(cat.[Categoria], '') AS [Clase]"
					final_query = self.query.replace('{CLASE_EXPR}', cl_expr)
					logger.debug("Ejecutando consulta (has_clase=%s)", has_clase)
					try:
						cursor.execute(final_query)
						executed_rows = cursor.fetchall()
						cols = [d[0] for d in cursor.description] if cursor.description else []
						logger.info("Consulta retornó %d filas; columnas: %s", len(executed_rows), cols)
						self.finished.emit(executed_rows, cols)
					except Exception as primary_err:
						# Propagate to outer except to be logged
						raise
				except Exception as e:
					logger.exception("Error al ejecutar consulta: %s", e)
					self.failed.emit(str(e))
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

		# Crear hilo y worker
		self._load_thread = QThread()
		self._load_worker = _LoadWorker(query)
		self._load_worker.moveToThread(self._load_thread)
		self._load_thread.started.connect(self._load_worker.run)
		self._load_worker.finished.connect(self._on_load_finished)
		self._load_worker.failed.connect(self._on_load_failed)
		self._load_worker.finished.connect(self._load_thread.quit)
		self._load_worker.failed.connect(self._load_thread.quit)
		self._load_thread.finished.connect(self._load_thread.deleteLater)
		self._load_thread.start()


	def _on_load_finished(self, rows, cols):
		# Poblar la tabla cuando termine la carga en background
		self._hide_loading_overlay()
		self.load_button.setEnabled(True)
		# Set columns/headers dynamically from cursor.description
		if cols:
			# Use column names as-is (no textual filter indicator appended)
			labels = list(cols)
			self.table.setColumnCount(len(cols))
			self.table.setHorizontalHeaderLabels(labels)
			# store current column indices for later use (editable column, lookups)
			self._col_indices = {c: i for i, c in enumerate(cols)}
		else:
			# Fallback: infer from first row
			if rows:
				self.table.setColumnCount(len(rows[0]))
				cols = [f"Col {i+1}" for i in range(len(rows[0]))]
				labels = list(cols)
				self.table.setHorizontalHeaderLabels(labels)
		# Populate table; suppress itemChanged while filling to avoid spurious saves
		self.table.setRowCount(len(rows))
		cl_idx = self._col_indices.get('Clase')
		self._suppress_item_changed = True
		for r, row in enumerate(rows):
			for c, value in enumerate(row):
				text = "" if value is None else str(value)
				item = QTableWidgetItem(text)
				# alignment by semantic (prefer name-based, but fallback to index heuristics)
				if cols is not None and len(cols) > c:
					name = cols[c]
					if any(k in name.lower() for k in ("codigo", "producto", "descripcion", "marca", "categoria")):
						item.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
					else:
						item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
				# Make only the 'Clase' column editable
				if cl_idx is not None and c == cl_idx:
					item.setFlags(item.flags() | Qt.ItemFlag.ItemIsEditable)
				else:
					item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
				self.table.setItem(r, c, item)
		self._suppress_item_changed = False
		# connect change handler (ensure not double-connected)
		try:
			self.table.itemChanged.disconnect(self._on_item_changed)
		except Exception:
			pass
		self.table.itemChanged.connect(self._on_item_changed)
		QMessageBox.information(self, "Listo", f"Cargados {len(rows)} fila(s).")
		# Ajustar columnas al contenido inicialmente, pero permitir redimensión manual
		try:
			self.table.resizeColumnsToContents()
		except Exception:
			pass
		# Re-apply filters if any active
		self._apply_filters()
		# Invalidate unique-values cache after a full reload
		self._invalidate_unique_cache()
		# Update resumen tab if present
		try:
			if hasattr(self, '_resumen_tab'):
				try:
					self._resumen_tab.update_summary(rows, cols)
				except Exception:
					logger.exception("Error actualizando la subpestaña Resumen")
		except Exception:
			pass

		# Log loaded columns and row count
		try:
			logger.info("Carga finalizada: %d filas; columnas: %s", len(rows), cols)
		except Exception:
			logger.exception("Error registrando información de carga")

	def _on_header_clicked(self, index: int) -> None:
		# Show a dropdown menu with unique values from the column for easy filtering
		header_item = self.table.horizontalHeaderItem(index)
		header_text = header_item.text() if header_item is not None else str(index)
		display_name = header_text.rstrip().rstrip('\u25BE').strip()
		# Collect unique values from the column. Use cached set if available.
		# Represent empty cells as the visible token "Vacio" so users can select and filter for empty values.
		if index in self._col_unique_cache:
			values = set(self._col_unique_cache[index])
		else:
			values = set()
			for r in range(self.table.rowCount()):
				it = self.table.item(r, index)
				if it is not None:
					val = it.text()
					display = "Vacio" if val == "" else val
					values.add(display)
			# store in cache (store strings)
			self._col_unique_cache[index] = set(values)
		values_list = sorted(values, key=lambda x: (x is None, x))
		# Build menu
		menu = QMenu(self)
		clear_action = menu.addAction("<Borrar filtro>")
		# Sorting actions
		sort_asc_action = menu.addAction("Ordenar ascendente")
		sort_desc_action = menu.addAction("Ordenar descendente")
		menu.addSeparator()

		# Ensure table sorting is enabled
		try:
			self.table.setSortingEnabled(True)
		except Exception:
			pass
		# Create a searchable list widget inside the menu using QWidgetAction
		container = QWidget()
		container_layout = QVBoxLayout(container)
		search = QLineEdit(container)
		search.setPlaceholderText("Buscar...")
		list_widget = QListWidget(container)
		for v in values_list:
			list_widget.addItem(QListWidgetItem(str(v)))
		container_layout.setContentsMargins(4, 4, 4, 4)
		container_layout.addWidget(search)
		container_layout.addWidget(list_widget)
		widget_action = QWidgetAction(menu)
		widget_action.setDefaultWidget(container)
		menu.addAction(widget_action)

		# Action to apply the current search text as a filter for all matching rows
		def _select_matches() -> None:
			st = search.text().strip()
			if st == "":
				# empty search -> clear this column filter
				if index in self._active_filters:
					del self._active_filters[index]
			else:
				self._active_filters[index] = st
			self._apply_filters()
			menu.close()

		select_action = menu.addAction("Todo")
		if select_action is not None:
			select_action.triggered.connect(_select_matches)

		# Connect sorting actions
		if sort_asc_action is not None:
			sort_asc_action.triggered.connect(lambda _, i=index: (self.table.sortItems(i, Qt.SortOrder.AscendingOrder), logger.info("Header sort asc: %s", display_name)))
		if sort_desc_action is not None:
			sort_desc_action.triggered.connect(lambda _, i=index: (self.table.sortItems(i, Qt.SortOrder.DescendingOrder), logger.info("Header sort desc: %s", display_name)))

		# filter function for the search box
		def _filter_list(text: str) -> None:
			needle = text.lower()
			for i in range(list_widget.count()):
				item = list_widget.item(i)
				if item is None:
					continue
				item.setHidden(needle not in item.text().lower())

		search.textChanged.connect(_filter_list)
		# When user clicks an item, store selection in a closure dict and close the menu
		selected_value: dict[str, Optional[str]] = { 'v': None }
		def _on_item_clicked(item: QListWidgetItem) -> None:
			selected_value['v'] = item.text()
			menu.close()

		list_widget.itemClicked.connect(_on_item_clicked)
		# default position: below the table (ensures `pos` is always defined)
		pos = self.table.mapToGlobal(QtCore.QPoint(0, self.table.height()))
		# Position menu next to the header section, choosing left/right based
		# on available screen space so the menu doesn't go off-screen.
		header = self.table.horizontalHeader()
		# Default fallback: below the table
		if header is None:
			pos = self.table.mapToGlobal(QtCore.QPoint(0, self.table.height()))
		else:
			try:
				# section left (relative to header viewport). Use sectionViewportPosition
				# so the position accounts for scrolling and visual order; fall back to sectionPosition.
				try:
					left = header.sectionViewportPosition(index)
				except Exception:
					left = header.sectionPosition(index)
				section_w = header.sectionSize(index)
				# global coordinates of the section's left
				global_left = header.mapToGlobal(QtCore.QPoint(left, 0)).x()
				# vertical position below header
				global_top = header.mapToGlobal(QtCore.QPoint(0, header.height())).y()
				menu_w = menu.sizeHint().width()
				# get the screen that contains the header section (handles multi-monitor)
				screen = QGuiApplication.screenAt(QtCore.QPoint(global_left, global_top))
				if screen is None:
					screen = QApplication.primaryScreen()
				if screen is not None:
					avail = screen.availableGeometry()
					# prefer showing menu to the right of the section
					if global_left + menu_w <= avail.x() + avail.width():
						menu_x = global_left
					else:
						# show aligned to the right edge of the section
						menu_x = global_left + max(0, section_w - menu_w)
					# clamp to screen
					menu_x = max(menu_x, avail.x())
				else:
					menu_x = global_left
				pos = QtCore.QPoint(int(menu_x), int(global_top))
			except Exception:
				pos = header.mapToGlobal(QtCore.QPoint(0, header.height()))
		selected_action = menu.exec(pos)
		# if the clear action was chosen via normal QAction, handle it
		if selected_action == clear_action:
			if index in self._active_filters:
				del self._active_filters[index]
			self._apply_filters()
			return
		# otherwise check the closure for selected value set by the list
		sel_text = selected_value.get('v')
		if sel_text is None:
			return
		self._active_filters[index] = sel_text
		self._apply_filters()

	def eventFilter(self, obj, event) -> bool:
		# Intercept mouse presses on the header viewport to decide whether
		# to open the filter menu (when clicking the small filter icon area)
		# or to perform sorting (when clicking elsewhere on the header).
		try:
			header = self.table.horizontalHeader()
			# If header is None, avoid attribute access and fall back to default handling
			if header is None:
				return super().eventFilter(obj, event)
			if obj is header.viewport() and event.type() == QtCore.QEvent.Type.MouseButtonPress:
				pos = event.pos()
				x = pos.x()
				col = None
				found_left = 0
				found_size = 0
				# determine which section was clicked by comparing viewport positions
				for c in range(self.table.columnCount()):
					try:
						left = header.sectionViewportPosition(c)
						size = header.sectionSize(c)
					except Exception:
						continue
					if left <= x < left + size:
						col = c
						found_left = left
						found_size = size
						break
				if col is None:
					return super().eventFilter(obj, event)
				# treat clicks within the right filter_icon_width pixels as filter-icon clicks
				try:
					ICON_WIDTH = getattr(header, 'filter_icon_width', 20)
				except Exception:
					ICON_WIDTH = 20
				# small margin (pixels) near the section divider reserved for resizing;
				# if the click is within this margin we should let the header handle
				# it (resize) rather than intercepting for sorting or filters.
				RESIZE_MARGIN = 6
				if x >= found_left + found_size - ICON_WIDTH:
					if x <= found_left + found_size - RESIZE_MARGIN:
						# open filter menu for this column (click inside filter area but
						# not inside the resize handle margin)
						self._on_header_clicked(col)
					else:
						# Click is within the resize handle margin; allow default handling
						return False
				else:
					# toggle sort for this column (preserve current behavior)
					try:
						curr_section = header.sortIndicatorSection()
						curr_order = header.sortIndicatorOrder()
						if curr_section == col:
							new_order = Qt.SortOrder.DescendingOrder if curr_order == Qt.SortOrder.AscendingOrder else Qt.SortOrder.AscendingOrder
						else:
							new_order = Qt.SortOrder.AscendingOrder
						self.table.sortItems(col, new_order)
						logger.info("Header clicked sort: col=%s order=%s", col, new_order)
					except Exception:
						logger.exception("Header sort failed for column %s", col)
				# consume the event
				return True
		except Exception:
			logger.exception("Error in header eventFilter")
		return super().eventFilter(obj, event)

	def _apply_filters(self) -> None:
		# If no filters, show all rows
		if not self._active_filters:
			for r in range(self.table.rowCount()):
				self.table.setRowHidden(r, False)
			return
		# Apply each active filter (AND across columns)
		for r in range(self.table.rowCount()):
			hide = False
			for col, pattern in self._active_filters.items():
				item = self.table.item(r, col)
				value = item.text() if item is not None else ""
				# Special-case the visible empty token: if the user selected "Vacio",
				# show only rows whose cell is empty.
				if pattern.lower() == "vacio":
					if value.strip() != "":
						hide = True
						break
				else:
					if pattern.lower() not in value.lower():
						hide = True
						break
			self.table.setRowHidden(r, hide)

	def _on_item_changed(self, item: QTableWidgetItem) -> None:
		# Called when a cell is edited by the user. We only act on changes to the
		# `Clase` column and only when not suppressed by programmatic updates.
		if self._suppress_item_changed:
			return
		if item is None:
			return
		col = item.column()
		# ensure we have column map
		cl_idx = self._col_indices.get('Clase')
		if cl_idx is None or col != cl_idx:
			return
		row = item.row()
		# Read new value early to allow duplicate suppression
		new_cat = item.text().strip()
		# suppress duplicate signals for same cell/value
		last = self._recent_edits.get((row, col))
		if last == new_cat:
			logger.debug("Duplicate Clase edit ignored: fila=%s, columna=%s", row, col)
			return
		# record this edit immediately to avoid re-entrancy
		self._recent_edits[(row, col)] = new_cat
		logger.info("Clase editada: fila=%s, columna=%s", row, col)
		# Invalidate unique-values cache for this column so header menus reflect the edit
		try:
			self._invalidate_unique_cache(col)
		except Exception:
			logger.exception("Error invalidando cache tras edicion en fila=%s col=%s", row, col)
		codigo_idx = self._col_indices.get('Codigo Alterno')
		if codigo_idx is None:
			logger.error("No se encontró la columna 'Codigo Alterno' para guardar la clase")
			return
		codigo_item = self.table.item(row, codigo_idx)
		if codigo_item is None:
			logger.error("Fila %s: no se encontró Codigo Alterno para guardar", row)
			return
		codigo = codigo_item.text().strip()
		if codigo == "":
			logger.error("Fila %s: Codigo Alterno vacío, no se puede guardar", row)
			QMessageBox.warning(self, "No guardado", "El Codigo Alterno está vacío; no se guardó la Clase.")
			return
		# new_cat already read above
		# run save in background thread
		class _SaveWorker(QObject):
			finished = pyqtSignal(str)
			failed = pyqtSignal(str)

			def __init__(self, codigo: str, categoria: str):
				super().__init__()
				self.codigo = codigo
				self.categoria = categoria

			def run(self):
				conn = None
				cursor = None
				try:
					conn = connect_db("compras_internacionales")
					cursor = conn.cursor()
					# Check existence
					cursor.execute(
						"SELECT COUNT(1) FROM [ComprasInternacionales].[kdx].[CategoríaProducto] WHERE [CodigoAlterno] = ?",
						(self.codigo,)
					)
					row = cursor.fetchone()
					exists = (row[0] if row and len(row) > 0 else 0) > 0
					if exists:
						action = 'UPDATE'
						cursor.execute(
							"UPDATE [ComprasInternacionales].[kdx].[CategoríaProducto] SET [Categoria] = ? WHERE [CodigoAlterno] = ?",
							(self.categoria, self.codigo)
						)
					else:
						action = 'INSERT'
						cursor.execute(
							"INSERT INTO [ComprasInternacionales].[kdx].[CategoríaProducto] ([CodigoAlterno],[Categoria]) VALUES (?, ?)",
							(self.codigo, self.categoria)
						)
					# Attempt commit and report affected rows
					affected = -1
					try:
						affected = cursor.rowcount if hasattr(cursor, 'rowcount') else -1
						conn.commit()
					except Exception as commit_err:
						logger.exception("Error al commitear cambio para %s: %s", self.codigo, commit_err)
						# re-raise so outer except will handle failed.emit
						raise
					self.finished.emit(f"Guardado: {self.codigo}, accion={action}, afectadas={affected}, categoria={self.categoria}")
				except Exception as e:
					logger.exception("Error guardando categoria: %s", e)
					self.failed.emit(str(e))
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

		# start worker (use local thread/worker variables so previous threads are not overwritten)
		thread = QThread()
		worker = _SaveWorker(codigo, new_cat)
		worker.moveToThread(thread)
		thread.started.connect(worker.run)
		def _on_save_finished(msg, r=row, c=col):
			try:
				self._recent_edits.pop((r, c), None)
			except Exception:
				logger.exception("Error limpiando recent_edits")
			logger.info(msg)

		def _on_save_failed(err, r=row, c=col):
			try:
				self._recent_edits.pop((r, c), None)
			except Exception:
				logger.exception("Error limpiando recent_edits (failed)")
			QMessageBox.critical(self, "Error", f"Error guardando categoria: {err}")

		worker.finished.connect(_on_save_finished)
		worker.finished.connect(thread.quit)
		worker.finished.connect(worker.deleteLater)
		worker.failed.connect(_on_save_failed)
		worker.failed.connect(thread.quit)
		worker.failed.connect(worker.deleteLater)
		thread.finished.connect(thread.deleteLater)
		# keep thread reference until it finishes
		self._active_threads.append(thread)
		def _thread_finished(t=thread):
			try:
				if t in self._active_threads:
					self._active_threads.remove(t)
					logger.debug("Thread finished and removed; active threads: %d", len(self._active_threads))
			except Exception:
				logger.exception("Error removing finished thread")
		thread.finished.connect(_thread_finished)
		logger.debug("Starting save thread for %s; active threads before start: %d", codigo, len(self._active_threads))
		thread.start()

	def _invalidate_unique_cache(self, col: Optional[int] = None) -> None:
		"""Invalidate cached unique-values for header menus.
		If col is None, clear the entire cache; otherwise remove that column entry.
		"""
		try:
			if col is None:
				self._col_unique_cache = {}
			else:
				self._col_unique_cache.pop(col, None)
		except Exception:
			logger.exception("Error invalidando cache de valores unicos para columna %s", col)

	def _on_cell_double_clicked(self, row: int, column: int) -> None:
		# If the user double-clicks a Clase cell, open a QLineEdit with a QCompleter
		cl_idx = self._col_indices.get('Clase')
		if cl_idx is None or column != cl_idx:
			return
		# current item/text
		item = self.table.item(row, column)
		current_text = "" if item is None else item.text()
		# build unique values from the Clase column (non-empty)
		values = set()
		for r in range(self.table.rowCount()):
			it = self.table.item(r, cl_idx)
			if it is None:
				continue
			v = it.text().strip()
			if v != "":
				values.add(v)
		values_list = sorted(values)
		completer = QCompleter(values_list, self)
		try:
			completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
		except Exception:
			pass
		# Set contains-style matching if available (use getattr to avoid Pylance attribute warnings)
		try:
			mf = getattr(Qt, 'MatchFlag', None)
			if mf is not None and hasattr(mf, 'MatchContains'):
				completer.setFilterMode(getattr(mf, 'MatchContains'))
			else:
				mc = getattr(Qt, 'MatchContains', None)
				if mc is not None:
					completer.setFilterMode(mc)
		except Exception:
			# If filter mode cannot be set, silently continue — completer still works with prefix matching
			pass
		# Create editor and place it in the cell
		editor = QLineEdit(self)
		editor.setText(current_text)
		editor.setCompleter(completer)
		editor.selectAll()
		self.table.setCellWidget(row, column, editor)
		editor.setFocus()

		def _finish_edit():
			new_text = editor.text().strip()
			# remove editor widget
			try:
				self.table.removeCellWidget(row, column)
			except Exception:
				pass
			editor.deleteLater()
			# update underlying item
			if item is None:
				itm = QTableWidgetItem(new_text)
				itm.setFlags(itm.flags() | Qt.ItemFlag.ItemIsEditable)
				itm.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
				self.table.setItem(row, column, itm)
				target = itm
			else:
				# avoid double-processing: suppress automatic handler and call manual save
				self._suppress_item_changed = True
				item.setText(new_text)
				self._suppress_item_changed = False
				target = item
			# trigger save flow
			try:
				self._on_item_changed(target)
			except Exception:
				logger.exception("Error saving cell edited via completer at row %s", row)

		editor.editingFinished.connect(_finish_edit)

	def _on_table_context_menu(self, pos: QtCore.QPoint) -> None:
		# Context menu to allow pasting clipboard value into multiple Clase cells
		menu = QMenu(self)
		cl_idx = self._col_indices.get('Clase')
		if cl_idx is None:
			# nothing specific to do
			menu.addAction("No actions available")
			menu.exec(self.table.mapToGlobal(pos))
			return
		# add paste action if user has a selection
		sel_ranges = self.table.selectedRanges()
		if not sel_ranges:
			menu.addAction("No rows selected")
			menu.exec(self.table.mapToGlobal(pos))
			return

		def _paste_to_selected() -> None:
			cb = QApplication.clipboard()
			if cb is None:
				return
			clipboard_text = cb.text()
			if clipboard_text is None:
				return
			# Apply clipboard_text to Clase column for all selected rows
			rows_to_save_local: list[tuple[str, str]] = []
			# ensure Codigo Alterno column index is resolved once
			codigo_idx_local = self._col_indices.get('Codigo Alterno')
			if codigo_idx_local is None:
				QMessageBox.information(self, "No se puede pegar", "No se encontró la columna 'Codigo Alterno'.")
				return
			for rng in sel_ranges:
				for r in range(rng.topRow(), rng.bottomRow() + 1):
					# ignore hidden rows (filters)
					if self.table.isRowHidden(r):
						continue
					# ensure item exists
					it = self.table.item(r, cl_idx)
					if it is None:
						it = QTableWidgetItem(clipboard_text)
						# keep alignment consistent
						it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
						it.setFlags(it.flags() | Qt.ItemFlag.ItemIsEditable)
						self.table.setItem(r, cl_idx, it)
					else:
						it.setText(clipboard_text)
					# collect for batch save (use Codigo Alterno)
					codigo_item = self.table.item(r, codigo_idx_local)
					if codigo_item is None:
						continue
					codigo = codigo_item.text().strip()
					if codigo == "":
						continue
					rows_to_save_local.append((codigo, clipboard_text))
			# reapply filters in case they depend on Clase
			# Invalidate cache and reapply filters in case they depend on Clase
			self._invalidate_unique_cache(cl_idx)
			self._apply_filters()
			# Start background batch save for all modified rows
			if rows_to_save_local:
				logger.info("Starting batch save for pasted rows: %d", len(rows_to_save_local))
				_start_batch_save(rows_to_save_local)

		paste_action = menu.addAction("Pegar portapapeles en selección (Clase)")
		if paste_action is not None:
			paste_action.triggered.connect(_paste_to_selected)

		# Paste only into selected items that belong to the Clase column
		def _paste_to_selected_cells() -> None:
			cb = QApplication.clipboard()
			if cb is None:
				return
			clipboard_text = cb.text()
			if clipboard_text is None:
				return
			items = self.table.selectedItems()
			if not items:
				QMessageBox.information(self, "Nada seleccionado", "No hay celdas seleccionadas.")
				return
			count = 0
			rows_to_save_local: list[tuple[str, str]] = []
			codigo_idx_local = self._col_indices.get('Codigo Alterno')
			if codigo_idx_local is None:
				QMessageBox.information(self, "No se puede pegar", "No se encontró la columna 'Codigo Alterno'.")
				return
			for it in items:
				if it.column() == cl_idx:
					r = it.row()
					if self.table.isRowHidden(r):
						continue
					it.setText(clipboard_text)
					codigo_item = self.table.item(r, codigo_idx_local)
					if codigo_item is None:
						continue
					codigo = codigo_item.text().strip()
					if codigo == "":
						continue
					rows_to_save_local.append((codigo, clipboard_text))
					count += 1
			logger.info("Pasted into %d Clase cells", count)
			# Invalidate cache and trigger batch save if needed
			self._invalidate_unique_cache(cl_idx)
			if rows_to_save_local:
				logger.info("Starting batch save for pasted cells: %d", len(rows_to_save_local))
				_start_batch_save(rows_to_save_local)

		paste_cells_action = menu.addAction("Pegar en celdas Clase seleccionadas")
		if paste_cells_action is not None:
			paste_cells_action.triggered.connect(_paste_to_selected_cells)

		# Synchronous save action for debugging/verifying writes immediately
		def _save_selection_now() -> None:
			# Collect only the Clase cells the user actually selected (prevent large implicit ranges)
			rows_to_save: list[tuple[str, str]] = []
			codigo_idx = self._col_indices.get('Codigo Alterno')
			if codigo_idx is None:
				QMessageBox.information(self, "No se puede guardar", "No se encontró la columna 'Codigo Alterno'.")
				return
			# Prefer selectedItems (actual selected cells). This respects user selection even with filters.
			items = self.table.selectedItems()
			if items:
				rows_seen: set[int] = set()
				for it in items:
					if it.column() != cl_idx:
						continue
					r = it.row()
					# ignore hidden rows
					if self.table.isRowHidden(r):
						continue
					if r in rows_seen:
						continue
					rows_seen.add(r)
					codigo_item = self.table.item(r, codigo_idx)
					if codigo_item is None:
						continue
					codigo = codigo_item.text().strip()
					if codigo == "":
						continue
					categoria = it.text().strip()
					rows_to_save.append((codigo, categoria))
			else:
				# Fallback: use selectedRanges but only include visible rows
				for rng in sel_ranges:
					for r in range(rng.topRow(), rng.bottomRow() + 1):
						if self.table.isRowHidden(r):
							continue
						codigo_item = self.table.item(r, codigo_idx)
						cl_item = self.table.item(r, cl_idx)
						if codigo_item is None or cl_item is None:
							continue
						codigo = codigo_item.text().strip()
						categoria = cl_item.text().strip()
						if codigo == "":
							continue
						rows_to_save.append((codigo, categoria))

			logger.info("Iniciando guardado síncrono; filas a procesar=%d (seleccionadas visibles)", len(rows_to_save))
			if not rows_to_save:
				QMessageBox.information(self, "Nada para guardar", "No se encontraron filas válidas para guardar.")
				return

			# Perform DB writes in a single connection/transaction for verification
			conn = None
			cursor = None
			success = 0
			failed = 0
			try:
				conn = connect_db("compras_internacionales")
				cursor = conn.cursor()
				for codigo, categoria in rows_to_save:
					try:
						cursor.execute(
							"SELECT COUNT(1) FROM [ComprasInternacionales].[kdx].[CategoríaProducto] WHERE [CodigoAlterno] = ?",
							(codigo,)
						)
						row = cursor.fetchone()
						exists = (row[0] if row and len(row) > 0 else 0) > 0
						if exists:
							action = 'UPDATE'
							cursor.execute(
								"UPDATE [ComprasInternacionales].[kdx].[CategoríaProducto] SET [Categoria] = ? WHERE [CodigoAlterno] = ?",
								(categoria, codigo)
							)
						else:
							action = 'INSERT'
							cursor.execute(
								"INSERT INTO [ComprasInternacionales].[kdx].[CategoríaProducto] ([CodigoAlterno],[Categoria]) VALUES (?, ?)",
								(codigo, categoria)
							)
						affected = cursor.rowcount if hasattr(cursor, 'rowcount') else -1
						logger.info("Guardado síncrono: codigo=%s, accion=%s, afectadas=%s, categoria=%s", codigo, action, affected, categoria)
						success += 1
					except Exception:
						logger.exception("Fallo guardando %s synchronously", codigo)
						failed += 1
				# commit once
				try:
					conn.commit()
					logger.info("Commit realizado (sincrono)")
				except Exception as commit_err:
					logger.exception("Error commiteando saves síncronos: %s", commit_err)
			except Exception:
				logger.exception("Error durante guardado síncrono")
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

			QMessageBox.information(self, "Guardado síncrono", f"Guardadas: {success}, fallidas: {failed}")

		save_now_action = menu.addAction("Guardar selección ahora (sin hilo)")
		if save_now_action is not None:
			save_now_action.triggered.connect(_save_selection_now)

		# Background batch save helper
		def _start_batch_save(rows: list[tuple[str, str]]) -> None:
			class _BatchSaveWorker(QObject):
				finished = pyqtSignal(int, int)
				failed = pyqtSignal(str)

				def __init__(self, rows_to_process: list[tuple[str, str]]):
					super().__init__()
					self.rows = rows_to_process

				def run(self):
					conn = None
					cursor = None
					success = 0
					failed = 0
					try:
						conn = connect_db("compras_internacionales")
						cursor = conn.cursor()
						for codigo, categoria in self.rows:
							try:
								cursor.execute(
									"SELECT COUNT(1) FROM [ComprasInternacionales].[kdx].[CategoríaProducto] WHERE [CodigoAlterno] = ?",
									(codigo,)
								)
								row = cursor.fetchone()
								exists = (row[0] if row and len(row) > 0 else 0) > 0
								if exists:
									cursor.execute(
										"UPDATE [ComprasInternacionales].[kdx].[CategoríaProducto] SET [Categoria] = ? WHERE [CodigoAlterno] = ?",
										(categoria, codigo)
									)
								else:
									cursor.execute(
										"INSERT INTO [ComprasInternacionales].[kdx].[CategoríaProducto] ([CodigoAlterno],[Categoria]) VALUES (?, ?)",
										(codigo, categoria)
									)
								success += 1
							except Exception:
								logger.exception("Batch save failed for %s", codigo)
								failed += 1
						# commit once
						try:
							conn.commit()
							logger.info("Batch commit completed; success=%d failed=%d", success, failed)
						except Exception:
							logger.exception("Error committing batch save")
					except Exception as e:
						logger.exception("Error in batch save worker: %s", e)
						self.failed.emit(str(e))
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
						self.finished.emit(success, failed)

			worker = _BatchSaveWorker(rows)
			thread = QThread()
			worker.moveToThread(thread)
			thread.started.connect(worker.run)
			def _on_finished(success_count, failed_count):
				logger.info("Batch save finished: success=%d failed=%d", success_count, failed_count)
				thread.quit()
				thread.wait(1000)
			worker.finished.connect(_on_finished)
			worker.failed.connect(lambda err: logger.error("Batch save failed: %s", err))
			worker.failed.connect(thread.quit)
			thread.finished.connect(thread.deleteLater)
			self._active_threads.append(thread)
			def _thread_done(t=thread):
				try:
					if t in self._active_threads:
						self._active_threads.remove(t)
				except Exception:
					logger.exception("Error removing batch thread")
			thread.finished.connect(_thread_done)
			thread.start()
		menu.exec(self.table.mapToGlobal(pos))

	def _on_load_failed(self, error_message):
		self._hide_loading_overlay()
		self.load_button.setEnabled(True)
		logger.error("Carga fallida: %s", error_message)
		QMessageBox.critical(self, "Error", f"Error al cargar datos: {error_message}")

	def _show_loading_overlay(self):
		parent = self
		label = self.loading_label
		label.resize(180, 180)
		parent_rect = parent.rect()
		label.move(
			(parent_rect.width() - label.width()) // 2,
			(parent_rect.height() - label.height()) // 2,
		)
		label.raise_()
		label.setVisible(True)
		self.loading_movie.start()
		QApplication.processEvents()

	def _hide_loading_overlay(self):
		try:
			self.loading_movie.stop()
		except Exception:
			pass
		self.loading_label.setVisible(False)

	def closeEvent(self, event):
		# Attempt to stop any active threads gracefully before the widget is destroyed.
		for t in list(self._active_threads):
			try:
				# ask thread to quit and wait briefly
				t.quit()
				# wait up to 2s for thread to finish
				t.wait(2000)
			except Exception:
				logger.exception("Error stopping thread on close")
		super().closeEvent(event)

