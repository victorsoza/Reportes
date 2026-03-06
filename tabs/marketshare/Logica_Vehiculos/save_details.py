"""Módulo encargado de la lógica del botón 'Guardar Detalles'.

Este módulo expone una función `handle_save_details(tab, db_key)` que
centraliza el comportamiento de mostrar/ocultar el panel lateral de
detalles. Diseñado para ser llamado desde `marketshare_vehiculos_tab`.
"""
from typing import Any
from db_config import connect_db

try:
    # importaciones necesarias para el autocompletado
    from PyQt6.QtWidgets import QCompleter
    from PyQt6.QtCore import QStringListModel, Qt
except Exception:
    QCompleter = None
    QStringListModel = None
    Qt = None

# intentar importar detect_models del módulo local model_detection
try:
    from .model_detection import detect_models  # type: ignore
except Exception:
    try:
        from model_detection import detect_models  # type: ignore
    except Exception:
        detect_models = None


def _capture_table_snapshot(tab: Any, cols: tuple[str, str, str]) -> list[tuple[str, str, str]]:
    """Captura una tupla (MODELO, MARCA, CATEGORIA) por fila para comparación posterior."""
    snap: list[tuple[str, str, str]] = []
    try:
        # localizar índices
        idxs = {}
        for i in range(tab.table.columnCount()):
            hi = tab.table.horizontalHeaderItem(i)
            if hi is None:
                continue
            name = hi.text().strip().upper()
            if name == 'MODELO' and 'MODELO' in cols:
                idxs['MODELO'] = i
            elif name == 'MARCA' and 'MARCA' in cols:
                idxs['MARCA'] = i
            elif name == 'CATEGORIA' and 'CATEGORIA' in cols:
                idxs['CATEGORIA'] = i
        # default missing indices to None
        total = tab.table.rowCount()
        for r in range(total):
            try:
                m = ''
                ma = ''
                c = ''
                try:
                    if 'MODELO' in idxs:
                        it = tab.table.item(r, idxs['MODELO'])
                        m = it.text().strip() if (it is not None and it.text() is not None) else ''
                except Exception:
                    m = ''
                try:
                    if 'MARCA' in idxs:
                        it = tab.table.item(r, idxs['MARCA'])
                        ma = it.text().strip() if (it is not None and it.text() is not None) else ''
                except Exception:
                    ma = ''
                try:
                    if 'CATEGORIA' in idxs:
                        it = tab.table.item(r, idxs['CATEGORIA'])
                        c = it.text().strip() if (it is not None and it.text() is not None) else ''
                except Exception:
                    c = ''
                snap.append((m, ma, c))
            except Exception:
                snap.append(('', '', ''))
        return snap
    except Exception:
        return []


def _compare_snapshots(before: list[tuple[str, str, str]] | None, after: list[tuple[str, str, str]] | None) -> list[int]:
    """Devuelve índices de filas cuyo contenido cambió entre before y after."""
    if before is None or after is None:
        return []
    changed: list[int] = []
    try:
        rows = min(len(before), len(after))
        for i in range(rows):
            if before[i] != after[i]:
                changed.append(i)
    except Exception:
        pass
    return changed


def _highlight_rows(tab: Any, rows: list[int]) -> None:
    """Resalta las filas indicadas con color celeste claro."""
    if not rows:
        return
    try:
        from PyQt6.QtGui import QColor
        from PyQt6.QtWidgets import QTableWidgetItem
    except Exception:
        return
    try:
        color = QColor(230, 245, 255)
        col_count = tab.table.columnCount()
        for r in rows:
            try:
                for c in range(col_count):
                    try:
                        it = tab.table.item(r, c)
                        if it is None:
                            it = QTableWidgetItem("")
                            tab.table.setItem(r, c, it)
                        try:
                            it.setBackground(color)
                        except Exception:
                            pass
                    except Exception:
                        continue
            except Exception:
                continue
    except Exception:
        try:
            tab.logger.debug("No se pudo resaltar filas modificadas")
        except Exception:
            pass


def _fill_marca_for_rows(tab: Any, rows: list[int]) -> None:
    """Intenta completar la columna MARCA consultando la tabla MarcaModeloMarketshare para cada fila indicada."""
    if not rows:
        return
    try:
        conn = connect_db('compras_internacionales')
        cursor = conn.cursor()
    except Exception:
        return
    try:
        # localizar índices
        idx_modelo = None
        idx_marca = None
        idx_vin = None
        idx_datos = None
        for i in range(tab.table.columnCount()):
            hi = tab.table.horizontalHeaderItem(i)
            if hi is None:
                continue
            name = hi.text().strip().upper()
            if name == 'MODELO' and idx_modelo is None:
                idx_modelo = i
            elif name == 'MARCA' and idx_marca is None:
                idx_marca = i
            elif name == 'VIN' and idx_vin is None:
                idx_vin = i
            elif name in ('DATOS', 'OTROS') and idx_datos is None:
                idx_datos = i
        if idx_modelo is None or idx_marca is None:
            return

        for r in rows:
            try:
                it = tab.table.item(r, idx_modelo)
                modelo_text = it.text().strip() if (it is not None and it.text() is not None) else ''
                if not modelo_text:
                    continue
                # Skip if MARCA already present and not N/A
                try:
                    ma_it = tab.table.item(r, idx_marca)
                    ma_text = ma_it.text().strip() if (ma_it is not None and ma_it.text() is not None) else ''
                    if ma_text and ma_text.strip().upper() != 'N/A':
                        continue
                except Exception:
                    pass

                # Buscar marca en BD: primero en las tablas donde guardamos desde el panel
                marca_found = None
                # 1) Si la fila contiene VIN, buscar en VinMarketshare por VIN
                try:
                    if idx_vin is not None:
                        vin_it = tab.table.item(r, idx_vin)
                        vin_val = vin_it.text().strip() if (vin_it is not None and vin_it.text() is not None) else ''
                        if vin_val:
                            try:
                                cursor.execute("SELECT TOP 1 [MARCA] FROM [ComprasInternacionales].[kdx].[VinMarketshare] WHERE [VIN]=?", (vin_val,))
                                row = cursor.fetchone()
                                if row and row[0] is not None:
                                    marca_found = str(row[0]).strip()
                            except Exception:
                                pass
                except Exception:
                    pass

                # 2) Si no encontrada y existe campo DATOS, buscar en la tabla MarcaModeloDatosMarketshareVehículos por DATOS exacto
                try:
                    if marca_found is None and idx_datos is not None:
                        datos_it = tab.table.item(r, idx_datos)
                        datos_val = datos_it.text().strip() if (datos_it is not None and datos_it.text() is not None) else ''
                        if datos_val:
                            try:
                                cursor.execute(
                                    "SELECT TOP 1 [MARCA] FROM [ComprasInternacionales].[kdx].[MarcaModeloDatosMarketshareVehículos] WHERE [DATOS]=?",
                                    (datos_val,)
                                )
                                row = cursor.fetchone()
                                if row and row[0] is not None:
                                    marca_found = str(row[0]).strip()
                            except Exception:
                                pass
                except Exception:
                    pass

                # 3) Fallback: buscar en tabla de mapeo por MODELO (LIKE o exacto)
                if marca_found is None:
                    try:
                        sql = "SELECT TOP 1 [MARCA] FROM [ComprasInternacionales].[kdx].[MarcaModeloMarketshare] WHERE [MODELO] LIKE ?"
                        cursor.execute(sql, (f"%{modelo_text}%",))
                        row = cursor.fetchone()
                        if row and row[0] is not None:
                            marca_found = str(row[0]).strip()
                    except Exception:
                        try:
                            cursor.execute("SELECT TOP 1 [MARCA] FROM [ComprasInternacionales].[kdx].[MarcaModeloMarketshare] WHERE [MODELO]=?", (modelo_text,))
                            row = cursor.fetchone()
                            if row and row[0] is not None:
                                marca_found = str(row[0]).strip()
                        except Exception:
                            pass

                if marca_found:
                    try:
                        from PyQt6.QtWidgets import QTableWidgetItem
                        tab.table.setItem(r, idx_marca, QTableWidgetItem(marca_found))
                    except Exception:
                        try:
                            tab.logger.debug("No se pudo asignar MARCA en fila %s", r)
                        except Exception:
                            pass
            except Exception:
                try:
                    tab.logger.exception("Error rellenando MARCA fila %s", r)
                except Exception:
                    pass
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _fill_categoria_for_rows(tab: Any, rows: list[int]) -> None:
    """Intenta completar la columna CATEGORIA consultando la tabla CategoriaMarketshare para cada fila indicada."""
    if not rows:
        return
    try:
        conn = connect_db('compras_internacionales')
        cursor = conn.cursor()
    except Exception:
        return
    try:
        # localizar índices
        idx_modelo = None
        idx_categoria = None
        idx_vin = None
        idx_datos = None
        for i in range(tab.table.columnCount()):
            hi = tab.table.horizontalHeaderItem(i)
            if hi is None:
                continue
            name = hi.text().strip().upper()
            if name == 'MODELO' and idx_modelo is None:
                idx_modelo = i
            elif name == 'CATEGORIA' and idx_categoria is None:
                idx_categoria = i
            elif name == 'VIN' and idx_vin is None:
                idx_vin = i
            elif name in ('DATOS', 'OTROS') and idx_datos is None:
                idx_datos = i
        if idx_modelo is None or idx_categoria is None:
            return

        for r in rows:
            try:
                it = tab.table.item(r, idx_modelo)
                modelo_text = it.text().strip() if (it is not None and it.text() is not None) else ''
                if not modelo_text:
                    continue
                # Skip if CATEGORIA already present and not N/A
                try:
                    c_it = tab.table.item(r, idx_categoria)
                    c_text = c_it.text().strip() if (c_it is not None and c_it.text() is not None) else ''
                    if c_text and c_text.strip().upper() != 'N/A':
                        continue
                except Exception:
                    pass

                categoria_found = None
                # 1) Si VIN disponible, intentar recuperar CATEGORIA desde VinMarketshare
                try:
                    if idx_vin is not None:
                        vin_it = tab.table.item(r, idx_vin)
                        vin_val = vin_it.text().strip() if (vin_it is not None and vin_it.text() is not None) else ''
                        if vin_val:
                            try:
                                cursor.execute("SELECT TOP 1 [CATEGORIA] FROM [ComprasInternacionales].[kdx].[VinMarketshare] WHERE [VIN]=?", (vin_val,))
                                row = cursor.fetchone()
                                if row and row[0] is not None:
                                    categoria_found = str(row[0]).strip()
                            except Exception:
                                pass
                except Exception:
                    pass

                # 2) Si no encontrada y DATOS disponible, intentar tabla MarcaModeloDatosMarketshareVehículos
                try:
                    if categoria_found is None and idx_datos is not None:
                        datos_it = tab.table.item(r, idx_datos)
                        datos_val = datos_it.text().strip() if (datos_it is not None and datos_it.text() is not None) else ''
                        if datos_val:
                            try:
                                cursor.execute(
                                    "SELECT TOP 1 [CATEGORIA] FROM [ComprasInternacionales].[kdx].[MarcaModeloDatosMarketshareVehículos] WHERE [DATOS]=?",
                                    (datos_val,)
                                )
                                row = cursor.fetchone()
                                if row and row[0] is not None:
                                    categoria_found = str(row[0]).strip()
                            except Exception:
                                pass
                except Exception:
                    pass

                # 3) Fallback: tabla de mapeo por MODELO
                if categoria_found is None:
                    try:
                        sql = "SELECT TOP 1 [CATEGORIA] FROM [ComprasInternacionales].[kdx].[CategoriaMarketshare] WHERE [MODELO] LIKE ?"
                        cursor.execute(sql, (f"%{modelo_text}%",))
                        row = cursor.fetchone()
                        if row and row[0] is not None:
                            categoria_found = str(row[0]).strip()
                    except Exception:
                        try:
                            cursor.execute("SELECT TOP 1 [CATEGORIA] FROM [ComprasInternacionales].[kdx].[CategoriaMarketshare] WHERE [MODELO]=?", (modelo_text,))
                            row = cursor.fetchone()
                            if row and row[0] is not None:
                                categoria_found = str(row[0]).strip()
                        except Exception:
                            pass

                if categoria_found:
                    try:
                        from PyQt6.QtWidgets import QTableWidgetItem
                        tab.table.setItem(r, idx_categoria, QTableWidgetItem(categoria_found))
                    except Exception:
                        try:
                            tab.logger.debug("No se pudo asignar CATEGORIA en fila %s", r)
                        except Exception:
                            pass
            except Exception:
                try:
                    tab.logger.exception("Error rellenando CATEGORIA fila %s", r)
                except Exception:
                    pass
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def handle_save_details(tab: Any, db_key: str = 'compras_internacionales') -> None:
    """Alterna la visualización del panel lateral de detalles.

    `tab` debe ser la instancia de `MarketshareVehiculosTab` (o similar)
    que expone los métodos `_create_details_panel`, `_show_details_panel`
    y `_hide_details_panel`.
    """
    # Aceptar señales booleanas (clicks pueden enviar True)
    if isinstance(db_key, bool):
        db_key = 'compras_internacionales'
    try:
        # Usar las funciones internas del módulo en lugar de métodos del tab
        if getattr(tab, '_details_panel', None) is None:
            try:
                create_details_panel(tab)
            except Exception:
                try:
                    tab.logger.exception("No se pudo crear el panel de detalles")
                except Exception:
                    pass

        dlg = getattr(tab, '_details_panel', None)
        if dlg is not None and dlg.isVisible():
            try:
                hide_details_panel(tab)
            except Exception:
                pass
        else:
            try:
                show_details_panel(tab)
            except Exception:
                pass
    except Exception as e:
        try:
            tab.logger.exception("Error mostrando panel de detalles en save_details: %s", e)
        except Exception:
            pass


def create_details_panel(tab: Any) -> None:
    """Crea el panel lateral derecho y lo asocia a `tab._details_panel`.

    Reproduce la lógica UI que antes estaba en el Tab; se espera que `tab`
    sea una instancia que expone `logger` y métodos de PyQt6.
    """
    try:
        # Importar localmente para evitar fallos en contextos sin GUI
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QTabWidget, QWidget, QTextEdit, QMessageBox, QLineEdit, QFormLayout
        from PyQt6.QtGui import QFont
        from PyQt6 import QtCore
        from PyQt6.QtGui import QGuiApplication
        from PyQt6.QtWidgets import QApplication
    except Exception:
        try:
            tab.logger.debug("create_details_panel: PyQt6 no disponible")
        except Exception:
            pass
        return

    try:
        parent_win = tab.window() or tab
        dlg = QDialog(parent_win)
        dlg.setObjectName("marketshare_details_panel")
        dlg.setWindowFlags(QtCore.Qt.WindowType.Tool | QtCore.Qt.WindowType.FramelessWindowHint)
        dlg.setModal(False)
        panel_width = 360
        dlg.setFixedWidth(panel_width)
        try:
            pw = parent_win.geometry()
            dlg.setFixedHeight(pw.height())
        except Exception:
            pass

        layout = QVBoxLayout(dlg)
        header_layout = QHBoxLayout()
        title = QLabel("Detalles")
        title.setFont(QFont("Arial", 11))
        header_layout.addWidget(title)
        header_layout.addStretch()
        close_btn = QPushButton("Cerrar")
        # conectar cierre a la función hide del módulo
        close_btn.clicked.connect(lambda: hide_details_panel(tab))
        header_layout.addWidget(close_btn)
        layout.addLayout(header_layout)

        # Contenido: pestañas para acciones dentro del panel
        tab_widget = QTabWidget(dlg)

        # Pestañas con formularios de texto (solo inputs, el usuario escribirá los datos)
        tab_vin = QWidget()
        vin_vlayout = QVBoxLayout(tab_vin)
        vin_form = QFormLayout()
        vin_form.setLabelAlignment(QtCore.Qt.AlignmentFlag.AlignLeft)
        marca_input = QLineEdit(tab_vin)
        marca_input.setPlaceholderText("Marca")
        modelo_input = QLineEdit(tab_vin)
        modelo_input.setPlaceholderText("Modelo")
        vin_input = QLineEdit(tab_vin)
        vin_input.setPlaceholderText("VIN")
        vin_form.addRow("Marca:", marca_input)
        vin_form.addRow("Modelo:", modelo_input)
        vin_form.addRow("VIN:", vin_input)
        # Botón Guardar para la pestaña VIN: añadir como fila del formulario y centrar
        try:
            vin_save_btn = QPushButton("Guardar")
            vin_save_btn.clicked.connect(lambda: save_vin_details(tab))
            btn_container = QWidget()
            btn_h = QHBoxLayout(btn_container)
            btn_h.setContentsMargins(0, 0, 0, 0)
            btn_h.addStretch()
            btn_h.addWidget(vin_save_btn)
            btn_h.addStretch()
            vin_form.addRow('', btn_container)
            try:
                tab._details_vin_save_btn = vin_save_btn
            except Exception:
                pass
        except Exception:
            try:
                tab.logger.debug("No se pudo crear botón Guardar (VIN)")
            except Exception:
                pass
        vin_vlayout.addLayout(vin_form)

        tab_otros = QWidget()
        otros_vlayout = QVBoxLayout(tab_otros)
        otros_form = QFormLayout()
        otros_form.setLabelAlignment(QtCore.Qt.AlignmentFlag.AlignLeft)
        marca_o_input = QLineEdit(tab_otros)
        marca_o_input.setPlaceholderText("Marca")
        modelo_o_input = QLineEdit(tab_otros)
        modelo_o_input.setPlaceholderText("Modelo")
        otros_input = QLineEdit(tab_otros)
        otros_input.setPlaceholderText("Otros")
        otros_form.addRow("Marca:", marca_o_input)
        otros_form.addRow("Modelo:", modelo_o_input)
        otros_form.addRow("Otros:", otros_input)
        # Botón Guardar para la pestaña Otros: añadir como fila del formulario y centrar
        try:
            otros_save_btn = QPushButton("Guardar")
            otros_save_btn.clicked.connect(lambda: save_otros_details(tab))
            otros_btn_container = QWidget()
            otros_btn_h = QHBoxLayout(otros_btn_container)
            otros_btn_h.setContentsMargins(0, 0, 0, 0)
            otros_btn_h.addStretch()
            otros_btn_h.addWidget(otros_save_btn)
            otros_btn_h.addStretch()
            otros_form.addRow('', otros_btn_container)
            try:
                tab._details_otros_save_btn = otros_save_btn
            except Exception:
                pass
        except Exception:
            try:
                tab.logger.debug("No se pudo crear botón Guardar (Otros)")
            except Exception:
                pass
        otros_vlayout.addLayout(otros_form)

        # pestañas con títulos claros
        tab_widget.addTab(tab_vin, "Guardar por VIN")
        tab_widget.addTab(tab_otros, "Guardar por Otros")

        # Guardar referencias de los inputs en el tab para uso futuro
        try:
            tab._details_vin_marca_input = marca_input
            tab._details_vin_modelo_input = modelo_input
            tab._details_vin_vin_input = vin_input
            tab._details_otros_marca_input = marca_o_input
            tab._details_otros_modelo_input = modelo_o_input
            tab._details_otros_otros_input = otros_input
        except Exception:
            pass

        # Conectar señales para actualizar los autocompletados de VIN y de Otros
        try:
            if QCompleter is not None and QStringListModel is not None and Qt is not None:
                # Crear un QCompleter persistente y su modelo para VIN
                try:
                    completer_model = QStringListModel([], vin_input)
                    completer = QCompleter(completer_model, vin_input)
                    completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
                    try:
                        completer.setFilterMode(Qt.MatchFlag.MatchContains)
                    except Exception:
                        pass
                    try:
                        completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
                    except Exception:
                        pass
                    vin_input.setCompleter(completer)
                    # Guardar referencias para uso en update_vin_completer
                    tab._details_vin_completer = completer
                    tab._details_vin_completer_model = completer_model
                except Exception:
                    try:
                        tab.logger.debug("No se pudo crear QCompleter persistente")
                    except Exception:
                        pass

                # Crear un QCompleter persistente y su modelo para Otros
                try:
                    otros_completer_model = QStringListModel([], otros_input)
                    otros_completer = QCompleter(otros_completer_model, otros_input)
                    otros_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
                    try:
                        otros_completer.setFilterMode(Qt.MatchFlag.MatchContains)
                    except Exception:
                        pass
                    try:
                        otros_completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
                    except Exception:
                        pass
                    otros_input.setCompleter(otros_completer)
                    tab._details_otros_completer = otros_completer
                    tab._details_otros_completer_model = otros_completer_model
                except Exception:
                    try:
                        tab.logger.debug("No se pudo crear QCompleter persistente para 'Otros'")
                    except Exception:
                        pass

                # Conectar señales a las funciones de actualización correspondientes
                marca_input.textChanged.connect(lambda _text='': update_vin_completer(tab))
                modelo_input.textChanged.connect(lambda _text='': update_vin_completer(tab))
                vin_input.textEdited.connect(lambda _text='': update_vin_completer(tab))

                marca_o_input.textChanged.connect(lambda _text='': update_otros_completer(tab))
                modelo_o_input.textChanged.connect(lambda _text='': update_otros_completer(tab))
                otros_input.textEdited.connect(lambda _text='': update_otros_completer(tab))
        except Exception:
            try:
                tab.logger.debug("No se pudieron conectar señales de autocompletado VIN/Otros")
            except Exception:
                pass

        layout.addWidget(tab_widget)

        # guardar referencias en el tab
        try:
            tab._details_panel = dlg
            tab._details_panel_tabwidget = tab_widget
        except Exception:
            pass
    except Exception as e:
        try:
            tab.logger.exception("Error creando panel de detalles en módulo: %s", e)
        except Exception:
            pass


def show_details_panel(tab: Any) -> None:
    try:
        dlg = getattr(tab, '_details_panel', None)
        if dlg is None:
            create_details_panel(tab)
            dlg = getattr(tab, '_details_panel', None)
            if dlg is None:
                return

        parent_win = tab.window() or tab
        try:
            mw_geo = parent_win.geometry()
            panel_w = dlg.width()
            x = mw_geo.x() + mw_geo.width() - panel_w
            y = mw_geo.y()
            dlg.move(max(x, 0), max(y, 0))
        except Exception:
            # fallback simple: intentar usar pantalla primaria o dimensiones seguras
            try:
                from PyQt6.QtGui import QGuiApplication
                from PyQt6.QtWidgets import QApplication
                screen = QGuiApplication.primaryScreen() if QGuiApplication is not None else None
                if screen is not None:
                    avail = screen.availableGeometry()
                    dlg.move(max(avail.x() + avail.width() - dlg.width(), 0), avail.y())
                else:
                    dlg.move(0, 0)
            except Exception:
                try:
                    dlg.move(0, 0)
                except Exception:
                    pass

        dlg.show()
    except Exception:
        try:
            tab.logger.exception("Error mostrando panel de detalles desde módulo")
        except Exception:
            pass


def hide_details_panel(tab: Any) -> None:
    try:
        dlg = getattr(tab, '_details_panel', None)
        if dlg is not None:
            try:
                dlg.hide()
            except Exception:
                try:
                    tab.logger.exception("Error ocultando panel de detalles (hide)")
                except Exception:
                    pass
    except Exception:
        try:
            tab.logger.exception("Error ocultando panel de detalles desde módulo")
        except Exception:
            pass


def update_vin_completer(tab: Any, db_key: str = 'compras_internacionales') -> None:
    """Carga sugerencias de VIN desde la tabla SQL y aplica QCompleter al campo VIN.

    Si hay texto en los campos Marca/Modelo, se usan como filtro (LIKE).
    """
    try:
        if QCompleter is None or QStringListModel is None or Qt is None:
            return
    except Exception:
        return
    try:
        vin_input = getattr(tab, '_details_vin_vin_input', None)
        if vin_input is None:
            return

        marca = ""
        modelo = ""
        try:
            widget = getattr(tab, '_details_vin_marca_input', None)
            if widget is not None:
                marca = widget.text().strip()
        except Exception:
            marca = ""
        try:
            widget = getattr(tab, '_details_vin_modelo_input', None)
            if widget is not None:
                modelo = widget.text().strip()
        except Exception:
            modelo = ""

        vins: list[str] = []
        try:
            conn = connect_db(db_key)
            cursor = conn.cursor()
            # Construir query con filtros opcionales
            sql = "SELECT DISTINCT [VIN] FROM [ComprasInternacionales].[kdx].[VinMarketshare] WHERE 1=1"
            params: list[str] = []
            if marca:
                sql += " AND [MARCA] LIKE ?"
                params.append(f"%{marca}%")
            if modelo:
                sql += " AND [MODELO] LIKE ?"
                params.append(f"%{modelo}%")
            sql += " ORDER BY [VIN]"
            try:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
                for r in rows:
                    try:
                        v = r[0]
                        if v is None:
                            continue
                        s = str(v).strip()
                        if s:
                            vins.append(s)
                    except Exception:
                        continue
            except Exception:
                try:
                    # intento sin parámetros por si el driver no soporta
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    for r in rows:
                        try:
                            v = r[0]
                            if v is None:
                                continue
                            s = str(v).strip()
                            if s:
                                vins.append(s)
                        except Exception:
                            continue
                except Exception:
                    pass
            try:
                cursor.close()
                conn.close()
            except Exception:
                pass
        except Exception:
            try:
                tab.logger.debug("No se pudieron cargar VINs desde DB para autocompletado")
            except Exception:
                pass

        # Actualizar el modelo del QCompleter persistente y forzar popup
        try:
            completer = getattr(tab, '_details_vin_completer', None)
            model = getattr(tab, '_details_vin_completer_model', None)
            if model is not None:
                try:
                    model.setStringList(vins)
                except Exception:
                    # reemplazar modelo si falla
                    try:
                        model = QStringListModel(vins)
                        if completer is not None:
                            completer.setModel(model)
                        tab._details_vin_completer_model = model
                    except Exception:
                        pass
            else:
                try:
                    model = QStringListModel(vins)
                    if completer is not None:
                        completer.setModel(model)
                    tab._details_vin_completer_model = model
                except Exception:
                    pass

            # Actualizar el prefix y abrir popup sólo si el usuario ya escribió algo
            try:
                if completer is not None:
                    prefix = ''
                    try:
                        prefix = vin_input.text().strip()
                    except Exception:
                        prefix = ''
                    try:
                        # actualizamos el prefix pero sólo forzamos apertura si hay texto
                        if prefix:
                            try:
                                completer.setCompletionPrefix(prefix)
                            except Exception:
                                pass
                            try:
                                if vins:
                                    completer.complete()
                            except Exception:
                                pass
                        else:
                            # sin prefix, no abrir popup; sólo actualizar modelo
                            try:
                                if model is not None:
                                    model.setStringList(vins)
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            try:
                tab.logger.debug("No se pudo asignar/actualizar QCompleter al input VIN")
            except Exception:
                pass
    except Exception:
        try:
            tab.logger.exception("Error actualizando completador VIN")
        except Exception:
            pass


def save_vin_details(tab: Any) -> None:
    """Manejador para el botón Guardar de la pestaña VIN.

    Actualmente muestra un diálogo de confirmación y registra los valores.
    """
    try:
        try:
            from PyQt6.QtWidgets import QMessageBox
        except Exception:
            QMessageBox = None
        parent = getattr(tab, '_details_panel', None) or tab.window() or tab

        marca = ''
        modelo = ''
        vin = ''
        try:
            w = getattr(tab, '_details_vin_marca_input', None)
            if w is not None:
                marca = w.text().strip()
        except Exception:
            marca = ''
        try:
            w = getattr(tab, '_details_vin_modelo_input', None)
            if w is not None:
                modelo = w.text().strip()
        except Exception:
            modelo = ''
        try:
            w = getattr(tab, '_details_vin_vin_input', None)
            if w is not None:
                vin = w.text().strip()
        except Exception:
            vin = ''

        # Convertir a mayúsculas antes de persistir/mostrar
        try:
            marca = marca.upper()
        except Exception:
            pass
        try:
            modelo = modelo.upper()
        except Exception:
            pass
        try:
            vin = vin.upper()
        except Exception:
            pass

        # Validación simple
        if not vin:
            try:
                if QMessageBox is not None:
                    QMessageBox.warning(parent, "Validación", "El campo VIN está vacío")
                return
            except Exception:
                return

        # Persistir en SQL Server: insertar sólo si no existe VIN
        try:
            conn = connect_db('compras_internacionales')
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT 1 FROM [ComprasInternacionales].[kdx].[VinMarketshare] WHERE [VIN]=?", (vin,))
                exists = cursor.fetchone() is not None
                if not exists:
                    cursor.execute(
                        "INSERT INTO [ComprasInternacionales].[kdx].[VinMarketshare] ([MARCA],[MODELO],[VIN]) VALUES (?,?,?)",
                        (marca, modelo, vin),
                    )
                    try:
                        conn.commit()
                    except Exception:
                        pass
                    inserted = True
                else:
                    inserted = False
            except Exception:
                inserted = False
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
        except Exception:
            try:
                tab.logger.exception("Error persistiendo VIN en BD")
            except Exception:
                pass
            inserted = False

        try:
            if inserted:
                if QMessageBox is not None:
                    QMessageBox.information(parent, "Guardado", f"Detalles guardados:\nMarca: {marca}\nModelo: {modelo}\nVIN: {vin}")
            else:
                if QMessageBox is not None:
                    QMessageBox.information(parent, "Sin cambios", "El VIN ya existe en la base de datos")
        except Exception:
            pass

        # Capturar snapshot previo a las detecciones para resaltar cambios visuales
        try:
            before_snapshot = _capture_table_snapshot(tab, ('MODELO', 'MARCA', 'CATEGORIA'))
        except Exception:
            before_snapshot = None

        # Ejecutar detección fase 2 (VIN) tras el guardado
        try:
            if detect_models is not None:
                try:
                    result = detect_models(tab, db_key='compras_internacionales', phase=2)
                    try:
                        if QMessageBox is not None:
                            QMessageBox.information(parent, "Detección (VIN)", f"Fase 2 completada: {result} coincidencias asignadas")
                    except Exception:
                        pass
                    # Después de asignar MODELO, ejecutar fases 6 y 7 para completar MARCA y CATEGORIA
                    try:
                        res6 = detect_models(tab, db_key='compras_internacionales', phase=6)
                        try:
                            if QMessageBox is not None:
                                QMessageBox.information(parent, "Detección (MARCA)", f"Fase 6 completada: {res6} coincidencias asignadas")
                        except Exception:
                            pass
                    except Exception as e:
                        try:
                            tab.logger.exception("Error ejecutando detect_models fase 6: %s", e)
                        except Exception:
                            pass
                    try:
                        res7 = detect_models(tab, db_key='compras_internacionales', phase=7)
                        try:
                            if QMessageBox is not None:
                                QMessageBox.information(parent, "Detección (CATEGORIA)", f"Fase 7 completada: {res7} coincidencias asignadas")
                        except Exception:
                            pass
                    except Exception as e:
                        try:
                            tab.logger.exception("Error ejecutando detect_models fase 7: %s", e)
                        except Exception:
                            pass
                except Exception as e:
                    try:
                        tab.logger.exception("Error ejecutando detect_models fase 2: %s", e)
                    except Exception:
                        pass
            else:
                try:
                    tab.logger.debug("detect_models no disponible; no se ejecuta fase 2")
                except Exception:
                    pass
        except Exception:
            try:
                tab.logger.exception("Error invocando detect_models fase 2")
            except Exception:
                pass
        # Capturar snapshot posterior, intentar rellenar MARCA/CATEGORIA para filas cambiadas, recapturar y resaltar
        try:
            after_snapshot = _capture_table_snapshot(tab, ('MODELO', 'MARCA', 'CATEGORIA'))
            changed = _compare_snapshots(before_snapshot, after_snapshot)
            try:
                # intentar completar MARCA/CATEGORIA para las filas donde cambió MODELO
                _fill_marca_for_rows(tab, changed)
                _fill_categoria_for_rows(tab, changed)
            except Exception:
                pass
            # recapturar para incluir cambios realizados por los helpers
            after_snapshot2 = _capture_table_snapshot(tab, ('MODELO', 'MARCA', 'CATEGORIA'))
            changed_final = _compare_snapshots(before_snapshot, after_snapshot2)
            _highlight_rows(tab, changed_final)
        except Exception:
            pass
    except Exception:
        try:
            tab.logger.exception("Error en save_vin_details")
        except Exception:
            pass


def save_otros_details(tab: Any) -> None:
    """Manejador para el botón Guardar de la pestaña Otros.

    Muestra un diálogo con los datos recogidos y los registra.
    """
    try:
        try:
            from PyQt6.QtWidgets import QMessageBox
        except Exception:
            QMessageBox = None
        parent = getattr(tab, '_details_panel', None) or tab.window() or tab

        marca = ''
        modelo = ''
        datos = ''
        try:
            w = getattr(tab, '_details_otros_marca_input', None)
            if w is not None:
                marca = w.text().strip()
        except Exception:
            marca = ''
        try:
            w = getattr(tab, '_details_otros_modelo_input', None)
            if w is not None:
                modelo = w.text().strip()
        except Exception:
            modelo = ''
        try:
            w = getattr(tab, '_details_otros_otros_input', None)
            if w is not None:
                datos = w.text().strip()
        except Exception:
            datos = ''

        # Convertir a mayúsculas antes de persistir/mostrar
        try:
            marca = marca.upper()
        except Exception:
            pass
        try:
            modelo = modelo.upper()
        except Exception:
            pass
        try:
            datos = datos.upper()
        except Exception:
            pass

        if not datos:
            try:
                if QMessageBox is not None:
                    QMessageBox.warning(parent, "Validación", "El campo 'Otros' está vacío")
                return
            except Exception:
                return

        # Persistir en SQL Server: insertar sólo si no existe la tupla (MARCA, MODELO, DATOS)
        try:
            conn = connect_db('compras_internacionales')
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT 1 FROM [ComprasInternacionales].[kdx].[MarcaModeloDatosMarketshareVehículos] WHERE [MARCA]=? AND [MODELO]=? AND [DATOS]=?",
                    (marca, modelo, datos),
                )
                exists = cursor.fetchone() is not None
                if not exists:
                    cursor.execute(
                        "INSERT INTO [ComprasInternacionales].[kdx].[MarcaModeloDatosMarketshareVehículos] ([MARCA],[MODELO],[DATOS]) VALUES (?,?,?)",
                        (marca, modelo, datos),
                    )
                    try:
                        conn.commit()
                    except Exception:
                        pass
                    inserted = True
                else:
                    inserted = False
            except Exception:
                inserted = False
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
        except Exception:
            try:
                tab.logger.exception("Error persistiendo 'Otros' en BD")
            except Exception:
                pass
            inserted = False

        try:
            if inserted:
                if QMessageBox is not None:
                    QMessageBox.information(parent, "Guardado", f"Detalles guardados:\nMarca: {marca}\nModelo: {modelo}\nDatos: {datos}")
            else:
                if QMessageBox is not None:
                    QMessageBox.information(parent, "Sin cambios", "La entrada ya existe en la base de datos")
        except Exception:
            pass

        # Capturar snapshot previo a las detecciones para resaltar cambios visuales
        try:
            before_snapshot = _capture_table_snapshot(tab, ('MODELO', 'MARCA', 'CATEGORIA'))
        except Exception:
            before_snapshot = None

        # Ejecutar detección fase 3 (DATOS) tras el guardado
        try:
            if detect_models is not None:
                try:
                    result = detect_models(tab, db_key='compras_internacionales', phase=3)
                    try:
                        if QMessageBox is not None:
                            QMessageBox.information(parent, "Detección (DATOS)", f"Fase 3 completada: {result} coincidencias asignadas")
                    except Exception:
                        pass
                    # Después de asignar MODELO, ejecutar fases 6 y 7 para completar MARCA y CATEGORIA
                    try:
                        res6 = detect_models(tab, db_key='compras_internacionales', phase=6)
                        try:
                            if QMessageBox is not None:
                                QMessageBox.information(parent, "Detección (MARCA)", f"Fase 6 completada: {res6} coincidencias asignadas")
                        except Exception:
                            pass
                    except Exception as e:
                        try:
                            tab.logger.exception("Error ejecutando detect_models fase 6: %s", e)
                        except Exception:
                            pass
                    try:
                        res7 = detect_models(tab, db_key='compras_internacionales', phase=7)
                        try:
                            if QMessageBox is not None:
                                QMessageBox.information(parent, "Detección (CATEGORIA)", f"Fase 7 completada: {res7} coincidencias asignadas")
                        except Exception:
                            pass
                    except Exception as e:
                        try:
                            tab.logger.exception("Error ejecutando detect_models fase 7: %s", e)
                        except Exception:
                            pass
                except Exception as e:
                    try:
                        tab.logger.exception("Error ejecutando detect_models fase 3: %s", e)
                    except Exception:
                        pass
            else:
                try:
                    tab.logger.debug("detect_models no disponible; no se ejecuta fase 3")
                except Exception:
                    pass
        except Exception:
            try:
                tab.logger.exception("Error invocando detect_models fase 3")
            except Exception:
                pass
        # Capturar snapshot posterior, intentar rellenar MARCA/CATEGORIA para filas cambiadas, recapturar y resaltar
        try:
            after_snapshot = _capture_table_snapshot(tab, ('MODELO', 'MARCA', 'CATEGORIA'))
            changed = _compare_snapshots(before_snapshot, after_snapshot)
            try:
                _fill_marca_for_rows(tab, changed)
                _fill_categoria_for_rows(tab, changed)
            except Exception:
                pass
            after_snapshot2 = _capture_table_snapshot(tab, ('MODELO', 'MARCA', 'CATEGORIA'))
            changed_final = _compare_snapshots(before_snapshot, after_snapshot2)
            _highlight_rows(tab, changed_final)
        except Exception:
            pass
    except Exception:
        try:
            tab.logger.exception("Error en save_otros_details")
        except Exception:
            pass


def update_otros_completer(tab: Any, db_key: str = 'compras_internacionales') -> None:
    """Carga sugerencias de 'Otros' desde la tabla SQL y aplica QCompleter al campo 'Otros'.

    Si hay texto en los campos Marca/Modelo (de la pestaña Otros), se usan como filtro (LIKE).
    """
    try:
        if QCompleter is None or QStringListModel is None or Qt is None:
            return
    except Exception:
        return
    try:
        otros_input = getattr(tab, '_details_otros_otros_input', None)
        if otros_input is None:
            return

        marca = ""
        modelo = ""
        try:
            widget = getattr(tab, '_details_otros_marca_input', None)
            if widget is not None:
                marca = widget.text().strip()
        except Exception:
            marca = ""
        try:
            widget = getattr(tab, '_details_otros_modelo_input', None)
            if widget is not None:
                modelo = widget.text().strip()
        except Exception:
            modelo = ""

        datos: list[str] = []
        try:
            conn = connect_db(db_key)
            cursor = conn.cursor()
            # Construir query con filtros opcionales
            sql = "SELECT DISTINCT [DATOS] FROM [ComprasInternacionales].[kdx].[MarcaModeloDatosMarketshareVehículos] WHERE 1=1"
            params: list[str] = []
            if marca:
                sql += " AND [MARCA] LIKE ?"
                params.append(f"%{marca}%")
            if modelo:
                sql += " AND [MODELO] LIKE ?"
                params.append(f"%{modelo}%")
            sql += " ORDER BY [DATOS]"
            try:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
                for r in rows:
                    try:
                        v = r[0]
                        if v is None:
                            continue
                        s = str(v).strip()
                        if s:
                            datos.append(s)
                    except Exception:
                        continue
            except Exception:
                try:
                    # intento sin parámetros por si el driver no soporta
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    for r in rows:
                        try:
                            v = r[0]
                            if v is None:
                                continue
                            s = str(v).strip()
                            if s:
                                datos.append(s)
                        except Exception:
                            continue
                except Exception:
                    pass
            try:
                cursor.close()
                conn.close()
            except Exception:
                pass
        except Exception:
            try:
                tab.logger.debug("No se pudieron cargar 'Otros' desde DB para autocompletado")
            except Exception:
                pass

        # Actualizar el modelo del QCompleter persistente y forzar popup
        try:
            completer = getattr(tab, '_details_otros_completer', None)
            model = getattr(tab, '_details_otros_completer_model', None)
            if model is not None:
                try:
                    model.setStringList(datos)
                except Exception:
                    # reemplazar modelo si falla
                    try:
                        model = QStringListModel(datos)
                        if completer is not None:
                            completer.setModel(model)
                        tab._details_otros_completer_model = model
                    except Exception:
                        pass
            else:
                try:
                    model = QStringListModel(datos)
                    if completer is not None:
                        completer.setModel(model)
                    tab._details_otros_completer_model = model
                except Exception:
                    pass

            # Actualizar el prefix y abrir popup sólo si el usuario ya escribió algo
            try:
                if completer is not None:
                    prefix = ''
                    try:
                        prefix = otros_input.text().strip()
                    except Exception:
                        prefix = ''
                    try:
                        if prefix:
                            try:
                                completer.setCompletionPrefix(prefix)
                            except Exception:
                                pass
                            try:
                                if datos:
                                    completer.complete()
                            except Exception:
                                pass
                        else:
                            try:
                                if model is not None:
                                    model.setStringList(datos)
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            try:
                tab.logger.debug("No se pudo asignar/actualizar QCompleter al input 'Otros'")
            except Exception:
                pass
    except Exception:
        try:
            tab.logger.exception("Error actualizando completador 'Otros'")
        except Exception:
            pass

