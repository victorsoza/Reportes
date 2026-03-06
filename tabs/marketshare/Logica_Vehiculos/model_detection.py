from typing import Optional, List
import re
import os
import logging
import unicodedata
import pyodbc

from db_config import connect_db

from PyQt6.QtWidgets import QTableWidgetItem, QMessageBox, QApplication
from PyQt6.QtCore import QObject, pyqtSignal, QThread, pyqtSlot, QTimer, Qt
from PyQt6.QtGui import QColor


def _normalize(s: Optional[str]) -> str:
	"""Normaliza una cadena para comparaciones:

	- Convierte a str
	- Elimina espacios al inicio/fin
	- Elimina diacríticos (acentos)
	- Pasa a minúsculas
	"""
	if s is None:
		return ""
	if not isinstance(s, str):
		s = str(s)
	s = s.strip()
	s = unicodedata.normalize('NFKD', s)
	s = ''.join(ch for ch in s if not unicodedata.combining(ch))
	return s.lower()


def _phase7_complete_categoria(tab, db_key: str = 'compras_internacionales', query: Optional[str] = None, models_list: Optional[list] = None, logger: Optional[logging.Logger] = None) -> int:
	if logger is None:
		logger = logging.getLogger("ReportesApp")
	logger.info("Iniciando fase 7: completar CATEGORIA desde MODELO")
	map_model_to_categoria: dict[str, str] = {}
	if models_list:
		for it in models_list:
			try:
				if isinstance(it, (list, tuple)) and len(it) >= 2:
					modelo_cand = str(it[0]).strip()
					categoria_cand = str(it[1]).strip()
					if not modelo_cand:
						continue
					map_model_to_categoria[_normalize(modelo_cand)] = categoria_cand
			except Exception:
				continue
	else:
		try:
			conn = connect_db(db_key)
			cur = conn.cursor()
			q = query or "SELECT [MODELO], [CATEGORIA] FROM [ComprasInternacionales].[kdx].[CategoriaMarketshare]"
			logger.debug("Ejecutando query para MODELO-CATEGORIA: %s", q)
			cur.execute(q)
			rows = cur.fetchall()
			for r in rows:
				try:
					modelo = str(r[0]).strip() if r and r[0] is not None else ""
					categoria = str(r[1]).strip() if r and len(r) > 1 and r[1] is not None else ""
					if modelo and categoria:
						map_model_to_categoria[_normalize(modelo)] = categoria
				except Exception:
					continue
			try:
				cur.close()
				conn.close()
			except Exception:
				pass
			logger.info("Mapeo MODELO-CATEGORIA cargado: %s", len(map_model_to_categoria))
		except Exception as e:
			logger.exception("No se pudieron cargar CATEGORIAS desde BD: %s", e)

	if not map_model_to_categoria:
		logger.warning("Mapa MODELO-CATEGORIA vacío; fase 7 no tiene trabajo.")
		return 0

	# localizar índices de columnas MODELO y CATEGORIA
	idx_modelo = None
	idx_categoria = None
	for i in range(tab.table.columnCount()):
		hi = tab.table.horizontalHeaderItem(i)
		if hi is None:
			continue
		name = hi.text().strip().upper()
		if name == 'MODELO' and idx_modelo is None:
			idx_modelo = i
		elif name == 'CATEGORIA' and idx_categoria is None:
			idx_categoria = i

	if idx_modelo is None or idx_categoria is None:
		logger.error("Columna 'MODELO' o 'CATEGORIA' no encontrada (fase 7)")
		return 0

	matched_cat = 0
	matched_cat_na = 0
	total_rows = tab.table.rowCount()
	for r in range(total_rows):
		try:
			try:
				m_item = tab.table.item(r, idx_modelo)
				modelo_text = m_item.text().strip() if (m_item is not None and m_item.text() is not None) else ""
			except Exception:
				modelo_text = ""
			if not modelo_text:
				continue
			try:
				c_item = tab.table.item(r, idx_categoria)
				categoria_text = c_item.text().strip() if (c_item is not None and c_item.text() is not None) else ""
			except Exception:
				categoria_text = ""
			if categoria_text:
				continue
			norm = _normalize(modelo_text)
			if not norm:
				continue
			# Intentar coincidencia exacta primero
			cat_val = map_model_to_categoria.get(norm)
			# Si no hay coincidencia exacta, intentar por substring o tokens
			if not cat_val:
				try:
					for k, v in map_model_to_categoria.items():
						if not k:
							continue
						if k in norm or norm in k:
							cat_val = v
							break
						k_tokens = set(k.split())
						n_tokens = set(norm.split())
						if k_tokens & n_tokens:
							cat_val = v
							break
				except Exception:
					cat_val = None
			if cat_val:
				try:
					tab.table.setItem(r, idx_categoria, QTableWidgetItem(cat_val))
					matched_cat += 1
				except Exception:
					continue
			else:
				# Si no se encontró, asignar N/A
				try:
					tab.table.setItem(r, idx_categoria, QTableWidgetItem("N/A"))
					matched_cat_na += 1
				except Exception:
					continue
			if r % 200 == 0:
				try:
					QApplication.processEvents()
				except Exception:
					pass
		except Exception:
			logger.exception("Error en fase 7 procesando fila %s", r)
			continue

	try:
		tab.update_row_markers()
	except Exception:
		pass
	try:
		tab.adjust_column_widths()
	except Exception:
		pass

	matched_cat_total = matched_cat + matched_cat_na
	logger.info("Detección fase 7 completada: %s coincidencias asignadas (CATEGORIA desde MODELO) + %s N/A asignadas", matched_cat, matched_cat_na)
	return matched_cat_total

# Detecta modelos comparando palabras completas en las columnas.
def detect_models(tab, db_key: str = 'compras_internacionales', query: Optional[str] = None, models_list: Optional[list] = None, phase: int = 1):

	logger = logging.getLogger("ReportesApp")
	logger.info("Iniciando detect_models (fase=%s)", phase)

	if phase == 1:
		# Cargar lista de modelos (solo MODELO) desde parámetro o BD
		models: List[str] = []
		if models_list:
			# `models_list` puede venir como lista de strings o tuplas; extraer modelo
			for it in models_list:
				try:
					if isinstance(it, (list, tuple)) and len(it) >= 2:
						modelo = str(it[1]).strip()
					else:
						modelo = str(it).strip()
					if modelo:
						models.append(modelo)
				except Exception:
					continue
			logger.debug("Usando lista de modelos proporcionada: %s", len(models))
		else:
			# Consultar BD por MODELO únicamente
			try:
				conn = connect_db(db_key)
				cur = conn.cursor()
				q = query or "SELECT [MODELO] FROM [ComprasInternacionales].[kdx].[MarcaModeloMarketshare]"
				logger.debug("Ejecutando query para modelos: %s", q)
				cur.execute(q)
				rows = cur.fetchall()
				for r in rows:
					try:
						modelo = str(r[0]).strip() if r and r[0] is not None else ""
						if modelo:
							models.append(modelo)
					except Exception:
						continue
				try:
					cur.close()
					conn.close()
				except Exception:
					pass
				logger.info("Modelos cargados desde BD: %s", len(models))
			except Exception as e:
				logger.exception("No se pudieron cargar modelos desde BD: %s", e)

		if not models:
			logger.warning("Lista de modelos vacía; no hay nada que detectar.")
			QMessageBox.information(tab, "Detectar Modelos", "No se encontraron modelos en la fuente configurada.")
			return 0

		# Determinar índices de columnas relevantes en la tabla de la UI
		idx_unidad = None
		idx_unidad2 = None
		idx_modelo = None
		for i in range(tab.table.columnCount()):
			hi = tab.table.horizontalHeaderItem(i)
			if hi is None:
				continue
			name = hi.text().strip().upper()
			if name == 'UNIDAD DE MEDIDA' and idx_unidad is None:
				idx_unidad = i
			elif name == 'UNIDAD DE MEDIDA2' and idx_unidad2 is None:
				idx_unidad2 = i
			elif name == 'MODELO' and idx_modelo is None:
				idx_modelo = i

		if idx_modelo is None:
			logger.error("Columna 'MODELO' no encontrada en la tabla de la UI")
			QMessageBox.critical(tab, "Error", "Columna 'MODELO' no encontrada en la tabla.")
			return 0

		matched = 0
		total_rows = tab.table.rowCount()

		# Precompilar regexes para cada modelo (buscar palabra completa)
		# Guardamos (modelo_original, patrón_normalizado)
		compiled: list[tuple[str, re.Pattern]] = []
		for modelo in models:
			if not modelo:
				continue
			modelo_orig = str(modelo).strip()
			modelo_norm = _normalize(modelo_orig)
			if not modelo_norm:
				continue
			# Buscamos palabra completa en texto normalizado
			try:
				pat = re.compile(r"\b" + re.escape(modelo_norm) + r"\b", flags=re.IGNORECASE)
				compiled.append((modelo_orig, pat))
			except re.error:
				# fallback: escapar y compilar
				pat = re.compile(re.escape(modelo_norm), flags=re.IGNORECASE)
				compiled.append((modelo_orig, pat))

		# Iterar filas y aplicar detección
		for r in range(total_rows):
			try:
				# Obtener textos de las columnas objetivo
				unidad_text = ""
				unidad2_text = ""
				try:
					if idx_unidad is not None:
						it = tab.table.item(r, idx_unidad)
						unidad_text = _normalize(it.text() if it is not None and it.text() is not None else "")
				except Exception:
					unidad_text = ""
				try:
					if idx_unidad2 is not None:
						it2 = tab.table.item(r, idx_unidad2)
						unidad2_text = _normalize(it2.text() if it2 is not None and it2.text() is not None else "")
				except Exception:
					unidad2_text = ""

				found = False
				for modelo_orig, pat in compiled:
					# comprobar primero en UNIDAD DE MEDIDA
					try:
						if unidad_text and pat.search(unidad_text):
							# Asignar MODELO
							tab.table.setItem(r, idx_modelo, QTableWidgetItem(modelo_orig))
							matched += 1
							found = True
							break
					except Exception:
						pass
					# si no coincide en unidad, probar unidad2
					try:
						if not found and unidad2_text and pat.search(unidad2_text):
							tab.table.setItem(r, idx_modelo, QTableWidgetItem(modelo_orig))
							matched += 1
							found = True
							break
					except Exception:
						pass
				# Procesar eventos de la UI periódicamente para mantener responsividad
				if r % 200 == 0:
					try:
						QApplication.processEvents()
					except Exception:
						pass
			except Exception:
				logger.exception("Error detectando modelo en fila %s", r)
				continue

		# Actualizar marcadores visuales y logs
		try:
			tab.update_row_markers()
		except Exception:
			pass
		try:
			tab.adjust_column_widths()
		except Exception:
			pass

		logger.info("Detección fase 1 completada: %s coincidencias asignadas en %s filas", matched, total_rows)

		# Guardar contador de la fase 1
		matched_phase1 = matched

		# Ejecutar fase 2 (VIN)
		matched_phase2 = 0
		try:
			matched_phase2 = int(detect_models(tab, db_key=db_key, query=None, models_list=None, phase=2) or 0)
			logger.info("Fase 2 (VIN) retornó %s coincidencias", matched_phase2)
		except Exception:
			logger.exception("No se pudo ejecutar fase 2")

		# Ejecutar fase 3 (DATOS)
		matched_phase3 = 0
		try:
			matched_phase3 = int(detect_models(tab, db_key=db_key, query=None, models_list=None, phase=3) or 0)
			logger.info("Fase 3 (DATOS) retornó %s coincidencias", matched_phase3)
		except Exception:
			logger.exception("No se pudo ejecutar fase 3")

		# Ejecutar fase 4: agrupación por PESO_BRUTO + VALOR_CIF y rellenar MODELO en filas vacías
		matched_phase4 = 0
		try:
			matched_phase4 = int(detect_models(tab, db_key=db_key, query=None, models_list=None, phase=4) or 0)
			logger.info("Fase 4 (PESO_BRUTO+VALOR_CIF) retornó %s coincidencias", matched_phase4)
		except Exception:
			logger.exception("No se pudo ejecutar fase 4")

		# Ejecutar fase 5 (Asignar N/A a MODELO vacío)
		matched_phase5 = 0
		try:
			matched_phase5 = int(detect_models(tab, db_key=db_key, query=None, models_list=None, phase=5) or 0)
			logger.info("Fase 5 (N/A) retornó %s coincidencias", matched_phase5)
		except Exception:
			logger.exception("No se pudo ejecutar fase 5")

		# Ejecutar fase 6 (Completar MARCA desde MODELO)
		matched_phase6 = 0
		try:
			matched_phase6 = int(detect_models(tab, db_key=db_key, query=None, models_list=None, phase=6) or 0)
			logger.info("Fase 6 (MARCA desde MODELO) retornó %s coincidencias", matched_phase6)
		except Exception:
			logger.exception("No se pudo ejecutar fase 6")

		# Ejecutar fase 7 (Completar CATEGORIA desde MODELO)
		matched_phase7 = 0
		try:
			matched_phase7 = int(detect_models(tab, db_key=db_key, query=None, models_list=None, phase=7) or 0)
			logger.info("Fase 7 (CATEGORIA desde MODELO) retornó %s coincidencias", matched_phase7)
		except Exception:
			logger.exception("No se pudo ejecutar fase 7")

		# Total filas cargadas
		total_rows_loaded = total_rows

		# Total filas con MODELO completado (contar no vacíos en la columna MODELO)
		total_models_completed = 0
		try:
			for r in range(tab.table.rowCount()):
				try:
					item = tab.table.item(r, idx_modelo)
					if item is not None and item.text() and item.text().strip():
						total_models_completed += 1
				except Exception:
					continue
		except Exception:
			logger.exception("Error contando filas con MODELO completado")

		# Preparar mensaje detallado
		msg = (
			f"Total filas cargadas: {total_rows_loaded}\n"
			f"Modelos completados - Fase 1 (MODELO): {matched_phase1}\n"
			f"Modelos completados - Fase 2 (VIN): {matched_phase2}\n"
				f"Modelos completados - Fase 3 (DATOS): {matched_phase3}\n"
				f"Modelos completados - Fase 4 (PESO_BRUTO+VALOR_CIF): {matched_phase4}\n"
				f"Modelos completados - Fase 5 (N/A asignado): {matched_phase5}\n"
				f"Modelos completados - Fase 6 (MARCA desde MODELO): {matched_phase6}\n"
				f"CATEGORIAS completadas - Fase 7 (CATEGORIA desde MODELO): {matched_phase7}\n"
				f"Total filas con MODELO asignado: {total_models_completed}"
		)

		logger.info("Resumen de detección: %s", msg.replace('\n', ' | '))
		QMessageBox.information(tab, "Detección completa", msg)
		return matched_phase1 + matched_phase2 + matched_phase3 + matched_phase4 + matched_phase5 + matched_phase6 + matched_phase7

	elif phase == 2:
		# Fase 2: detección por VIN (comparación parcial, substring)
		logger.info("Iniciando fase 2: detección por VIN")
		vins: list[tuple[str, str]] = []  # (modelo_orig, vin)
		if models_list:
			for it in models_list:
				try:
					# soporta tuplas (modelo, vin) o (marca, modelo, vin)
					if isinstance(it, (list, tuple)) and len(it) >= 2:
						if len(it) >= 3:
							modelo = str(it[1]).strip()
							vin = str(it[2]).strip()
						else:
							modelo = str(it[0]).strip()
							vin = str(it[1]).strip()
					else:
						# no se puede interpretar, skip
						continue
					if vin and modelo:
						vins.append((modelo, vin))
				except Exception:
					continue
			logger.debug("Usando lista de VINs proporcionada: %s", len(vins))
		else:
			try:
				conn = connect_db(db_key)
				cur = conn.cursor()
				q = query or "SELECT [MODELO], [VIN] FROM [ComprasInternacionales].[kdx].[VinMarketshare]"
				logger.debug("Ejecutando query para VINs: %s", q)
				cur.execute(q)
				rows = cur.fetchall()
				for r in rows:
					try:
						modelo = str(r[0]).strip() if r and r[0] is not None else ""
						vin = str(r[1]).strip() if r and len(r) > 1 and r[1] is not None else ""
						if vin and modelo:
							vins.append((modelo, vin))
					except Exception:
						continue
				try:
					cur.close()
					conn.close()
				except Exception:
					pass
				logger.info("VINs cargados desde BD: %s", len(vins))
			except Exception as e:
				logger.exception("No se pudieron cargar VINs desde BD: %s", e)

		if not vins:
			logger.warning("Lista de VINs vacía; fase 2 no tiene trabajo.")
			return 0

		# localizar índices (MODELO ya debe existir)
		idx_unidad = None
		idx_unidad2 = None
		idx_modelo = None
		for i in range(tab.table.columnCount()):
			hi = tab.table.horizontalHeaderItem(i)
			if hi is None:
				continue
			name = hi.text().strip().upper()
			if name == 'UNIDAD DE MEDIDA' and idx_unidad is None:
				idx_unidad = i
			elif name == 'UNIDAD DE MEDIDA2' and idx_unidad2 is None:
				idx_unidad2 = i
			elif name == 'MODELO' and idx_modelo is None:
				idx_modelo = i

		if idx_modelo is None:
			logger.error("Columna 'MODELO' no encontrada (fase 2)")
			return 0

		matched_vin = 0
		total_rows = tab.table.rowCount()

		# Preparar lista normalizada de VINs
		vins_norm: list[tuple[str, str]] = []  # (modelo_orig, vin_norm)
		for modelo, vin in vins:
			vin_norm = _normalize(vin).replace(' ', '')
			if vin_norm:
				vins_norm.append((modelo, vin_norm))

		# Iterar filas y comparar VINs (substring)
		for r in range(total_rows):
			try:
				# sólo procesar si MODELO está vacío (no fue detectado en fase1)
				try:
					cur_item = tab.table.item(r, idx_modelo)
					cur_model_text = cur_item.text().strip() if (cur_item is not None and cur_item.text() is not None) else ""
					# Si tiene valor distinto de vacío y distinto de 'N/A', saltar (ya tiene MODELO válido)
					if cur_model_text and cur_model_text.strip().upper() != 'N/A':
						continue
				except Exception:
					pass

				unidad_text = ""
				unidad2_text = ""
				try:
					if idx_unidad is not None:
						it = tab.table.item(r, idx_unidad)
						unidad_text = _normalize(it.text() if it is not None and it.text() is not None else "").replace(' ', '')
				except Exception:
					unidad_text = ""
				try:
					if idx_unidad2 is not None:
						it2 = tab.table.item(r, idx_unidad2)
						unidad2_text = _normalize(it2.text() if it2 is not None and it2.text() is not None else "").replace(' ', '')
				except Exception:
					unidad2_text = ""

				found = False
				for modelo_orig, vin_norm in vins_norm:
					try:
						if unidad_text and vin_norm in unidad_text:
							tab.table.setItem(r, idx_modelo, QTableWidgetItem(modelo_orig))
							matched_vin += 1
							found = True
							break
					except Exception:
						pass
					try:
						if not found and unidad2_text and vin_norm in unidad2_text:
							tab.table.setItem(r, idx_modelo, QTableWidgetItem(modelo_orig))
							matched_vin += 1
							found = True
							break
					except Exception:
						pass
				if r % 200 == 0:
					try:
						QApplication.processEvents()
					except Exception:
						pass
			except Exception:
				logger.exception("Error en fase 2 detectando VIN en fila %s", r)
				continue

		try:
			tab.update_row_markers()
		except Exception:
			pass
		try:
			tab.adjust_column_widths()
		except Exception:
			pass

		logger.info("Detección fase 2 completada: %s coincidencias asignadas (VIN)", matched_vin)
		return matched_vin

	elif phase == 3:
		# Fase 3: detección por DATOS (comparación por palabra completa)
		logger.info("Iniciando fase 3: detección por DATOS")
		datos_rows: list[tuple[str, str]] = []  # (modelo, datos)
		if models_list:
			for it in models_list:
				try:
					# soporta tuplas (modelo, datos) o (marca, modelo, datos)
					if isinstance(it, (list, tuple)) and len(it) >= 2:
						if len(it) >= 3:
							modelo = str(it[1]).strip()
							datos = str(it[2]).strip()
						else:
							modelo = str(it[0]).strip()
							datos = str(it[1]).strip()
					else:
						continue
					if modelo and datos:
						datos_rows.append((modelo, datos))
				except Exception:
					continue
			logger.debug("Usando lista de DATOS proporcionada: %s", len(datos_rows))
		else:
			try:
				conn = connect_db(db_key)
				cur = conn.cursor()
				q = query or "SELECT [MODELO], [DATOS] FROM [ComprasInternacionales].[kdx].[MarcaModeloDatosMarketshareVehiculos]"
				logger.debug("Ejecutando query para DATOS: %s", q)
				try:
					cur.execute(q)
				except pyodbc.ProgrammingError as e:
					logger.error("No se pudieron cargar DATOS desde BD con query inicial: %s | error: %s", q, e)
					# Intentar variante con 'Vehículos' si la tabla tiene acento en el nombre
					alt_q = q.replace("Vehiculos", "Vehículos")
					if alt_q != q:
						logger.debug("Intentando query alternativo para DATOS: %s", alt_q)
						try:
							cur.execute(alt_q)
							q = alt_q
						except Exception as e2:
							logger.exception("Query alternativo para DATOS falló: %s", e2)
							# Propagar para que el handler exterior registre y devuelva lista vacía
							raise
				rows = cur.fetchall()
				for r in rows:
					try:
						# r puede contener MODELO, DATOS
						modelo = str(r[0]).strip() if r and len(r) > 0 and r[0] is not None else ""
						datos = str(r[1]).strip() if r and len(r) > 1 and r[1] is not None else ""
						if modelo and datos:
							datos_rows.append((modelo, datos))
					except Exception:
						continue
				try:
					cur.close()
					conn.close()
				except Exception:
					pass
				logger.info("DATOS cargados desde BD: %s", len(datos_rows))
			except Exception as e:
				logger.exception("No se pudieron cargar DATOS desde BD: %s", e)

		if not datos_rows:
			logger.warning("Lista de DATOS vacía; fase 3 no tiene trabajo.")
			return 0

		# localizar índices
		idx_unidad = None
		idx_unidad2 = None
		idx_modelo = None
		for i in range(tab.table.columnCount()):
			hi = tab.table.horizontalHeaderItem(i)
			if hi is None:
				continue
			name = hi.text().strip().upper()
			if name == 'UNIDAD DE MEDIDA' and idx_unidad is None:
				idx_unidad = i
			elif name == 'UNIDAD DE MEDIDA2' and idx_unidad2 is None:
				idx_unidad2 = i
			elif name == 'MODELO' and idx_modelo is None:
				idx_modelo = i

		if idx_modelo is None:
			logger.error("Columna 'MODELO' no encontrada (fase 3)")
			return 0

		matched_datos = 0
		total_rows = tab.table.rowCount()

		# Precompilar patrones normalizados para DATOS (búsqueda por palabra completa)
		compiled_datos: list[tuple[str, re.Pattern]] = []
		for modelo, datos in datos_rows:
			datos_norm = _normalize(datos)
			if not datos_norm:
				continue
			try:
				pat = re.compile(r"\b" + re.escape(datos_norm) + r"\b", flags=re.IGNORECASE)
				compiled_datos.append((modelo, pat))
			except re.error:
				pat = re.compile(re.escape(datos_norm), flags=re.IGNORECASE)
				compiled_datos.append((modelo, pat))

		# Iterar filas y aplicar detección por DATOS
		for r in range(total_rows):
			try:
				# sólo procesar si MODELO está vacío
				try:
					cur_item = tab.table.item(r, idx_modelo)
					cur_model_text = cur_item.text().strip() if (cur_item is not None and cur_item.text() is not None) else ""
					# Si tiene valor distinto de vacío y distinto de 'N/A', saltar (ya tiene MODELO válido)
					if cur_model_text and cur_model_text.strip().upper() != 'N/A':
						continue
				except Exception:
					pass

				unidad_text = ""
				unidad2_text = ""
				try:
					if idx_unidad is not None:
						it = tab.table.item(r, idx_unidad)
						unidad_text = _normalize(it.text() if it is not None and it.text() is not None else "")
				except Exception:
					unidad_text = ""
				try:
					if idx_unidad2 is not None:
						it2 = tab.table.item(r, idx_unidad2)
						unidad2_text = _normalize(it2.text() if it2 is not None and it2.text() is not None else "")
				except Exception:
					unidad2_text = ""

				found = False
				for modelo, pat in compiled_datos:
					try:
						if unidad_text and pat.search(unidad_text):
							tab.table.setItem(r, idx_modelo, QTableWidgetItem(modelo))
							matched_datos += 1
							found = True
							break
					except Exception:
						pass
					try:
						if not found and unidad2_text and pat.search(unidad2_text):
							tab.table.setItem(r, idx_modelo, QTableWidgetItem(modelo))
							matched_datos += 1
							found = True
							break
					except Exception:
						pass
				if r % 200 == 0:
					try:
						QApplication.processEvents()
					except Exception:
						pass
			except Exception:
				logger.exception("Error en fase 3 detectando DATOS en fila %s", r)
				continue

		try:
			tab.update_row_markers()
		except Exception:
			pass
		try:
			tab.adjust_column_widths()
		except Exception:
			pass

		logger.info("Detección fase 3 completada: %s coincidencias asignadas (DATOS)", matched_datos)
		return matched_datos

	elif phase == 4:
			# Fase 4: agrupar por PESO_BRUTO y VALOR_CIF; propagar MODELO presente en la agrupación
			logger.info("Iniciando fase 4: agrupación por PESO_BRUTO y VALOR_CIF")
			idx_peso = None
			idx_valor = None
			idx_modelo = None
			for i in range(tab.table.columnCount()):
				hi = tab.table.horizontalHeaderItem(i)
				if hi is None:
					continue
				name = hi.text().strip().upper()
				if name == 'PESO_BRUTO' and idx_peso is None:
					idx_peso = i
				elif name == 'VALOR_CIF' and idx_valor is None:
					idx_valor = i
				elif name == 'MODELO' and idx_modelo is None:
					idx_modelo = i

			if idx_modelo is None:
				logger.error("Columna 'MODELO' no encontrada (fase 4)")
				return 0

			# Construir agrupaciones por key (peso_norm, valor_norm)
			from collections import Counter
			groups: dict[tuple[str, str], dict] = {}
			total_rows = tab.table.rowCount()
			for r in range(total_rows):
				try:
					# leer peso y valor
					try:
						peso_item = tab.table.item(r, idx_peso) if idx_peso is not None else None
						peso_val = _normalize(peso_item.text() if (peso_item is not None and peso_item.text() is not None) else "")
					except Exception:
						peso_val = ""
					try:
						valor_item = tab.table.item(r, idx_valor) if idx_valor is not None else None
						valor_val = _normalize(valor_item.text() if (valor_item is not None and valor_item.text() is not None) else "")
					except Exception:
						valor_val = ""
					key = (peso_val, valor_val)
					if key not in groups:
						groups[key] = {'rows': [], 'models': []}
					groups[key]['rows'].append(r)
					# capturar modelo si existe
					try:
						m_item = tab.table.item(r, idx_modelo)
						m_text = m_item.text().strip() if (m_item is not None and m_item.text() is not None) else ""
					except Exception:
						m_text = ""
					if m_text:
						groups[key]['models'].append(m_text)
				except Exception:
					logger.exception("Error leyendo fila %s durante agrupación fase 4", r)
					continue

			# Para cada grupo que tiene al menos un modelo detectado, asignar a filas vacías
			matched_grp = 0
			for key, info in groups.items():
				rows = info.get('rows', [])
				models_in_group = info.get('models', [])
				if not models_in_group:
					continue
				# escoger el modelo más frecuente en el grupo
				most_common_model = Counter(models_in_group).most_common(1)[0][0]
				for r in rows:
					try:
						try:
							item = tab.table.item(r, idx_modelo)
							cur_text = item.text().strip() if (item is not None and item.text() is not None) else ""
						except Exception:
							cur_text = ""
						if cur_text:
							continue
						# asignar el modelo más común
						try:
							# Crear item y marcar origen de la detección (agrupación)
							itm = QTableWidgetItem(most_common_model)
							try:
								itm.setData(Qt.ItemDataRole.UserRole, 'AGRUPACION_PESO_VALOR')
								itm.setToolTip('Detectado por agrupación PESO_BRUTO+VALOR_CIF')
								itm.setBackground(QColor(230, 245, 255))
							except Exception:
								# si no está disponible alguna API visual, seguir sin fallar
								pass
							tab.table.setItem(r, idx_modelo, itm)
							matched_grp += 1
						except Exception:
							continue
						if r % 200 == 0:
							try:
								QApplication.processEvents()
							except Exception:
								pass

					# Capturar errores de la iteraci�n de filas del grupo y continuar
					except Exception:
						logger.exception("Error procesando fila %s durante agrupaci�n fase 4", r)
						continue

			try:
				tab.update_row_markers()
			except Exception:
				pass
			try:
				tab.adjust_column_widths()
			except Exception:
				pass

			logger.info("Detección fase 4 completada: %s coincidencias asignadas (agrupación PESO/VALOR)", matched_grp)
			return matched_grp


	elif phase == 5:
		# Fase 5: asignar 'N/A' a filas cuyo MODELO sigue vacío
		logger.info("Iniciando fase 5: asignar 'N/A' a MODELO vacío")
		idx_modelo = None
		for i in range(tab.table.columnCount()):
			hi = tab.table.horizontalHeaderItem(i)
			if hi is None:
				continue
			name = hi.text().strip().upper()
			if name == 'MODELO' and idx_modelo is None:
				idx_modelo = i

		if idx_modelo is None:
			logger.error("Columna 'MODELO' no encontrada (fase 5)")
			return 0

		matched_na = 0
		total_rows = tab.table.rowCount()
		for r in range(total_rows):
			try:
				try:
					item = tab.table.item(r, idx_modelo)
					cur_text = item.text().strip() if (item is not None and item.text() is not None) else ""
					if cur_text:
						continue
				except Exception:
					# si hay error leyendo la celda, intentar asignar N/A
					pass
				try:
					tab.table.setItem(r, idx_modelo, QTableWidgetItem("N/A"))
					matched_na += 1
				except Exception:
					continue
			except Exception:
				logger.exception("Error asignando N/A en fila %s", r)
				continue

		try:
			tab.update_row_markers()
		except Exception:
			pass
		try:
			tab.adjust_column_widths()
		except Exception:
			pass

		logger.info("Detección fase 5 completada: %s coincidencias asignadas (N/A)", matched_na)
		return matched_na

	elif phase == 6:
		# Fase 6: completar la columna MARCA en base al MODELO usando tabla MarcaModeloMarketshare
		logger.info("Iniciando fase 6: completar MARCA desde MODELO")
		map_model_to_marca: dict[str, str] = {}
		if models_list:
			for it in models_list:
				try:
					# soporta tuplas (marca, modelo) o (modelo, marca)
					if isinstance(it, (list, tuple)) and len(it) >= 2:
						# intentar (marca, modelo) primero
						marca_cand = str(it[0]).strip()
						modelo_cand = str(it[1]).strip()
						if not marca_cand:
							continue
						map_model_to_marca[_normalize(modelo_cand)] = marca_cand
				except Exception:
					continue
		else:
			try:
				conn = connect_db(db_key)
				cur = conn.cursor()
				q = query or "SELECT [MARCA], [MODELO] FROM [ComprasInternacionales].[kdx].[MarcaModeloMarketshare]"
				logger.debug("Ejecutando query para MARCA-MODELO: %s", q)
				cur.execute(q)
				rows = cur.fetchall()
				for r in rows:
					try:
						marca = str(r[0]).strip() if r and r[0] is not None else ""
						modelo = str(r[1]).strip() if r and len(r) > 1 and r[1] is not None else ""
						if marca and modelo:
							map_model_to_marca[_normalize(modelo)] = marca
					except Exception:
						continue
				try:
					cur.close()
					conn.close()
				except Exception:
					pass
				logger.info("Mapeo MARCA-MODELO cargado: %s", len(map_model_to_marca))
			except Exception as e:
				logger.exception("No se pudieron cargar MARCAS desde BD: %s", e)

		if not map_model_to_marca:
			logger.warning("Mapa MARCA-MODELO vacío; fase 6 no tiene trabajo.")
			return 0

		# localizar índices de columnas MODELO y MARCA
		idx_modelo = None
		idx_marca = None
		idx_unidad = None
		idx_unidad2 = None
		for i in range(tab.table.columnCount()):
			hi = tab.table.horizontalHeaderItem(i)
			if hi is None:
				continue
			name = hi.text().strip().upper()
			if name == 'MODELO' and idx_modelo is None:
				idx_modelo = i
			elif name == 'MARCA' and idx_marca is None:
				idx_marca = i
			elif name == 'UNIDAD DE MEDIDA' and idx_unidad is None:
				idx_unidad = i
			elif name == 'UNIDAD DE MEDIDA2' and idx_unidad2 is None:
				idx_unidad2 = i

		if idx_modelo is None or idx_marca is None:
			logger.error("Columna 'MODELO' o 'MARCA' no encontrada (fase 6)")
			return 0

		matched_marca = 0
		matched_marca_na = 0
		total_rows = tab.table.rowCount()
		for r in range(total_rows):
			try:
				try:
					m_item = tab.table.item(r, idx_modelo)
					modelo_text = m_item.text().strip() if (m_item is not None and m_item.text() is not None) else ""
				except Exception:
					modelo_text = ""
				if not modelo_text:
					continue
				try:
					ma_item = tab.table.item(r, idx_marca)
					marca_text = ma_item.text().strip() if (ma_item is not None and ma_item.text() is not None) else ""
				except Exception:
					marca_text = ""
				if marca_text:
					continue
				norm = _normalize(modelo_text)
				if not norm:
					continue
				# Intentar coincidencia exacta primero
				marca_val = map_model_to_marca.get(norm)
				# Si no hay coincidencia exacta, intentar por substring o tokens
				if not marca_val:
					try:
						for k, v in map_model_to_marca.items():
							if not k:
								continue
							# coincidencia por substring
							if k in norm or norm in k:
								marca_val = v
								break
							# coincidencia por tokens (palabras en común)
							k_tokens = set(k.split())
							n_tokens = set(norm.split())
							if k_tokens & n_tokens:
								marca_val = v
								break
					except Exception:
						marca_val = None
				if marca_val:
					try:
						tab.table.setItem(r, idx_marca, QTableWidgetItem(marca_val))
						matched_marca += 1
					except Exception:
						continue
				else:
					# Intentar buscar la MARCA dentro de los campos UNIDAD DE MEDIDA y UNIDAD DE MEDIDA2
					unidad_text = ""
					unidad2_text = ""
					try:
						if idx_unidad is not None:
							it = tab.table.item(r, idx_unidad)
							unidad_text = _normalize(it.text() if it is not None and it.text() is not None else "")
					except Exception:
						unidad_text = ""
					try:
						if idx_unidad2 is not None:
							it2 = tab.table.item(r, idx_unidad2)
							unidad2_text = _normalize(it2.text() if it2 is not None and it2.text() is not None else "")
					except Exception:
						unidad2_text = ""
					# buscar cada marca conocida en los textos de unidad
					if not marca_val:
						for m in set(map_model_to_marca.values()):
							try:
								m_norm = _normalize(m)
								if not m_norm:
									continue
								if unidad_text and m_norm in unidad_text:
									marca_val = m
									break
								if unidad2_text and m_norm in unidad2_text:
									marca_val = m
									break
							except Exception:
								continue
					if marca_val:
						try:
							tab.table.setItem(r, idx_marca, QTableWidgetItem(marca_val))
							matched_marca += 1
						except Exception:
							continue
					else:
						# Si aún no se encontró, asignar N/A
						try:
							tab.table.setItem(r, idx_marca, QTableWidgetItem("N/A"))
							matched_marca_na += 1
						except Exception:
							continue
				if r % 200 == 0:
					try:
						QApplication.processEvents()
					except Exception:
						pass
			except Exception:
				logger.exception("Error en fase 6 procesando fila %s", r)
				continue

		try:
			tab.update_row_markers()
		except Exception:
			pass
		try:
			tab.adjust_column_widths()
		except Exception:
			pass

		matched_marca_total = matched_marca + matched_marca_na
		logger.info("Detección fase 6 completada: %s coincidencias asignadas (MARCA desde MODELO) + %s N/A asignadas", matched_marca, matched_marca_na)
		return matched_marca_total


	elif phase == 7:
			# Delegar a función separada para reducir complejidad
			try:
				return _phase7_complete_categoria(tab, db_key=db_key, query=query, models_list=models_list, logger=logger)
			except Exception:
				logger.exception("Error ejecutando fase 7")
				return 0

	else:
		logger.error("Fase desconocida: %s", phase)
		return 0
		