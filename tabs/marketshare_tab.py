from PyQt6.QtWidgets import QWidget, QVBoxLayout, QTabWidget
from typing import TYPE_CHECKING

# Para que Pylance resuelva durante el análisis estático, usamos TYPE_CHECKING.
if TYPE_CHECKING:
	# import estático solo para el type checker
	from .marketshare_vehiculos_tab import MarketshareVehiculosTab  # type: ignore
else:
	# Import dinámico: intenta relativo, luego absoluto
	try:
		from .marketshare_vehiculos_tab import MarketshareVehiculosTab
	except Exception:
		try:
			from tabs.marketshare.marketshare_vehiculos_tab import MarketshareVehiculosTab
		except Exception:
			# último recurso: importar por nombre completo
			import importlib
			MarketshareVehiculosTab = importlib.import_module("tabs.marketshare.marketshare_vehiculos_tab").MarketshareVehiculosTab


class MarketshareTab(QWidget):

	def __init__(self, parent=None):
		super().__init__(parent)
		layout = QVBoxLayout(self)
		self.tab_widget = QTabWidget()
		layout.addWidget(self.tab_widget)

		# Pestañas internas
		self.vehiculos_tab = MarketshareVehiculosTab()
		self.tab_widget.addTab(self.vehiculos_tab, "Vehículos")

		# Placeholder: agregar más pestañas aquí si se encuentran en el proyecto
