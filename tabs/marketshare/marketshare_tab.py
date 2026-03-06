from PyQt6.QtWidgets import QWidget, QVBoxLayout, QTabWidget

from .marketshare_vehiculos_tab import MarketshareVehiculosTab
from .marketshare_repuestos_tab import MarketshareRepuestosTab


class MarketshareTab(QWidget):

	def __init__(self, parent=None):
		super().__init__(parent)
		layout = QVBoxLayout(self)
		self.tab_widget = QTabWidget()
		layout.addWidget(self.tab_widget)

		# Pestañas internas
		self.vehiculos_tab = MarketshareVehiculosTab()
		self.tab_widget.addTab(self.vehiculos_tab, "Vehículos")

		# Nueva sub-pestaña para Repuestos
		try:
			self.repuestos_tab = MarketshareRepuestosTab()
			self.tab_widget.addTab(self.repuestos_tab, "Marketshare_Repuestos")
		except Exception:
			# si falla la creación, no bloquear la carga
			try:
				# crear placeholder mínimo
				ph = QWidget()
				self.tab_widget.addTab(ph, "Marketshare_Repuestos")
			except Exception:
				pass
