from PyQt6.QtWidgets import QWidget, QVBoxLayout, QTabWidget

from .marketshare_vehiculos_tab import MarketshareVehiculosTab


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
