from PyQt6.QtWidgets import QWidget, QTabWidget, QVBoxLayout

class ReportesTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        self.tab_widget = QTabWidget()
        layout.addWidget(self.tab_widget)

        # Importar subpestañas solo cuando se usan
        from .reporte_inventario_seguro_tab import ReporteInventarioSeguroTab
        from .reporte_ine_tab import ReporteINETab

        self.reporte_inventario_seguro_tab = ReporteInventarioSeguroTab()
        self.tab_widget.addTab(self.reporte_inventario_seguro_tab, "Reporte Inventario Seguro")

        self.reporte_ine_tab = ReporteINETab()
        self.tab_widget.addTab(self.reporte_ine_tab, "Reporte INE")
