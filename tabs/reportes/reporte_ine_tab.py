from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel

class ReporteINETab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        label = QLabel("Próximamente")
        layout.addWidget(label)
