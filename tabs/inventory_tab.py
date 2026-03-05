import logging
from typing import cast

from PyQt6.QtWidgets import QWidget, QVBoxLayout, QPushButton, QTableWidget, QTableWidgetItem, QTabWidget
from PyQt6.QtCore import Qt


class InventoryTabMixin:
    logger: logging.Logger
    tab_widget: QTabWidget
    column_names: list
    left_aligned_columns: set
    current_df = None

    def _connect_db(self, key):
        raise NotImplementedError

    def _parent_widget(self):
        return cast(QWidget, self)

    def load_excel(self):
        """Stub para que Pylance reconozca el método; implementado por la clase principal."""
        raise NotImplementedError

    def setup_inventory_import_tab(self):
        self.inventory_import_tab = QWidget()
        layout = QVBoxLayout(self.inventory_import_tab)

        self.load_inventory_button = QPushButton("Cargar inventario desde Excel")
        self.load_inventory_button.clicked.connect(self.load_excel)
        layout.addWidget(self.load_inventory_button)

        # Preview table for the imported inventory
        self.inventory_preview_table = QTableWidget()
        layout.addWidget(self.inventory_preview_table)

        self.tab_widget.addTab(self.inventory_import_tab, "Importar Inventario")

    def update_inventory_preview(self):
        df = getattr(self, 'current_df', None)
        if df is None:
            self.inventory_preview_table.setRowCount(0)
            self.inventory_preview_table.setColumnCount(0)
            return
        cols = getattr(self, 'column_names', list(df.columns))
        self.inventory_preview_table.setColumnCount(len(cols))
        self.inventory_preview_table.setHorizontalHeaderLabels(cols)
        self.inventory_preview_table.setRowCount(len(df))
        for i in range(len(df)):
            for j, col in enumerate(cols):
                value = df.iloc[i].get(col, "") if hasattr(df.iloc[i], 'get') else df.iloc[i, j]
                item = QTableWidgetItem("")
                if value is not None:
                    item.setText(str(value))
                if col in getattr(self, 'left_aligned_columns', set()):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
                else:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.inventory_preview_table.setItem(i, j, item)
