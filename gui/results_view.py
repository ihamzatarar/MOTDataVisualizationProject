from PyQt5.QtWidgets import QTableView, QAbstractItemView, QGroupBox, QVBoxLayout
from gui.utils import PandasModel

class ResultsView(QGroupBox):
    def __init__(self):
        super().__init__("Results")
        layout = QVBoxLayout()
        self.results_table = QTableView()
        self.results_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.results_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        layout.addWidget(self.results_table)
        self.setLayout(layout)

    def display_results(self, results):
        if results is not None and not results.empty:
            model = PandasModel(results)
            self.results_table.setModel(model)
        else:
            self.results_table.setModel(None)