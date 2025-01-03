from PyQt5.QtWidgets import QGroupBox, QVBoxLayout, QTableView, QAbstractItemView


class ResultsGroup(QGroupBox):
    def __init__(self, parent=None):
        super().__init__("Results", parent)

        layout = QVBoxLayout()

        # Create the QTableView and expose it via an attribute
        self.table = QTableView()
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        layout.addWidget(self.table)

        self.setLayout(layout)
