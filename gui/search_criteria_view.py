from PyQt5.QtWidgets import (QLabel, QLineEdit, QPushButton, QVBoxLayout, QHBoxLayout, QGroupBox, QMessageBox)

class SearchCriteriaView(QGroupBox):
    def __init__(self, search_callback):
        super().__init__("Search Criteria")
        self.search_callback = search_callback

        layout = QVBoxLayout()

        # Make layout
        make_layout = QHBoxLayout()
        make_layout.addWidget(QLabel("Make:"))
        self.search_make_edit = QLineEdit()
        make_layout.addWidget(self.search_make_edit)
        layout.addLayout(make_layout)

        # Model layout
        model_layout = QHBoxLayout()
        model_layout.addWidget(QLabel("Model:"))
        self.search_model_edit = QLineEdit()
        model_layout.addWidget(self.search_model_edit)
        layout.addLayout(model_layout)

        # Year layout
        year_layout = QHBoxLayout()
        year_layout.addWidget(QLabel("Year:"))
        self.search_year_edit = QLineEdit()
        year_layout.addWidget(self.search_year_edit)
        layout.addLayout(year_layout)

        # Mileage layout
        mileage_layout = QHBoxLayout()
        mileage_layout.addWidget(QLabel("Min Mileage:"))
        self.search_min_mileage_edit = QLineEdit()
        mileage_layout.addWidget(self.search_min_mileage_edit)
        mileage_layout.addWidget(QLabel("Max Mileage:"))
        self.search_max_mileage_edit = QLineEdit()
        mileage_layout.addWidget(self.search_max_mileage_edit)
        layout.addLayout(mileage_layout)

        # Search button
        self.search_button = QPushButton("Search")
        self.search_button.clicked.connect(self.on_search_clicked)
        layout.addWidget(self.search_button)

        self.setLayout(layout)

    def on_search_clicked(self):
        make = self.search_make_edit.text()
        model = self.search_model_edit.text()
        try:
            year = int(self.search_year_edit.text()) if self.search_year_edit.text() else None
            min_mileage = int(self.search_min_mileage_edit.text()) if self.search_min_mileage_edit.text() else None
            max_mileage = int(self.search_max_mileage_edit.text()) if self.search_max_mileage_edit.text() else None
        except ValueError:
            QMessageBox.warning(self, "Input Error", "Year and mileage must be numbers.")
            return

        if min_mileage is not None and max_mileage is not None and min_mileage > max_mileage:
            QMessageBox.warning(self, "Input Error", "Minimum mileage cannot be greater than maximum mileage.")
            return

        search_criteria = {
            'make': make,
            'model': model,
            'year': year,
            'min_mileage': min_mileage,
            'max_mileage': max_mileage
        }
        self.search_callback(search_criteria)