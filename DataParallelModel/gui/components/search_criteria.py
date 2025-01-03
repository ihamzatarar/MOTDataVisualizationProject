from PyQt5.QtWidgets import QGroupBox, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton

class SearchCriteriaGroup(QGroupBox):
    def __init__(self, search_callback):
        super().__init__("Search Criteria")
        self.search_callback = search_callback

        search_layout = QVBoxLayout()

        # Make
        make_layout = QHBoxLayout()
        make_layout.addWidget(QLabel("Make:"))
        self.make_edit = QLineEdit()
        make_layout.addWidget(self.make_edit)
        search_layout.addLayout(make_layout)

        # Model
        model_layout = QHBoxLayout()
        model_layout.addWidget(QLabel("Model:"))
        self.model_edit = QLineEdit()
        model_layout.addWidget(self.model_edit)
        search_layout.addLayout(model_layout)

        # Year
        year_layout = QHBoxLayout()
        year_layout.addWidget(QLabel("Year:"))
        self.year_edit = QLineEdit()
        year_layout.addWidget(self.year_edit)
        search_layout.addLayout(year_layout)

        # Mileage
        mileage_layout = QHBoxLayout()
        mileage_layout.addWidget(QLabel("Min Mileage:"))
        self.min_mileage_edit = QLineEdit()
        mileage_layout.addWidget(self.min_mileage_edit)
        mileage_layout.addWidget(QLabel("Max Mileage:"))
        self.max_mileage_edit = QLineEdit()
        mileage_layout.addWidget(self.max_mileage_edit)
        search_layout.addLayout(mileage_layout)

        # Search Button
        self.search_button = QPushButton("Search")
        self.search_button.clicked.connect(self.search_callback)
        search_layout.addWidget(self.search_button)

        self.setLayout(search_layout)
