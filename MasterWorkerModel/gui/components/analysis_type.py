# gui/components/analysis_type.py

from PyQt5.QtWidgets import QGroupBox, QVBoxLayout, QRadioButton

class AnalysisTypeGroup(QGroupBox):
    def __init__(self):
        super().__init__("Analysis Type")
        layout = QVBoxLayout()

        self.analysis_age_radio = QRadioButton("Analyze by Age")
        self.analysis_age_radio.setChecked(True)  # Default selection
        self.analysis_mileage_radio = QRadioButton("Analyze by Mileage")

        layout.addWidget(self.analysis_age_radio)
        layout.addWidget(self.analysis_mileage_radio)

        self.setLayout(layout)
