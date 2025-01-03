# gui/main_window.py
from PyQt5.QtWidgets import (
    QWidget, QHBoxLayout, QVBoxLayout, QGroupBox, QMessageBox, QLabel, QLineEdit, QTableView, QAbstractItemView,
    QPushButton, QRadioButton
)
from PyQt5.QtCore import Qt

from analysis.core import calculate_pass_rate_by_age, calculate_pass_rate_by_mileage
from gui.components.search_criteria import SearchCriteriaGroup
from gui.components.analysis_type import AnalysisTypeGroup
from gui.components.results_display import ResultsGroup
from gui.components.plot_view import PlotGroup
from gui.utils import PandasModel, draw_figure, MatplotlibCanvas
from analysis.search_analysis import SearchAnalyzer
from gui.styles import app_style_sheet  # Import the stylesheet
from gui.components.plot_view import PlotGroup, MatplotlibCanvas


class MainWindow(QWidget):
    def __init__(self, comm, rank, size, vehicle_df, test_df):
        super().__init__()

        self.comm = comm
        self.rank = rank
        self.size = size
        self.search_analyzer = SearchAnalyzer(comm, rank, size)
        self.vehicle_df = vehicle_df
        self.test_df = test_df

        self.setStyleSheet(app_style_sheet)

        self.setWindowTitle("MOT Data Analysis")

        # --- Components ---
        self.search_group = SearchCriteriaGroup(self.search)
        self.analysis_type_group = AnalysisTypeGroup()
        self.results_group = ResultsGroup()
        self.plot_group = PlotGroup()

        # --- Analysis Mode Toggle ---
        self.analysis_mode_button = QPushButton("Analysis Mode")
        self.analysis_mode_button.setCheckable(True)  # Make it a toggle button
        self.analysis_mode_button.setChecked(False)  # Initially off
        self.analysis_mode_button.clicked.connect(self.toggle_analysis_mode)
        self.search_group.layout().addWidget(self.analysis_mode_button)  # Add to the search layout

        # --- Main Layout ---
        main_layout = QHBoxLayout()
        left_layout = QVBoxLayout()
        left_layout.addWidget(self.search_group)
        left_layout.addWidget(self.analysis_type_group)
        main_layout.addLayout(left_layout)

        right_layout = QVBoxLayout()
        right_layout.addWidget(self.results_group)
        right_layout.addWidget(self.plot_group)
        main_layout.addLayout(right_layout)

        self.setLayout(main_layout)


        # Disable GUI interaction for worker nodes
        if self.rank != 0:
            self.setEnabled(False)

    def toggle_analysis_mode(self):
        if self.analysis_mode_button.isChecked():
            self.analysis_mode_button.setText("Analysis Mode ON")
        else:
            self.analysis_mode_button.setText("Analysis Mode OFF")

    def search(self):
        # Only the master node (rank 0) gathers input
        if self.rank == 0:
            make = self.search_group.make_edit.text()
            model = self.search_group.model_edit.text()
            try:
                year = int(self.search_group.year_edit.text()) if self.search_group.year_edit.text() else None
            except ValueError:
                QMessageBox.warning(self, "Input Error", "Year must be a number.")
                return None

            try:
                min_mileage = int(
                    self.search_group.min_mileage_edit.text()) if self.search_group.min_mileage_edit.text() else None
            except ValueError:
                QMessageBox.warning(self, "Input Error", "Minimum mileage must be a number.")
                return None

            try:
                max_mileage = int(
                    self.search_group.max_mileage_edit.text()) if self.search_group.max_mileage_edit.text() else None
            except ValueError:
                QMessageBox.warning(self, "Input Error", "Maximum mileage must be a number.")
                return None

            if min_mileage is not None and max_mileage is not None and min_mileage > max_mileage:
                QMessageBox.warning(self, "Input Error", "Minimum mileage cannot be greater than maximum mileage.")
                return None

            # Package search criteria into a dictionary
            search_criteria = {
                'make': make,
                'model': model,
                'year': year,
                'min_mileage': min_mileage,
                'max_mileage': max_mileage
            }

        else:
            search_criteria = None

        # Broadcast search criteria to all processes
        search_criteria = self.comm.bcast(search_criteria, root=0)

        # All nodes perform the search
        results = self.search_analyzer.distribute_search(
            self.vehicle_df, self.test_df, **search_criteria
        )

        # Display results and perform analysis only on the master node
        if self.rank == 0:
            if results is not None and not results.empty:
                model = PandasModel(results)
                self.results_group.table.setModel(model)

                # If Analysis Mode is ON, perform analysis and display plot
                if self.analysis_mode_button.isChecked():
                    self.analyze_and_display(search_criteria, results)
            else:
                QMessageBox.information(self, "Search Results", "No results found.")

    def analyze_and_display(self, search_criteria, results):
        analysis_type = (
            "age" if self.analysis_type_group.analysis_age_radio.isChecked() else "mileage"
        )

        if analysis_type == "age":
            try:
                analysis_result = calculate_pass_rate_by_age(results)
            except Exception as e:
                QMessageBox.critical(self, "Analysis Error", f"Error during analysis by age: {e}")
                return
        else:  # analysis_type == "mileage"
            try:
                analysis_result = calculate_pass_rate_by_mileage(results)
            except Exception as e:
                QMessageBox.critical(self, "Analysis Error", f"Error during analysis by mileage: {e}")
                return

        if analysis_result is not None:
            draw_figure(
                self.plot_group.plot_canvas, analysis_result, analysis_type,
                search_criteria.get("make"), search_criteria.get("model")
            )
        else:
            QMessageBox.information(self, "Analysis Results", "Could not generate analysis results.")

