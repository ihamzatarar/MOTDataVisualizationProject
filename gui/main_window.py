import sys
import pandas as pd
from PyQt5.QtWidgets import (
    QApplication, QWidget, QLabel, QLineEdit, QPushButton,
    QVBoxLayout, QHBoxLayout, QMessageBox, QTableView, QAbstractItemView, QGroupBox, QRadioButton
)
from PyQt5.QtCore import Qt, QAbstractTableModel
from PyQt5.QtGui import QFont
from mpi4py import MPI

# Import your SearchAnalyzer class
from search_analysis import SearchAnalyzer
from analysis import calculate_pass_rate_by_age, calculate_pass_rate_by_mileage
from gui.utils import PandasModel, MatplotlibCanvas, draw_figure

class MainWindow(QWidget):
    def __init__(self, comm, rank, size, vehicle_df, test_df):
        super().__init__()

        self.comm = comm
        self.rank = rank
        self.size = size
        self.search_analyzer = SearchAnalyzer(comm, rank, size)
        self.vehicle_df = vehicle_df
        self.test_df = test_df

        self.setWindowTitle("MOT Data Analysis")

        # --- Search Criteria ---
        search_group = QGroupBox("Search Criteria")
        search_layout = QVBoxLayout()

        make_layout = QHBoxLayout()
        make_layout.addWidget(QLabel("Make:"))
        self.search_make_edit = QLineEdit()
        make_layout.addWidget(self.search_make_edit)
        search_layout.addLayout(make_layout)

        model_layout = QHBoxLayout()
        model_layout.addWidget(QLabel("Model:"))
        self.search_model_edit = QLineEdit()
        model_layout.addWidget(self.search_model_edit)
        search_layout.addLayout(model_layout)

        year_layout = QHBoxLayout()
        year_layout.addWidget(QLabel("Year:"))
        self.search_year_edit = QLineEdit()
        year_layout.addWidget(self.search_year_edit)
        search_layout.addLayout(year_layout)

        mileage_layout = QHBoxLayout()
        mileage_layout.addWidget(QLabel("Min Mileage:"))
        self.search_min_mileage_edit = QLineEdit()
        mileage_layout.addWidget(self.search_min_mileage_edit)
        mileage_layout.addWidget(QLabel("Max Mileage:"))
        self.search_max_mileage_edit = QLineEdit()
        mileage_layout.addWidget(self.search_max_mileage_edit)
        search_layout.addLayout(mileage_layout)

        self.search_button = QPushButton("Search")
        self.search_button.clicked.connect(self.search)
        search_layout.addWidget(self.search_button)

        # --- Analysis Mode Toggle ---
        self.analysis_mode_button = QPushButton("Analysis Mode")
        self.analysis_mode_button.setCheckable(True)  # Make it a toggle button
        self.analysis_mode_button.setChecked(False)  # Initially off
        self.analysis_mode_button.clicked.connect(self.toggle_analysis_mode)
        search_layout.addWidget(self.analysis_mode_button)

        search_group.setLayout(search_layout)

        # --- Analysis Type Selection ---
        analysis_type_group = QGroupBox("Analysis Type")
        analysis_type_layout = QVBoxLayout()

        self.analysis_age_radio = QRadioButton("Analyze by Age")
        self.analysis_age_radio.setChecked(True)  # Default to analysis by age
        self.analysis_mileage_radio = QRadioButton("Analyze by Mileage")
        analysis_type_layout.addWidget(self.analysis_age_radio)
        analysis_type_layout.addWidget(self.analysis_mileage_radio)

        analysis_type_group.setLayout(analysis_type_layout)

        # --- Results ---
        results_group = QGroupBox("Results")
        results_layout = QVBoxLayout()

        self.results_table = QTableView()
        self.results_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.results_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        results_layout.addWidget(self.results_table)

        results_group.setLayout(results_layout)

        # Plot
        plot_group = QGroupBox("Plot")
        plot_layout = QVBoxLayout()

        self.plot_canvas = MatplotlibCanvas(self, width=5, height=4, dpi=100)
        plot_layout.addWidget(self.plot_canvas)

        plot_group.setLayout(plot_layout)

        # --- Main Layout ---
        main_layout = QHBoxLayout()
        left_layout = QVBoxLayout()
        left_layout.addWidget(search_group)
        left_layout.addWidget(analysis_type_group)  # Add analysis type selection
        main_layout.addLayout(left_layout)
        right_layout = QVBoxLayout()
        right_layout.addWidget(results_group)
        right_layout.addWidget(plot_group)
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
            make = self.search_make_edit.text()
            model = self.search_model_edit.text()
            try:
                year = int(self.search_year_edit.text()) if self.search_year_edit.text() else None
            except ValueError:
                QMessageBox.warning(self, "Input Error", "Year must be a number.")
                return None

            try:
                min_mileage = int(self.search_min_mileage_edit.text()) if self.search_min_mileage_edit.text() else None
            except ValueError:
                QMessageBox.warning(self, "Input Error", "Minimum mileage must be a number.")
                return None

            try:
                max_mileage = int(self.search_max_mileage_edit.text()) if self.search_max_mileage_edit.text() else None
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
                self.results_table.setModel(model)

                # If Analysis Mode is ON, perform analysis and display plot
                if self.analysis_mode_button.isChecked():
                    self.analyze_and_display(search_criteria, results)
            else:
                QMessageBox.information(self, "Search Results", "No results found.")

    def analyze_and_display(self, search_criteria, results):
        # Determine analysis type
        analysis_type = "age" if self.analysis_age_radio.isChecked() else "mileage"

        # Perform analysis directly on the results DataFrame
        if analysis_type == "age":
            try:
                # Pass the results DataFrame directly to the analysis function
                analysis_result = calculate_pass_rate_by_age(results)
            except Exception as e:
                QMessageBox.critical(self, "Analysis Error", f"Error during analysis by age: {e}")
                return
        else:  # analysis_type == "mileage":
            try:
                # Pass the results DataFrame directly to the analysis function
                analysis_result = calculate_pass_rate_by_mileage(results)
            except Exception as e:
                QMessageBox.critical(self, "Analysis Error", f"Error during analysis by mileage: {e}")
                return

        # Display plot
        if analysis_result is not None:
            draw_figure(self.plot_canvas, analysis_result, analysis_type, search_criteria['make'], search_criteria['model'])
        else:
            QMessageBox.information(self, "Analysis Results", "Could not generate analysis results.")

def main(comm, rank, size, vehicle_df, test_df):
    app = QApplication(sys.argv)

    # Create and show the main window only for the master process
    if rank == 0:
        main_window = MainWindow(comm, rank, size, vehicle_df, test_df)
        main_window.show()
    else:
        # Worker processes need a SearchAnalyzer instance but not a GUI
        search_analyzer = SearchAnalyzer(comm, rank, size)
        # Keep worker processes alive to participate in the search
        while True:
            # Receive search criteria
            search_criteria = comm.bcast(None, root=0)

            if search_criteria is not None:
                # Perform search
                search_analyzer.distribute_search(vehicle_df, test_df, **search_criteria)
            else:
                # Exit condition (optional, for graceful termination)
                break

    if rank == 0:
        sys.exit(app.exec_())