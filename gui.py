import sys
import pandas as pd
from PyQt5.QtWidgets import (
    QApplication, QWidget, QLabel, QLineEdit, QPushButton,
    QVBoxLayout, QHBoxLayout, QMessageBox, QTableView, QAbstractItemView, QGroupBox, QRadioButton
)
from PyQt5.QtCore import Qt, QAbstractTableModel
from PyQt5.QtGui import QFont
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
from mpi4py import MPI
import matplotlib

matplotlib.use('Qt5Agg')  # Use the Qt5Agg backend for Matplotlib

# Import your SearchAnalyzer class
from search_analysis import SearchAnalyzer
from analysis import Analyzer
from gui.utils import PandasModel, draw_figure , MatplotlibCanvas
# --- Helper Functions for Matplotlib ---


class MainWindow(QWidget):
    def __init__(self, comm, rank, size, vehicle_df, test_df):
        super().__init__()

        self.comm = comm
        self.rank = rank
        self.size = size
        self.search_analyzer = SearchAnalyzer(comm, rank, size)
        self.analyzer = Analyzer()
        self.vehicle_df = vehicle_df
        self.test_df = test_df
        self.figure_canvas_agg = None

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

        search_group.setLayout(search_layout)

        # --- Analysis Criteria ---
        analysis_group = QGroupBox("Analysis")
        analysis_layout = QVBoxLayout()

        make_layout = QHBoxLayout()
        make_layout.addWidget(QLabel("Make:"))
        self.analysis_make_edit = QLineEdit()
        make_layout.addWidget(self.analysis_make_edit)
        analysis_layout.addLayout(make_layout)

        model_layout = QHBoxLayout()
        model_layout.addWidget(QLabel("Model:"))
        self.analysis_model_edit = QLineEdit()
        model_layout.addWidget(self.analysis_model_edit)
        analysis_layout.addLayout(model_layout)

        self.analysis_age_radio = QRadioButton("Analyze by Age")
        self.analysis_age_radio.setChecked(True)  # Default to analysis by age
        self.analysis_mileage_radio = QRadioButton("Analyze by Mileage")
        analysis_layout.addWidget(self.analysis_age_radio)
        analysis_layout.addWidget(self.analysis_mileage_radio)

        self.analyze_button = QPushButton("Analyze")
        self.analyze_button.clicked.connect(self.analyze)
        analysis_layout.addWidget(self.analyze_button)

        analysis_group.setLayout(analysis_layout)

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
        left_layout.addWidget(analysis_group)
        main_layout.addLayout(left_layout)
        right_layout = QVBoxLayout()
        right_layout.addWidget(results_group)
        right_layout.addWidget(plot_group)
        main_layout.addLayout(right_layout)

        self.setLayout(main_layout)

        # Disable GUI interaction for worker nodes
        if self.rank != 0:
            self.setEnabled(False)

    def search(self):
        # Only the master node (rank 0) gathers input
        if self.rank == 0:
            make = self.search_make_edit.text()
            model = self.search_model_edit.text()
            try:
                year = int(self.search_year_edit.text()) if self.search_year_edit.text() else None
            except ValueError:
                QMessageBox.warning(self, "Input Error", "Year must be a number.")
                return

            try:
                min_mileage = int(self.search_min_mileage_edit.text()) if self.search_min_mileage_edit.text() else None
            except ValueError:
                QMessageBox.warning(self, "Input Error", "Minimum mileage must be a number.")
                return

            try:
                max_mileage = int(self.search_max_mileage_edit.text()) if self.search_max_mileage_edit.text() else None
            except ValueError:
                QMessageBox.warning(self, "Input Error", "Maximum mileage must be a number.")
                return

            if min_mileage is not None and max_mileage is not None and min_mileage > max_mileage:
                QMessageBox.warning(self, "Input Error", "Minimum mileage cannot be greater than maximum mileage.")
                return

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

        # Perform the search operation (only on master after gathering results)
        if self.rank == 0:
          self.perform_search(search_criteria)

    def perform_search(self, search_criteria):
        # All nodes perform the search
        results = self.search_analyzer.distribute_search(
            self.vehicle_df, self.test_df, **search_criteria
        )

        # Display results only on the master node
        if self.rank == 0:
            if results is not None and not results.empty:
                model = PandasModel(results)
                self.results_table.setModel(model)
            else:
                QMessageBox.information(self, "Search Results", "No results found.")

    def analyze(self):
        # Only the master node (rank 0) gathers input
        if self.rank == 0:
            make = self.analysis_make_edit.text()
            model = self.analysis_model_edit.text()
            analyze_by_age = self.analysis_age_radio.isChecked()

            # Package analysis criteria into a dictionary
            analysis_criteria = {
                'make': make,
                'model': model,
                'analyze_by_age': analyze_by_age
            }
        else:
            analysis_criteria = None

        # Broadcast analysis criteria to all processes
        analysis_criteria = self.comm.bcast(analysis_criteria, root=0)

        # Perform the analysis
        self.perform_analysis(analysis_criteria)

    def perform_analysis(self, analysis_criteria):
        # All nodes perform the analysis
        if self.rank == 0:
            make = analysis_criteria['make']
            model = analysis_criteria['model']
            analyze_by_age = analysis_criteria['analyze_by_age']

            if analyze_by_age:
                fig = self.analyzer.calculate_pass_rate_by_age(self.vehicle_df, self.test_df, make, model)
            else:
                fig = self.analyzer.calculate_pass_rate_by_mileage(self.vehicle_df, self.test_df, make, model)

            # Update the plot only on the master node
            if fig:
                if self.figure_canvas_agg:
                    self.figure_canvas_agg.get_tk_widget().deleteLater()
                self.figure_canvas_agg = draw_figure(self.plot_canvas.get_tk_widget(), fig)
            else:
                QMessageBox.information(self, "Analysis Results", "Could not generate plot.")

def main(comm, rank, size, vehicle_df, test_df):
    app = QApplication(sys.argv)
    main_window = MainWindow(comm, rank, size, vehicle_df, test_df)

    if rank == 0:
        # Only show the window for the master process
        main_window.show()

    # Start the application event loop
    sys.exit(app.exec_())