import sys
from PyQt5.QtWidgets import QApplication
from mpi4py import MPI
from gui.main_window import MainWindow
from analysis.search_analysis import SearchAnalyzer


def gui_main(comm, rank, size, vehicle_df, test_df):
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
