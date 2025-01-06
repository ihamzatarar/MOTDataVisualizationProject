import sys
from PyQt5.QtWidgets import QApplication
from mpi4py import MPI
from gui.main_window import MainWindow
from analysis.search_analysis import SearchAnalyzer

def gui_main(comm, rank, size, vehicle_df, test_df):
    print(f"Process {rank}: Entering gui_main")  # Print for all processes

    app = QApplication(sys.argv)

    if rank == 0:
        # Master process
        print("Master process started")
        main_window = MainWindow(comm, rank, size, vehicle_df, test_df)
        main_window.show()

        app.exec_()  # Start the PyQt event loop only for the master



        # Clean shutdown after app closes (for both master and workers)
        print("Master sending termination signal")
        MPI.Finalize()
        for i in range(1, size):
            comm.isend(None, dest=i, tag=5)  # Send termination signal



    else:
        # Worker processes
        print(f"Worker {rank} started")  # Print when a worker starts
        search_analyzer = SearchAnalyzer(comm, rank, size)
        search_analyzer.worker_process(vehicle_df, test_df)

