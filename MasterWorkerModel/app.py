import os
from mpi4py import MPI
import pandas as pd
import gui.gui_main as gui
from data.modules.data_loader import MasterWorkerDataLoader
from data.modules.data_cleaner import DataCleaner

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def main():
    data_cleaner = DataCleaner()
    # Load data using the MasterWorkerDataLoader
    data_loader = MasterWorkerDataLoader(data_cleaner, rows_per_file=1000)
    vehicle_df, test_df = data_loader.load_data()

    # Start the GUI
    # if vehicle_df is not None and test_df is not None:
    gui.gui_main(comm, rank, size, vehicle_df, test_df)


if __name__ == "__main__":
    main()