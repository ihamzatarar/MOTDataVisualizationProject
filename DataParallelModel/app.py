from data.modules.data_cleaner import DataCleaner
from data.modules.data_loader import DataLoader
import gui.gui_main as gui  # Import the GUI code
import os
from mpi4py import MPI
import pandas as pd
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def main():
    # Initialize DataCleaner and DataLoader
    data_cleaner = DataCleaner()

    # Set load_all_rows to False to load 50,000 rows, True to load 1,000,000 rows
    load_all_rows = False
    rows_per_file = 10000 if not load_all_rows else 1000000

    data_loader = DataLoader(data_cleaner, rows_per_file)
    # Distribute work and get the DataFrames on the master node

    if os.path.isfile("database/local_db/vehicle_df.pkl"):
        if rank == 0:
            vehicle_df = pd.read_pickle("database/local_db/vehicle_df.pkl")
            test_df = pd.read_pickle("database/local_db/test_df.pkl")
            print("DataFrames loaded from Pickle files.")
        else:
            vehicle_df = None
            test_df = None
    else:
        print("Loading data from CSV...")
        vehicle_df, test_df = data_loader.distribute_work()


    # Start the GUI and SearchAnalyzer
    gui.gui_main(comm, rank, size, vehicle_df, test_df)

if __name__ == "__main__":
    main()