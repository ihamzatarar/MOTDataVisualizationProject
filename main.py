from data_loader import DataLoader
from data_cleaner import DataCleaner
from data_frames import DataFrameCreator
import gui.main_window as gui  # Import the GUI code
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
    rows_per_file = 1000000 if not load_all_rows else 1000000

    data_loader = DataLoader(data_cleaner, rows_per_file)
    # Distribute work and get the DataFrames on the master node

    if os.path.isfile("local_db/vehicle_df.pkl"):
        if rank == 0:
            vehicle_df = pd.read_pickle("local_db/vehicle_df.pkl")
            test_df = pd.read_pickle("local_db/test_df.pkl")
            print("DataFrames loaded from Pickle files.")
        else:
            vehicle_df = None
            test_df = None
    else:
        print("Loading data from CSV...")
        vehicle_df, test_df = data_loader.distribute_work()


    # Start the GUI and SearchAnalyzer
    gui.main(comm, rank, size, vehicle_df, test_df)

if __name__ == "__main__":
    main()