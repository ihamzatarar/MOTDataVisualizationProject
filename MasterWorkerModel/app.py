from analysis.search_analysis import SearchAnalyzer
from data.modules.data_cleaner import DataCleaner
from data.modules.data_loader import DataLoader
import gui.gui_main as gui_main  # Corrected import
import os
from mpi4py import MPI
import pandas as pd

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def main():
    data_cleaner = DataCleaner()

    if rank == 0:
        # Master process
        data_loader = DataLoader(data_cleaner, rows_per_file=10000)
        if os.path.isfile("database/local_db/vehicle_df.pkl"):
            vehicle_df = pd.read_pickle("database/local_db/vehicle_df.pkl")
            test_df = pd.read_pickle("database/local_db/test_df.pkl")
            print("DataFrames loaded from Pickle files.")
        else:
            print("Loading data from CSV...")
            vehicle_df, test_df = data_loader.load_and_clean_data()  # Modified method

        # Divide data into chunks for workers (simple partitioning for now)
        vehicle_chunks = [vehicle_df[i::size - 1] for i in range(size - 1)]
        test_chunks = [test_df[i::size - 1] for i in range(size - 1)]

        # Start the GUI
        gui_main.gui_main(comm, rank, size, vehicle_chunks, test_chunks)

    else:
        # Worker process
        search_analyzer = SearchAnalyzer(comm, rank, size)  # Initialize SearchAnalyzer

        # Worker waits for tasks
        while True:
            task = comm.recv(source=0, tag=2)  # Tag 2 for tasks
            if task is None:
                break  # Exit signal

            result = search_analyzer.handle_task(task) # Modified method
            comm.send(result, dest=0, tag=3)  # Tag 3 for results

if __name__ == "__main__":
    main()