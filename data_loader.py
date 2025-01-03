import pandas as pd
import csv
import os
from mpi4py import MPI
from data_frames import DataFrameCreator
from data_cleaner import DataCleaner  # Import the DataCleaner class


class DataLoader:
    """
    Handles loading and distributing the MOT dataset using MPI.
    """

    def __init__(self, data_cleaner, rows_per_file=1000000):
        if rows_per_file <= 0:
            raise ValueError("rows_per_file must be a positive integer.")
        self.data_cleaner = data_cleaner
        self.rows_per_file = rows_per_file
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()

    def process_file(self, filename, start_row):
        """
        Processes a portion of a CSV file, cleans the data, and returns a Pandas DataFrame.

        Args:
            filename (str): The path to the CSV file.
            start_row (int): The row number to start processing from.

        Returns:
            pandas.DataFrame: A DataFrame containing the cleaned data.
        """
        data = []
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i < start_row:
                    continue
                if i >= start_row + self.rows_per_file:
                    break

                cleaned_row = self.data_cleaner.clean_row(row)
                data.append(cleaned_row)

                if (i + 1) % 1000 == 0:
                    print(f"Rank {self.rank}: Processed {i + 1} rows from {filename}")

        return pd.DataFrame(data)

    def distribute_work(self):
        """
        Distributes the work of loading and cleaning data among MPI processes.
        """
        if self.rank == 0:  # Master node
            csv_files = [f"./test_result_2022/{f}" for f in os.listdir('./test_result_2022') if f.endswith('.csv')]
            chunks = [[] for _ in range(self.size)]
            for i, file in enumerate(csv_files):
                chunks[i % self.size].append(file)

            print(f"Master node: Found {len(csv_files)} CSV files.")
        else:
            chunks = None

        # Scatter the work
        files_to_process = self.comm.scatter(chunks, root=0)

        local_df = pd.DataFrame()
        for file in files_to_process:
            print(f"Rank {self.rank} processing {file}")
            df_chunk = self.process_file(file, 0)
            local_df = pd.concat([local_df, df_chunk], ignore_index=True)
            print(f"Rank {self.rank} finished processing {file}")

        # Gather data on master for further processing (optional)
        all_data = self.comm.gather(local_df, root=0)

        if self.rank == 0:
            # If needed, combine data from all workers
            # final_df = pd.concat(all_data, ignore_index=True)
            final_df = pd.concat(all_data, ignore_index=True)
            print("Data loading and cleaning complete.")
            # ... (Further processing on master, if required)
            # Ensure 'vehicle_id' is a column


            df_creator = DataFrameCreator()
            vehicle_df, test_df = df_creator.create_data_frames(final_df)
            if not os.path.exists("local_db"):
                # Create the directory
                os.makedirs("local_db")
            vehicle_df.to_pickle("local_db/vehicle_df.pkl")
            test_df.to_pickle("local_db/test_df.pkl")

            return vehicle_df, test_df  # Return the DataFrames
        else:
            return None, None