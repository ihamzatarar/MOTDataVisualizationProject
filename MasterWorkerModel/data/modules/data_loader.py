import pandas as pd
import csv
import os
from mpi4py import MPI
from data.modules.data_frames import DataFrameCreator

class MasterWorkerDataLoader:
    def __init__(self, data_cleaner, rows_per_file=1000000):
        self.data_cleaner = data_cleaner
        self.rows_per_file = rows_per_file
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()
        self.num_workers = self.size - 1
        self.vehicle_df = None
        self.test_df = None

    def load_data(self):
        if self.rank == 0:
            # Master process
            if os.path.isfile("database/local_db/vehicle_df.pkl"):
                print("Loading data from Pickle files...")
                vehicle_df = pd.read_pickle("database/local_db/vehicle_df.pkl")
                test_df = pd.read_pickle("database/local_db/test_df.pkl")
                print("DataFrames loaded from Pickle files.")
            else:
                vehicle_df, test_df = self.master_process_data_loading()
            self.vehicle_df = vehicle_df
            self.test_df = test_df

        else:
            # Worker process
            if os.path.isfile("database/local_db/vehicle_df.pkl"):
                print("Loading data from Pickle files...")
                vehicle_df = None
                test_df = None
                print("DataFrames loaded from Pickle files.")
            else:
                self.worker_process_data_loading()
        return self.vehicle_df, self.test_df


    def master_process_data_loading(self):
            csv_files = [f"database/test_result_2022/{f}" for f in os.listdir('database/test_result_2022') if
                         f.endswith('.csv')]
            num_files = len(csv_files)

            # Send data to workers
            file_index = 0
            requests = []
            for i in range(1, min(self.num_workers + 1, num_files + 1)):
                req = self.comm.isend(csv_files[file_index], dest=i, tag=file_index)
                requests.append(req)
                file_index += 1

            # Receive processed data from workers
            processed_data = []
            status = MPI.Status()  # Create a Status object
            while file_index < num_files:
                # Wait for any worker to finish
                index = MPI.Request.Waitany(requests)

                # Receive the processed data from the worker
                worker_data = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG,
                                             status=status)  # Corrected line: receive with status
                processed_data.append(worker_data)

                # Get worker rank from the status object
                worker_rank = status.Get_source()

                if file_index < num_files:
                    # Send the next file to the worker that just finished
                    req = self.comm.isend(csv_files[file_index], dest=worker_rank, tag=file_index)
                    requests[index] = req  # Replace the completed request with the new one
                    file_index += 1

            # Wait for remaining requests
            MPI.Request.Waitall(requests)

            # Receive the last batch of processed data
            for _ in range(min(self.num_workers, num_files)):
                worker_data = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)  # Don't need status here
                processed_data.append(worker_data)

            # Concatenate all processed data
            combined_df = pd.concat(processed_data, ignore_index=True)
            df_creator = DataFrameCreator()
            vehicle_df, test_df = df_creator.create_data_frames(combined_df)

            if not os.path.exists("database/local_db"):
                os.makedirs("database/local_db")
            vehicle_df.to_pickle("database/local_db/vehicle_df.pkl")
            test_df.to_pickle("database/local_db/test_df.pkl")

            return vehicle_df, test_df

    def worker_process_data_loading(self):
        while True:
            # Receive a file from the master
            file_to_process = self.comm.recv(source=0, tag=MPI.ANY_TAG)

            if file_to_process is None:
                # No more files to process
                break

            # Process the file
            local_df = pd.DataFrame()
            df_chunk = self.process_file(file_to_process, 0)
            local_df = pd.concat([local_df, df_chunk], ignore_index=True)

            # Send the processed data back to the master
            self.comm.send(local_df, dest=0, tag=self.rank)

    def process_file(self, filename, start_row):
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