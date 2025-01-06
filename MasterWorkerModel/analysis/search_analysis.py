import pickle

import pandas as pd
from mpi4py import MPI


class SearchAnalyzer:
    def __init__(self, comm, rank, size):
        self.comm = comm
        self.rank = rank
        self.size = size
        self.num_blocks = self.size * 4  # You can adjust the number of blocks

    def search_by_make(self, df, make):
        """Searches for vehicles of a specific make."""
        return df[df['make'] == make.upper()]

    def search_by_model(self, df, model):
        """Searches for vehicles of a specific model."""
        return df[df['model'] == model.upper()]

    def search_by_year(self, df, year):
        """Searches for vehicles first used in a specific year."""
        return df[pd.to_datetime(df['first_use_date']).dt.year == year]

    def search_by_mileage_range(self, df, min_mileage, max_mileage):
        """Searches for tests within a specific mileage range."""
        return df[(df['test_mileage'] >= min_mileage) & (df['test_mileage'] <= max_mileage)]

    def combined_search(self, local_vehicle_df, local_test_df, make=None, model=None, year=None, min_mileage=None,
                        max_mileage=None):
        """
        Performs a combined search based on multiple criteria.

        Args:
            local_vehicle_df (pd.DataFrame): The local vehicle DataFrame.
            local_test_df (pd.DataFrame): The local test DataFrame.
            make (str, optional): The make to search for.
            model (str, optional): The model to search for.
            year (int, optional): The year of first use to search for.
            min_mileage (int, optional): The minimum mileage.
            max_mileage (int, optional): The maximum mileage.

        Returns:
            pd.DataFrame: A DataFrame containing the matching results.
        """

        print(f"Rank {self.rank}: Entering combined_search")

        # Start with all vehicles on this worker
        filtered_vehicles = local_vehicle_df.copy()

        # Filter by make, model, and year if provided
        if make:
            filtered_vehicles = self.search_by_make(filtered_vehicles, make)
        if model:
            filtered_vehicles = self.search_by_model(filtered_vehicles, model)
        if year:
            filtered_vehicles = self.search_by_year(filtered_vehicles, year)

        # If mileage range is provided, filter tests and then get corresponding vehicles
        if min_mileage is not None and max_mileage is not None:
            filtered_tests = self.search_by_mileage_range(local_test_df, min_mileage, max_mileage)
            # Get the vehicle_ids from the filtered tests
            filtered_vehicle_ids = filtered_tests['vehicle_id'].unique()
            # Filter vehicles based on the vehicle_ids from the filtered tests
            filtered_vehicles = filtered_vehicles[filtered_vehicles['vehicle_id'].isin(filtered_vehicle_ids)]

        # Merge to get all details
        if not filtered_vehicles.empty and not local_test_df.empty:
            merged_df = pd.merge(filtered_vehicles, local_test_df, on='vehicle_id')
        else:
            # Create an empty DataFrame with the desired columns if one of them is empty
            merged_df = pd.DataFrame(columns=['test_id', 'vehicle_id', 'test_date', 'test_class_id', 'test_type',
                                              'test_result', 'test_mileage', 'postcode_area', 'make', 'model',
                                              'colour', 'fuel_type', 'cylinder_capacity', 'first_use_date'])

        # Select and reorder columns to match the desired output
        columns_to_display = ['test_id', 'vehicle_id', 'test_date', 'test_class_id', 'test_type',
                              'test_result', 'test_mileage', 'postcode_area', 'make', 'model',
                              'colour', 'fuel_type', 'cylinder_capacity', 'first_use_date']

        # More robust column selection
        merged_df = merged_df.reindex(columns=columns_to_display, fill_value=None)

        print(f"Rank {self.rank}: Exiting combined_search")
        return merged_df

    def master_process(self, vehicle_df, test_df, search_criteria):
        """Handles the master process logic with Block-Cyclic and Dynamic Mapping."""
        print("Master: Entering master_process for combined search")

        # 1. Block-Cyclic Decomposition
        vehicle_chunks = [pd.DataFrame() for _ in range(self.size)]
        test_chunks = [pd.DataFrame() for _ in range(self.size)]

        vehicle_block_size = len(vehicle_df) // self.num_blocks
        test_block_size = len(test_df) // self.num_blocks

        for i in range(self.num_blocks):
            vehicle_start = i * vehicle_block_size
            vehicle_end = (i + 1) * vehicle_block_size
            test_start = i * test_block_size
            test_end = (i + 1) * test_block_size

            process_id = i % self.size

            vehicle_chunks[process_id] = pd.concat([vehicle_chunks[process_id], vehicle_df[vehicle_start:vehicle_end]])
            test_chunks[process_id] = pd.concat([test_chunks[process_id], test_df[test_start:test_end]])

        print("Master: Block-Cyclic Decomposition complete")

        # 2. Create a single list of search criteria (not sub-queries)
        print("Master: Creating search criteria list")
        search_criteria_list = []
        if search_criteria.get('make'):
            search_criteria_list.append({'type': 'make', 'value': search_criteria['make']})
        if search_criteria.get('model'):
            search_criteria_list.append({'type': 'model', 'value': search_criteria['model']})
        if search_criteria.get('year'):
            search_criteria_list.append({'type': 'year', 'value': search_criteria['year']})
        if search_criteria.get('min_mileage') is not None and search_criteria.get('max_mileage') is not None:
            search_criteria_list.append({'type': 'mileage', 'min_value': search_criteria['min_mileage'],
                                         'max_value': search_criteria['max_mileage']})
        print(f"Master: Created search criteria list with {len(search_criteria_list)} criteria")

        # 3. Dynamic Task Assignment
        print("Master: Assigning tasks to workers")
        tasks = []
        for i in range(1, self.size):
            tasks.append({
                'vehicle_chunk': vehicle_chunks[i - 1],
                'test_chunk': test_chunks[i - 1],
                'search_criteria_list': search_criteria_list  # Send the entire list
            })

        print(f"Master: Created {len(tasks)} tasks")

        available_workers = list(range(1, self.size))
        task_counter = 0
        results = []

        while True:
            # Send tasks to available workers
            while available_workers and task_counter < len(tasks):
                worker_id = available_workers.pop(0)
                task = tasks[task_counter]

                # Serialize DataFrames using pickle to bytes
                vehicle_bytes = pickle.dumps(task['vehicle_chunk'])
                test_bytes = pickle.dumps(task['test_chunk'])
                search_criteria_bytes = pickle.dumps(task['search_criteria_list'])  # Serialize the list

                # Send data size first, then data
                self.comm.send(len(vehicle_bytes), dest=worker_id, tag=0)  # Vehicle size
                self.comm.send(vehicle_bytes, dest=worker_id, tag=1)  # Vehicle data
                self.comm.send(len(test_bytes), dest=worker_id, tag=2)  # Test size
                self.comm.send(test_bytes, dest=worker_id, tag=3)  # Test data
                self.comm.send(len(search_criteria_bytes), dest=worker_id, tag=4)  # Search criteria size
                self.comm.send(search_criteria_bytes, dest=worker_id, tag=5)  # Search criteria

                task_counter += 1

            if task_counter >= len(tasks) and len(results) == len(tasks):
                print("Master: All tasks sent and results received. Breaking loop.")
                break

            # Receive results
            if len(results) < task_counter:
                print("Master: Waiting for results...")
                result = self.comm.recv(source=MPI.ANY_SOURCE, tag=6)
                results.append(result)
                print(f"Master: Received result from worker {result[1]}")

                worker_id = result[1]
                available_workers.append(worker_id)

        # 4. Aggregate Results
        print("Master: Aggregating results")
        received_results = []
        for r in results:
            received_results.append(r[0])

        combined_results = pd.concat(received_results)
        print("Master: Exiting master_process")
        return combined_results

    def worker_process(self, vehicle_df, test_df):
        while True:
            try:
                # Signal availability to the master
                print(f"Worker {self.rank}: Signaling availability to master")
                self.comm.send(None, dest=0, tag=0)



                    # Receive vehicle data size and data
                print(f"Worker {self.rank}: Waiting for vehicle data size")
                vehicle_size = self.comm.recv(source=0, tag=0)
                print(f"Worker {self.rank}: Received vehicle data size: {vehicle_size}")
                vehicle_data = self.comm.recv(source=0, tag=1)
                local_vehicle_df = pickle.loads(vehicle_data)

                # Receive test data size and data
                print(f"Worker {self.rank}: Waiting for test data size")
                test_size = self.comm.recv(source=0, tag=2)
                print(f"Worker {self.rank}: Received test data size: {test_size}")
                test_data = self.comm.recv(source=0, tag=3)
                local_test_df = pickle.loads(test_data)

                # Receive search criteria size and data
                print(f"Worker {self.rank}: Waiting for search criteria size")
                search_criteria_size = self.comm.recv(source=0, tag=4)
                print(f"Worker {self.rank}: Received search criteria size: {search_criteria_size}")
                search_criteria_data = self.comm.recv(source=0, tag=5)
                search_criteria_list = pickle.loads(search_criteria_data)  # Deserialize the list

                # Perform Search with ALL criteria
                print(f"Worker {self.rank}: Performing search with criteria: {search_criteria_list}")
                search_kwargs = {}
                for criteria in search_criteria_list:
                    if criteria['type'] == 'make':
                        search_kwargs['make'] = criteria['value']
                    elif criteria['type'] == 'model':
                        search_kwargs['model'] = criteria['value']
                    elif criteria['type'] == 'year':
                        search_kwargs['year'] = criteria['value']
                    elif criteria['type'] == 'mileage':
                        search_kwargs['min_mileage'] = criteria['min_value']
                        search_kwargs['max_mileage'] = criteria['max_value']

                local_results = self.combined_search(local_vehicle_df, local_test_df, **search_kwargs)

                # Send results back to master along with worker ID
                print(f"Worker {self.rank}: Sending results back to master")
                self.comm.send((local_results, self.rank), dest=0, tag=6)  # Tag 6 for results

                # Check for termination signal
                if self.comm.iprobe(source=0, tag=7):  # Tag 7 for termination
                    print(f"Worker {self.rank}: Received termination signal.")
                    self.comm.recv(source=0, tag=7)  # Actually receive the signal
                    print(f"Worker {self.rank}: Exiting.")
                    break

            except Exception as e:
                print(f"Worker {self.rank}: Error occurred: {e}")
                break