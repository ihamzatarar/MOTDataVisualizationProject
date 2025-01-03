import pandas as pd
from mpi4py import MPI

class SearchAnalyzer:
    def __init__(self, comm, rank, size):
        self.comm = comm
        self.rank = rank
        self.size = size


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

    def distribute_search(self, vehicle_df, test_df, make=None, model=None, year=None, min_mileage=None, max_mileage=None):
        """
        Distributes the search operation among MPI processes.
        """
        print(f"Rank {self.rank}: Entering distribute_search")  # Debug print

        if self.rank == 0:
            # Master node
            # Check if DataFrames are empty
            if vehicle_df is None or vehicle_df.empty:
                print("Rank 0: vehicle_df is empty. Returning empty results.")
                return pd.DataFrame()  # Return an empty DataFrame

            if test_df is None or test_df.empty:
                print("Rank 0: test_df is empty. Returning empty results.")
                return pd.DataFrame()

                # Divide the data into chunks for each worker
            vehicle_chunks = [pd.DataFrame() for _ in range(self.size)]
            test_chunks = [pd.DataFrame() for _ in range(self.size)]

            if vehicle_df is not None:
                vehicle_df.reset_index(drop=True, inplace=True)
                vehicle_chunks = [vehicle_df[i::self.size] for i in range(self.size)]
            if test_df is not None:
                test_chunks = [test_df[i::self.size] for i in range(self.size)]

            # Debug prints for master node
            print(f"Rank {self.rank}: vehicle_chunks length: {len(vehicle_chunks)}")
            print(f"Rank {self.rank}: test_chunks length: {len(test_chunks)}")
            for i, chunk in enumerate(vehicle_chunks):
                print(f"Rank {self.rank}: vehicle_chunks[{i}] shape: {chunk.shape}")
            for i, chunk in enumerate(test_chunks):
                print(f"Rank {self.rank}: test_chunks[{i}] shape: {chunk.shape}")

        else:
            vehicle_chunks = None
            test_chunks = None

        # Scatter the data chunks to worker nodes
        local_vehicle_df = self.comm.scatter(vehicle_chunks, root=0)
        local_test_df = self.comm.scatter(test_chunks, root=0)

        # Debug prints for all nodes
        print(f"Rank {self.rank}: Received local_vehicle_df with shape: {local_vehicle_df.shape if local_vehicle_df is not None else 'None'}")
        print(f"Rank {self.rank}: Received local_test_df with shape: {local_test_df.shape if local_test_df is not None else 'None'}")

        # Perform search on each worker node
        local_results = self.combined_search(local_vehicle_df, local_test_df, make, model, year, min_mileage, max_mileage)

        # Debug print after combined_search
        print(f"Rank {self.rank}: combined_search completed, results shape: {local_results.shape if local_results is not None else 'None'}")

        # Gather the results from all worker nodes
        all_results = self.comm.gather(local_results, root=0)
        print(f"Rank {self.rank}: Gather completed")  # Debug print

        if self.rank == 0:
            # Combine the results on the master node
            combined_results = pd.concat(all_results)
            return combined_results
        else:
            return None

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

        # ...
        print(f"Rank {self.rank}: Entering combined_search")
        # ...

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