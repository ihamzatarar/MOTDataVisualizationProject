import pandas as pd
from mpi4py import MPI

class SearchAnalyzer:
    def __init__(self, comm, rank, size):
        self.comm = comm
        self.rank = rank
        self.size = size

    def handle_task(self, task):
        """
        Handles a task received from the master process.

        Args:
            task (dict): A dictionary containing task information.

        Returns:
            pd.DataFrame: The result of the task (e.g., search results).
        """
        task_type = task['type']
        data = task['data']
        criteria = task['criteria']

        if task_type == 'search':
            vehicle_df, test_df = data
            result = self.combined_search(vehicle_df, test_df, **criteria)
            return result
        else:
            print(f"Rank {self.rank}: Unknown task type: {task_type}")
            return None

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
        Performs a combined search based on multiple criteria. This is now executed by worker processes.

        Args:
            local_vehicle_df (pd.DataFrame): The local vehicle DataFrame (received as part of a task).
            local_test_df (pd.DataFrame): The local test DataFrame (received as part of a task).
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