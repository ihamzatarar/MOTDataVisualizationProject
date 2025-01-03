import pandas as pd

class DataFrameCreator:
    """
    Handles the creation of the vehicle and test DataFrames from the combined MOT data.
    """

    def create_vehicle_df(self, df):
        """
        Creates the vehicle DataFrame with unique vehicles.

        Args:
            df (pd.DataFrame): The combined DataFrame from all worker nodes.
        """
        vehicle_df = df.groupby('vehicle_id').first().reset_index()
        vehicle_df = vehicle_df[['vehicle_id', 'make', 'model', 'colour', 'fuel_type', 'cylinder_capacity', 'first_use_date']]
        return vehicle_df

    def create_test_df(self, df):
        """
        Creates the test DataFrame.

        Args:
            df (pd.DataFrame): The combined DataFrame from all worker nodes.
        """
        test_df = df.drop(['make', 'model', 'colour', 'fuel_type', 'cylinder_capacity', 'first_use_date'], axis=1)

        # Rename columns in test_df to be more descriptive (optional)
        test_df = test_df.rename(columns={
            'test_id': 'test_id',
            'test_date': 'test_date',
            'test_class_id': 'test_class_id',
            'test_type' : 'test_type',
            'test_result': 'test_result',
            'test_mileage': 'test_mileage',
            'postcode_area': 'postcode_area'
        })
        return test_df

    def create_data_frames(self, df):
        """
        Creates the vehicle and test DataFrames.

        Args:
            df (pd.DataFrame): The combined DataFrame from all worker nodes.
        """
        vehicle_df = self.create_vehicle_df(df)
        test_df = self.create_test_df(df)

        print("Vehicle and Test DataFrames created.")

        return vehicle_df, test_df