import pandas as pd


class DataCleaner:
    """
    Handles cleaning of MOT data, with a separate function for each column.
    """

    def clean_test_id(self, value):
        # No specific cleaning needed for test_id, assuming it's a unique identifier
        return value

    def clean_vehicle_id(self, value):
        # No specific cleaning needed for vehicle_id, assuming it's an anonymized identifier
        return value

    def clean_test_date(self, value):
        try:
            return pd.to_datetime(value)
        except ValueError:
            print(f"Warning: Invalid date format: {value}")
            return None

    def clean_test_class_id(self, value):
        # Assuming test_class_id is a categorical value, no specific cleaning needed
        return value

    def clean_test_type(self, value):
        # Assuming test_type is a categorical value, no specific cleaning needed
        return value

    def clean_test_result(self, value):
        # Assuming test_result is a categorical value, no specific cleaning needed
        return value

    def clean_test_mileage(self, value):
        try:
            mileage = int(value)
            if mileage < 0:
                print(f"Warning: Negative mileage found: {mileage}")
                return 0  # Or handle it differently (e.g., set to 0, set to a specific value, etc.)
            return mileage
        except (ValueError, TypeError):
            # print(f"Warning: Invalid mileage value: {value}")
            return 0

    def clean_postcode_area(self, value):
        # No specific cleaning needed for postcode_area, assuming it's a categorical value
        return value

    def clean_make(self, value):
        # You might want to clean make names by converting to uppercase and handling variations
        return value.upper().strip() if value else None

    def clean_model(self, value):
        # You might want to clean model names by converting to uppercase and handling variations
        return value.upper().strip() if value else None

    def clean_colour(self, value):
        # You might want to clean colour names by converting to uppercase and handling variations
        return value.upper().strip() if value else None

    def clean_fuel_type(self, value):
        # You might want to clean fuel_type names by converting to uppercase and handling variations
        return value.upper().strip() if value else None

    def clean_cylinder_capacity(self, value):
        try:
            return int(value)
        except (ValueError, TypeError):
            # print(f"Warning: Invalid cylinder capacity: {value}")
            return 0

    def clean_first_use_date(self, value):
        try:
            return pd.to_datetime(value)
        except ValueError:
            print(f"Warning: Invalid date format: {value}")
            return None

    def clean_row(self, row):
        """
        Cleans a single row of MOT data using the individual column cleaning functions.

        Args:
            row (dict): A dictionary representing a row of data from the CSV file.

        Returns:
            dict: A cleaned dictionary representing the row.
        """
        cleaned_row = {}
        for key, value in row.items():
            clean_func = getattr(self, f"clean_{key}", None)  # Get the cleaning function for the key
            if clean_func:
                cleaned_row[key] = clean_func(value)
            else:
                cleaned_row[key] = value  # If no cleaning function found, keep the original value

        return cleaned_row