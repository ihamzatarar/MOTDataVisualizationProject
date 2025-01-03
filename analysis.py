import pandas as pd

def calculate_pass_rate_by_age(merged_data):
    """
    Calculates the pass rate for vehicles based on their age.

    Args:
        merged_data: A Pandas DataFrame containing merged vehicle and test data.
                     It should have columns 'test_result', 'test_date', and 'first_use_date'.

    Returns:
        A dictionary where keys are ages (integers) and values are the corresponding pass rates (floats).
    """
    # Convert 'test_date' and 'first_use_date' to datetime objects if they are not already
    merged_data['test_date'] = pd.to_datetime(merged_data['test_date'])
    merged_data['first_use_date'] = pd.to_datetime(merged_data['first_use_date'])

    # Calculate the age of the vehicle at the time of the test
    merged_data['age'] = merged_data['test_date'].dt.year - merged_data['first_use_date'].dt.year

    # Group by age and calculate pass rates
    pass_counts = merged_data[merged_data['test_result'] == 'P'].groupby('age').size()
    total_counts = merged_data.groupby('age').size()
    pass_rates = (pass_counts / total_counts).fillna(0)  # Handle cases where total_counts might be 0

    return pass_rates.to_dict()

def calculate_pass_rate_by_mileage(merged_data):
    """
    Calculates the pass rate for vehicles based on their mileage.

    Args:
        merged_data: A Pandas DataFrame containing merged vehicle and test data.
                     It should have columns 'test_result' and 'test_mileage'.

    Returns:
        A dictionary where keys are mileage ranges (strings) and values are the corresponding pass rates (floats).
    """
    # Convert 'test_mileage' to numeric if it's not already
    merged_data['test_mileage'] = pd.to_numeric(merged_data['test_mileage'], errors='coerce')

    # Define mileage ranges (you can adjust the bins as needed)
    mileage_bins = range(0, 200001, 10000)  # 0-10k, 10k-20k, ..., 190k-200k
    mileage_labels = [f"{i}-{i + 10000}" for i in mileage_bins[:-1]]

    # Categorize mileage into ranges
    merged_data['mileage_range'] = pd.cut(merged_data['test_mileage'], bins=mileage_bins, labels=mileage_labels, right=False)

    # Group by mileage range and calculate pass rates
    pass_counts = merged_data[merged_data['test_result'] == 'P'].groupby('mileage_range').size()
    total_counts = merged_data.groupby('mileage_range').size()
    pass_rates = (pass_counts / total_counts).fillna(0)

    return pass_rates.to_dict()