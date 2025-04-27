import re
from datetime import date
import pandas as pd


def get_turbine_group_from_filename(group_name: str) -> int:
    # Example: data_group_1.csv → group 1, turbines 1-5
    match = re.match(r'data_group_(\d+)', group_name)
    if match:
        return int(match.group(1))
    else:
        raise ValueError(f"Invalid file name format: {group_name}")


def filter_today_data(df: pd.DataFrame, target_date: date) -> pd.DataFrame:
    """
    Filters the dataframe to only include rows from the target_date.
    """
    mask = df['timestamp'].dt.date == target_date
    return df[mask].copy()