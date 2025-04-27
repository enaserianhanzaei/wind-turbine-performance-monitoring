import pytest
import pandas as pd
from ingestion.cleaning import remove_duplicates, handle_missing_values, clean_physical_limits, \
    detect_and_handle_outliers_statistically_std, clean_data

# Sample data for testing
sample_data = {
    "timestamp": pd.date_range("2025-04-01", periods=6, freq="5T"),
    "turbine_id": [1, 1, 1, 2, 2, 2],
    "wind_speed": [10, 15, 12, 5, 0, 8],
    "power_output": [100, 150, 120, 50, 0, 70],
    "wind_direction": [90, 90, 90, 180, 180, 180],
}


@pytest.fixture
def sample_dataframe():
    """Fixture to provide a simple DataFrame for testing."""
    return pd.DataFrame(sample_data)


def test_remove_duplicates(sample_dataframe):
    # Introduce duplicate rows to test the removal function
    df_with_duplicates = pd.concat([sample_dataframe, sample_dataframe.iloc[0:1]], ignore_index=True)
    df_cleaned = remove_duplicates(df_with_duplicates)
    assert len(df_cleaned) == len(sample_dataframe), "Duplicates not removed properly."


def test_handle_missing_values(sample_dataframe):
    # Introduce missing values to test handling
    df_with_missing_values = sample_dataframe.copy()
    df_with_missing_values.loc[2, "wind_speed"] = None
    df_with_missing_values.loc[4, "power_output"] = None

    # Test that missing values are handled (forward filled)
    df_cleaned = handle_missing_values(df_with_missing_values)

    # Check that no missing values are left in sensor columns
    assert not df_cleaned["wind_speed"].isnull().any(), "Missing wind_speed values not handled properly."
    assert not df_cleaned["power_output"].isnull().any(), "Missing power_output values not handled properly."


def test_clean_physical_limits(sample_dataframe):
    # Set physical limits for wind_speed
    sensor_limits = {"wind_speed": {"min": 5, "max": 20}}

    df_with_out_of_bounds = sample_dataframe.copy()
    df_with_out_of_bounds.loc[3, "wind_speed"] = 25  # Outside the limit
    df_with_out_of_bounds.loc[4, "wind_speed"] = 3  # Outside the limit

    df_cleaned = clean_physical_limits(df_with_out_of_bounds, sensor_limits)

    # Assert the out-of-bounds rows are dropped
    assert len(df_cleaned) == len(sample_dataframe) - 2, "Rows outside physical limits not removed correctly."


def test_clean_data(sample_dataframe):
    # Set physical limits for wind_speed
    sensor_limits = {"wind_speed": {"min": 5, "max": 20}}

    df_with_missing_values = sample_dataframe.copy()
    df_with_missing_values.loc[2, "wind_speed"] = None
    df_with_missing_values.loc[4, "wind_speed"] = None
    df_with_out_of_bounds = df_with_missing_values.copy()
    df_with_out_of_bounds.loc[2, "wind_speed"] = 25  # Outside the limit
    df_with_out_of_bounds.loc[4, "wind_speed"] = 3  # Outside the limit

    # Running the full cleaning pipeline
    df_cleaned = clean_data(df_with_out_of_bounds, sensor_limits)

    # Assert that after cleaning, no missing values, no outliers, and within limits
    assert not df_cleaned["wind_speed"].isnull().any(), "Missing wind_speed values not handled properly."
    assert not df_cleaned["power_output"].isnull().any(), "Missing power_output values not handled properly."
    assert len(df_cleaned) == len(sample_dataframe) - 2, "Rows outside physical limits not removed correctly."
