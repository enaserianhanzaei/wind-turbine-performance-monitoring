import pandas as pd

REQUIRED_COLUMNS = {
    "timestamp": "datetime64[ns]",
    "turbine_id": "int64",
    "wind_speed": "float64",
    "wind_direction": "float64",
    "power_output": "float64",
}


def read_csv_file(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        raise ValueError(f"File not found: {path}")
    except pd.errors.EmptyDataError:
        raise ValueError(f"File is empty: {path}")
    except pd.errors.ParserError:
        raise ValueError(f"File is corrupted or badly formatted: {path}")
    except Exception as e:
        raise ValueError(f"Unexpected error reading {path}: {str(e)}")
    return df


def validate_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Validate column names
    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Enforce data types
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="raise")
        df = df.astype({k: v for k, v in REQUIRED_COLUMNS.items() if k != "timestamp"})
    except Exception as e:
        raise ValueError(f"Data type coercion failed: {e}")

    # Optional: warn on missing critical data
    if df["timestamp"].isnull().any() or df["turbine_id"].isnull().any():
        raise ValueError("Missing critical values in timestamp or turbine_id columns.")

    return df


def read_and_validate_csv(path: str) -> pd.DataFrame:
    df = read_csv_file(path)
    return validate_dataframe(df)