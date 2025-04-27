from typing import Optional
import pandas as pd

from ingestion.validation import REQUIRED_COLUMNS, TURBINE_GROUPS
from ingestion.utils import get_turbine_group_from_filename

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def read_csv_file(path: str) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(path)
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {path}")
    except pd.errors.EmptyDataError:
        logger.error(f"File is empty: {path}")
    except pd.errors.ParserError:
        logger.error(f"File is corrupted or badly formatted: {path}")
    except Exception as e:
        logger.error(f"Unexpected error reading {path}: {str(e)}")
    return None


def validate_dataframe(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    # Validate column names
    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        return None

    # Enforce data types
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="raise")
        df = df.astype({k: v for k, v in REQUIRED_COLUMNS.items() if k != "timestamp"})
    except Exception as e:
        logger.error(f"Data type coercion failed: {e}")
        return None

    return df


def validate_turbine_ids(df: pd.DataFrame, group_name: str) -> bool:
    turbine_group = get_turbine_group_from_filename(group_name)
    min_id, max_id = TURBINE_GROUPS[turbine_group]

    # Check if any expected turbines are missing
    missing_turbines = set(range(min_id, max_id + 1)) - set(df["turbine_id"].unique())
    if missing_turbines:
        # We decide to continue processing and handle this later in data cleaning
        logger.warning(f"Missing turbines in {group_name}: {missing_turbines}")
        # Optionally, we could decide to halt processing

    # Check if all turbine IDs are within the allowed range
    invalid_turbines = df[~df["turbine_id"].between(min_id, max_id)]
    if not invalid_turbines.empty:
        logger.error(f"Turbine IDs outside expected range in {group_name}: {invalid_turbines['turbine_id'].unique()}")
        return False

    return True


def read_and_validate_csv(path: str, group_name: str) -> Optional[pd.DataFrame]:
    df = read_csv_file(path)
    df = df.sort_values('timestamp')
    if df is None:
        logger.error(f"Skipping file due to read failure: {path}")
        return None

    try:
        df = validate_dataframe(df)
        if df is None:
            logger.error(f"Skipping file due to validation failure: {path}")
            return None

        # Validate turbine IDs are within the correct range
        if not validate_turbine_ids(df, group_name):
            logger.error(f"Skipping file due to turbine ID validation failure: {path}")
            return None

        return df
    except ValueError as e:
        logger.error(f"Validation failed for {path}: {str(e)}")
        return None
