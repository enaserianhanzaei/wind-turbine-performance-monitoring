from typing import Dict
import pandas as pd

from ingestion.validation import SENSOR_COLUMNS
from config.constants import FORWARD_FILL_LIMIT_MINUTES, REPORT_FREQ, IQR_FACTOR, SENSOR_LIMITS, OUTLIER_STD_THRESHOLD

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=["timestamp", "turbine_id"])
    after = len(df)
    logger.info(f"Removed {before - after} duplicate rows.")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    # Drop rows missing critical columns (timestamp, turbine_id)
    df = df.dropna(subset=["timestamp", "turbine_id"])
    df = df.sort_values(["turbine_id", "timestamp"])

    # Flag turbines with missing sensor data
    turbines_with_missing_data = []

    def fill_missing_sensor_data(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values("timestamp")

        missing_data = group[SENSOR_COLUMNS].isnull().sum(axis=1)
        if missing_data.any():
            turbines_with_missing_data.append(group["turbine_id"].iloc[0])
            logger.warning(f"Missing sensor data for turbine {group['turbine_id'].iloc[0]}")

        # Apply forward-fill for missing sensor data, but limit the number of steps (e.g., no more than 10 minutes gap)
        for col in SENSOR_COLUMNS:
            group[col] = group[col].ffill(limit=2)  # 2 step, for example, 10 min if 5T freq

        return group

    # Apply to each turbine group
    df = df.groupby("turbine_id", group_keys=False).apply(fill_missing_sensor_data)

    # Remove rows where sensor values are still missing after filling (drop problematic rows)
    df = df.dropna(subset=SENSOR_COLUMNS)

    if turbines_with_missing_data:
        logger.error(f"Turbines with missing sensor data: {set(turbines_with_missing_data)}")

    logger.info(f"Data after missing value handling: {len(df)} rows.")

    return df


def clean_physical_limits(df: pd.DataFrame, sensor_limits: dict = SENSOR_LIMITS) -> pd.DataFrame:
    """
    Enforce physical min/max for each sensor feature.
    Drop rows where any feature is outside its allowed range.
    """
    before_len = len(df)

    def _check_within_limits(col: pd.Series, limits: dict) -> pd.Series:
        mask = pd.Series(False, index=col.index)
        if limits.get("min") is not None:
            mask |= col < limits["min"]
        if limits.get("max") is not None:
            mask |= col > limits["max"]
        return mask

    invalid_masks = []
    for feature, limits in sensor_limits.items():
        if feature not in df.columns:
            logger.warning(f"Skipped {feature}: not in DataFrame")
            continue

        mask = _check_within_limits(df[feature], limits)
        if mask.any():
            n = mask.sum()
            logger.info(f"Removing {n} rows: {feature} outside [{limits['min']}, {limits['max']}]")
            invalid_masks.append(mask)

    if invalid_masks:
        combined = invalid_masks[0]
        for m in invalid_masks[1:]:
            combined |= m
        df = df.loc[~combined].reset_index(drop=True)

    after_len = len(df)
    logger.info(f"clean_physical_limits: dropped {before_len - after_len} rows, {after_len} remain.")
    return df


def detect_and_handle_outliers_statistically_std(df: pd.DataFrame,
                                                 feature: str = "power_output",
                                                 action: str = 'drop') -> pd.DataFrame:
    """
    Per turbine, compute standard deviation bounds on `feature` and then:
      - "flag":  add `is_outlier` boolean column
      - "drop":  remove those rows entirely
    """
    if action not in {"flag", "drop"}:
        raise ValueError(f"action must be one of flag|drop, got {action!r}")

    before_len = len(df)
    total_outliers = 0

    def _process_grp(g):
        nonlocal total_outliers
        mean = g[feature].mean()
        std_dev = g[feature].std()

        # Define the threshold based on standard deviation
        lo, hi = mean - OUTLIER_STD_THRESHOLD * std_dev, mean + OUTLIER_STD_THRESHOLD * std_dev

        # Flag or filter rows that are outside the range of (lo, hi)
        is_out = ~g[feature].between(lo, hi)

        n_out = is_out.sum()
        total_outliers += int(n_out)
        if n_out:
            logger.info(f"Turbine {g.name}: {n_out} outliers detected")

        g = g.copy()
        if action == "flag":
            g["is_outlier"] = is_out
            return g
        if action == "drop":
            return g.loc[~is_out]

    # Apply per-group processing
    df_result = (
        df
        .groupby("turbine_id", group_keys=False)
        .apply(_process_grp)
        .reset_index(drop=True)
    )

    after_len = len(df_result)

    logger.info(f"Total outliers {action!r}: {total_outliers}")
    logger.info(f"DataFrame size before: {before_len}, after: {after_len}")

    return df_result


def detect_and_handle_outliers_statistically_IQR(df: pd.DataFrame,
                                                 feature: str = "power_output",
                                                 action: str = 'drop') -> pd.DataFrame:
    """
    Per turbine, compute IQR bounds on `feature` and then:
      - "flag":  add `is_outlier` boolean column
      - "drop":  remove those rows entirely
    """
    if action not in {"flag", "drop"}:
        raise ValueError(f"action must be one of flag|drop|mask, got {action!r}")

    before_len = len(df)
    total_outliers = 0

    def _process_grp(g):
        nonlocal total_outliers
        q1, q3 = g[feature].quantile([0.25, 0.75])
        iqr = q3 - q1
        lo, hi = q1 - IQR_FACTOR * iqr, q3 + IQR_FACTOR * iqr
        is_out = ~g[feature].between(lo, hi)

        n_out = is_out.sum()
        total_outliers += int(n_out)
        if n_out:
            logger.info(f"Turbine {g.name}: {n_out} outliers detected")

        g = g.copy()
        if action == "flag":
            g["is_outlier"] = is_out
            return g
        if action == "drop":
            return g.loc[~is_out]

    # Apply per-group processing
    df_result = (
        df
        .groupby("turbine_id", group_keys=False)
        .apply(_process_grp)
        .reset_index(drop=True)
    )

    after_len = len(df_result)

    logger.info(f"Total outliers {action!r}: {total_outliers}")
    logger.info(f"DataFrame size before: {before_len}, after: {after_len}")

    return df_result


def clean_data(df, sensor_limits: Dict = SENSOR_LIMITS):
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = clean_physical_limits(df, sensor_limits)
    df = detect_and_handle_outliers_statistically_std(df)
    return df