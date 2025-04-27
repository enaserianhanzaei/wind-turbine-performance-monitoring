
from datetime import date
from typing import Optional
import pandas as pd

from sqlalchemy.orm import Session

from persistence.crud import load_historical_daily_avg_stats, load_historical_daily_totals_stats


def detect_daily_output_sum_anomalies(
        db: Session, today_df: pd.DataFrame, target_date: date, window_days: Optional[int] = 7,
        sigma_threshold: float = 2.0) -> pd.DataFrame:
    """
    Identify daily anomalies for each turbine on target_date.
    Uses load_historical_daily_stats to fetch past daily totals.
    Flags if today's total_power_output is outside ±sigma_threshold·std.
    """
    # Aggregate today's data to get total power output per turbine for the target date
    today_df["date"] = today_df["timestamp"].dt.date  # Extract the date part
    today_aggregated = today_df.groupby(["turbine_id", "date"])["power_output"].sum().reset_index()
    today_aggregated = today_aggregated.rename(columns={"power_output": "total_power_output"})

    # Load historical mean/std using the helper
    hist_stats = load_historical_daily_totals_stats(
        db=db,
        before_date=target_date,
        window_days=window_days
    )

    # Merge today's summary with historical stats
    merged = today_aggregated.merge(hist_stats, on="turbine_id", how="left")

    # Flag anomalies
    def _flag(row):
        return (
            row.total_power_output > row.hist_mean_daily_output + sigma_threshold * row.hist_std_daily_output or
            row.total_power_output < row.hist_mean_daily_output - sigma_threshold * row.hist_std_daily_output
        )

    merged["is_anomaly"] = merged.apply(_flag, axis=1)

    merged = merged.reset_index(drop=True).rename(columns={'timestamp': 'date'})

    return merged[
        ["date", "turbine_id", "total_power_output", "hist_mean_daily_output", "hist_std_daily_output", "is_anomaly"]
    ][merged['is_anomaly']]