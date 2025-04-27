import logging
from typing import Any, Dict, List, Optional

from datetime import date, timedelta

import pandas as pd

from sqlalchemy import func, cast, Date
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from persistence.models import TurbineReading, DailySummary, DailyAnomaly

logger = logging.getLogger(__name__)


def load_historical_daily_totals_stats(
        db: Session,
        before_date: date,
        window_days: Optional[int] = None
) -> pd.DataFrame:
    """
    Compute the historical mean & std of *daily total* power_output per turbine,
    based on TurbineReading.

    Returns DataFrame with columns:
      turbine_id, hist_mean_daily_output, hist_std_daily_output
    """
    # Build base query: sum power_output per day per turbine
    q = (
        db.query(
            TurbineReading.turbine_id,
            func.date(TurbineReading.timestamp).label("date"),
            func.sum(TurbineReading.power_output).label("daily_total"),
        )
        .filter(TurbineReading.timestamp < before_date)  # exclude today
    )

    # apply rolling window if requested
    if window_days is not None:
        cutoff = before_date - timedelta(days=window_days)
        q = q.filter(TurbineReading.timestamp >= cutoff)

    # group by turbine_id + date
    q = q.group_by(TurbineReading.turbine_id, func.date(TurbineReading.timestamp))
    # read into pandas
    df = pd.read_sql(q.statement, db.bind, parse_dates=["date"])

    if df.empty:
        return pd.DataFrame(
            columns=["turbine_id", "hist_mean_daily_output", "hist_std_daily_output"]
        )

    # aggregate per turbine
    stats = (
        df
        .groupby("turbine_id")["daily_total"]
        .agg(
            hist_mean_daily_output="mean",
            hist_std_daily_output="std"
        )
        .reset_index()
    )
    return stats


def load_historical_daily_avg_stats(
        db: Session, before_date: date, window_days: Optional[int] = None
) -> pd.DataFrame:
    """
    Fetch past DailySummary.avg_power_output for each turbine,
    up to (but not including) before_date. Optionally restrict to the last `window_days`.
    Returns a DataFrame with columns: turbine_id, hist_mean, hist_std.
    """
    # Build base query
    q_ = db.query(
        DailySummary.turbine_id,
        DailySummary.mean_power_output,
        DailySummary.date
    ).filter(DailySummary.date < before_date)

    # Apply rolling‐window filter if requested
    if window_days is not None:
        cutoff = before_date - timedelta(days=window_days)
        q_ = q_.filter(DailySummary.date >= cutoff)

    # Read into pandas for fast group‐agg
    df = pd.read_sql(q_.statement, db.bind, parse_dates=["date"])

    if df.empty:
        # no history at all
        return pd.DataFrame(columns=["turbine_id", "hist_mean_power_output", "hist_std_power_output"])

    # Compute per‐turbine mean & std of daily averages
    stats = (
        df
        .groupby("turbine_id")["mean_power_output"]
        .agg(hist_mean_power_output="mean", hist_std_power_output="std")
        .reset_index()
    )
    return stats


def insert_or_update_readings_from_dataframe(
        db: Session, df: pd.DataFrame, update_existing: bool = False
) -> None:
    """
    Insert new turbine readings or update existing ones based on timestamp+turbine_id.
    """
    records: List[Dict[str, Any]] = df.to_dict(orient="records")
    stmt = sqlite_insert(TurbineReading).values(records)

    if update_existing:
        stmt = stmt.on_conflict_do_update(
            index_elements=["timestamp", "turbine_id"],
            set_={
                "wind_speed": stmt.excluded.wind_speed,
                "wind_direction": stmt.excluded.wind_direction,
                "power_output": stmt.excluded.power_output,
            },
        )
    else:
        stmt = stmt.on_conflict_do_nothing(index_elements=["timestamp", "turbine_id"])

    try:
        result = db.execute(stmt)
        logger.info(f"Upserted {result.rowcount or 0} turbine readings.")
    except SQLAlchemyError:
        logger.exception("Failed to insert/update turbine readings")
        db.rollback()
        raise


def insert_daily_summary(db: Session, summary_df: pd.DataFrame) -> None:
    """
    Bulk-insert daily summary statistics. Skip duplicates on (date, turbine_id).
    """
    records = summary_df.to_dict(orient="records")
    stmt = sqlite_insert(DailySummary).values(records)
    stmt = stmt.on_conflict_do_nothing(index_elements=["date", "turbine_id"])

    try:
        result = db.execute(stmt)
        logger.info(f"Inserted {result.rowcount or 0} daily summary records.")
    except SQLAlchemyError:
        logger.exception("Failed to insert daily summaries")
        db.rollback()
        raise


def insert_reading_level_anomalies(db: Session, anomalies_df: pd.DataFrame) -> None:
    """
    Bulk‐insert the flagged readings into daily_anomalies, skipping duplicates.
    """
    if anomalies_df.empty:
        logger.info("No anomalies to insert.")
        return

    records = anomalies_df.to_dict(orient='records')
    stmt = sqlite_insert(DailyAnomaly).values(records)
    stmt = stmt.on_conflict_do_nothing(index_elements=['date', 'turbine_id'])

    try:
        result = db.execute(stmt)
        logger.info(f"Inserted {result.rowcount or 0} anomaly records.")
    except SQLAlchemyError:
        logger.exception("Failed to insert daily anomalies")
        db.rollback()
        raise
    except Exception as e:
        logger.exception(f"Failed to insert daily anomalies {e}")
        db.rollback()
        raise
