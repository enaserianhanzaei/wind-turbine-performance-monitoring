import pandas as pd


def calculate_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate min, max, mean power output, wind speed, and wind direction per turbine per day."""
    summary = (
        df.groupby([df['timestamp'].dt.date, 'turbine_id'])
        .agg(
            min_power_output=('power_output', 'min'),
            max_power_output=('power_output', 'max'),
            mean_power_output=('power_output', 'mean'),
        )
        .reset_index()
        .rename(columns={'timestamp': 'date'})
    )
    return summary
