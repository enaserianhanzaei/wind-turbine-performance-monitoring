from datetime import date

from ingestion.reader import read_and_validate_csv
from ingestion.cleaning import clean_data
from ingestion.utils import filter_today_data
from persistence.database import create_database, get_db_session
from analysis.statistics import calculate_daily_summary
from analysis.anomaly_detection import detect_daily_output_sum_anomalies
from persistence.crud import (
    insert_or_update_readings_from_dataframe,
    insert_daily_summary,
    insert_reading_level_anomalies)

# Configure logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_pipeline(csv_file_path: str,
                 target_date: date = None,
                 update_existing: bool = False,
                 group_name: str = None,
                 window_days: int = 7):
    # Ensure tables exist (no-op if already created)
    create_database()

    if not group_name:
        group_name = csv_file_path.split('/')[-1].split('.')[0]

    # Read and validate the CSV file
    logger.info(f"Reading and validating CSV file: {csv_file_path}")
    df = read_and_validate_csv(csv_file_path, group_name)

    if df is None:
        logger.error("Failed to read or validate the CSV file. Exiting.")
        return

    # Filter today's data
    if target_date:
        df = filter_today_data(df, target_date)
        print(len(df))
        if df.empty:
            logger.warning(f"No data for {target_date}. Skipping insert.")
            return

    # Clean the data (handle missing values, outliers, duplicates)
    logger.info("Cleaning the data...")
    df = clean_data(df)

    # Analyze (Summary + Anomaly Detection)
    logger.info("Calculating summary statistics...")
    summary_df = calculate_daily_summary(df)

    # Insert or update data in the database
    logger.info("Inserting/Updating data in the database...")
    with get_db_session() as session:
        if not df.empty:
            insert_or_update_readings_from_dataframe(
                session, df, update_existing=update_existing
            )
        if not summary_df.empty:
            logger.info("Inserting daily summary...")
            insert_daily_summary(session, summary_df)

        if target_date:
            logger.info(f"Detecting daily anomalies for data {target_date}")
            anomalies_df = detect_daily_output_sum_anomalies(db=session,
                                                             today_df=df,
                                                             target_date=target_date,
                                                             window_days=window_days)
            if not anomalies_df.empty:
                logger.info("Inserting daily anomalies...")
                insert_reading_level_anomalies(session, anomalies_df)

    logger.info("Data processing complete and stored in the database.")