import os
import argparse

import pandas as pd

from datetime import datetime

import ingestion_db_pipeline

if __name__ == "__main__":
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Process wind turbine data CSV and insert into the database.")
    parser.add_argument(
        "--csv_folder",
        default=os.path.join(os.path.dirname(__file__), "../resources/data_group_1"),
        type=lambda path: path if os.path.isdir(path) else parser.error(
            f"The file {path} does not exist."),
        help="Path to the folder of csv files to process",
    )
    parser.add_argument(
        "--window_days",
        default=7,
        type=int,
        help="number of days to be considered to calculate the historical pattern",
    )

    args = parser.parse_args()

    folder_name = os.path.basename(args.csv_folder)
    # List all the CSV files in the folder
    csv_files = [f for f in os.listdir(args.csv_folder) if f.endswith('.csv')]
    csv_files.sort(key=lambda x: datetime.strptime(x, '%Y-%m-%d.csv'))

    for csv_file in csv_files:
        # Create the full file path
        file_path = os.path.join(args.csv_folder, csv_file)
        target_date = pd.Timestamp(csv_file.split('.')[0]).date()
        ingestion_db_pipeline.run_pipeline(csv_file_path=file_path,
                                           target_date=target_date,
                                           group_name=folder_name,
                                           window_days=args.window_days)
