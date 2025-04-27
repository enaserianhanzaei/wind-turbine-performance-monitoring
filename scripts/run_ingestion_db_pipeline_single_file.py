import os
import argparse

import pandas as pd

import ingestion_db_pipeline

if __name__ == "__main__":
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Process wind turbine data CSV and insert into the database.")
    parser.add_argument(
        "--csv_file",
        default=os.path.join(os.path.dirname(__file__), "../resources/data/data_group_1.csv"),
        type=lambda path: path if os.path.isfile(path) else parser.error(
            f"The file {path} does not exist."),
        help="Path to the CSV file to process",
    )
    parser.add_argument(
        "--target_date",
        default="2022-03-30",
        type=str,
        help="target date",
    )

    parser.add_argument(
        "--group_name",
        default=None,
        type=str,
        help="name of the data group",
    )

    parser.add_argument(
        "--window_days",
        default=7,
        type=int,
        help="number of days to be considered to calculate the historical pattern",
    )

    args = parser.parse_args()

    # for the purpose of test
    target_date = pd.Timestamp(args.target_date).date()

    group_name = args.csv_file.split('/')[-1].split('.')[0] if not args.group_name else args.group_name

    ingestion_db_pipeline.run_pipeline(args.csv_file, target_date, group_name, window_days=args.window_days)
