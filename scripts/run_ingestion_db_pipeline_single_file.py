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
    args = parser.parse_args()

    # for the purpose of test
    target_date = pd.Timestamp(args.target_date).date()

    ingestion_db_pipeline.run_pipeline(args.csv_file, target_date)
