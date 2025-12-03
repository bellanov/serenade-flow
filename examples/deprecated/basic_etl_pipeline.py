"""Basic ETL pipeline example using local JSON files."""

import json
import os
import sys
from typing import Dict

import pandas as pd

from serenade_flow import pipeline
from serenade_flow.quality import DataQualityAssessor


def extract_local_json_files(data_directory: str) -> Dict[str, pd.DataFrame]:
    """Extract data from local JSON files."""
    data_frames: Dict[str, pd.DataFrame] = {}

    if not os.path.isdir(data_directory):
        print(f"Directory not found: {data_directory}")
        return data_frames

    print(f"Scanning directory: {data_directory}")

    for filename in os.listdir(data_directory):
        if filename.endswith(".json"):
            file_path = os.path.join(data_directory, filename)
            print(f"Processing: {filename}")

            try:
                with open(file_path, "r") as file:
                    data = json.load(file)
                    df = pipeline._process_json_data(data, filename)

                    if not df.empty:
                        data_frames[filename] = df
                        print(f"  Extracted {len(df)} records")
                    else:
                        print("  No valid records found")
            except Exception as e:
                print(f"  Error processing {filename}: {str(e)}")

    return data_frames


def transform_data(data_frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Transform extracted data."""
    transformed_data: Dict[str, pd.DataFrame] = {}

    print("\nTransforming data...")

    for key, df in data_frames.items():
        if df.empty:
            continue

        print(f"Transforming: {key}")

        try:
            if "home_team" in df.columns:
                df["home_team"] = df["home_team"].str.title()
            if "away_team" in df.columns:
                df["away_team"] = df["away_team"].str.title()

            if "commence_time" in df.columns:
                df["commence_time"] = pd.to_datetime(df["commence_time"])
            if "market_last_update" in df.columns:
                df["market_last_update"] = pd.to_datetime(df["market_last_update"])

            if "outcome_point" in df.columns:
                df["outcome_point"] = pd.to_numeric(df["outcome_point"], errors="coerce")

            from datetime import datetime, timezone
            df["processed_at"] = datetime.now(timezone.utc)
            df["source_file"] = key

            transformed_data[key] = df
            print(f"  Transformed {len(df)} records")

        except Exception as e:
            print(f"  Error transforming {key}: {str(e)}")
            continue

    return transformed_data


def assess_quality(data_frames: Dict[str, pd.DataFrame]) -> None:
    """Assess data quality for extracted data."""
    print("\nAssessing data quality...")

    assessor = DataQualityAssessor()

    for key, df in data_frames.items():
        print(f"Quality report for {key}:")
        report = assessor.assess({key: df})

        print(f"  Score: {report['score']}/100")
        print(f"  Missing values: {report['missing_values'][key]['total_missing']}")
        print(f"  Duplicates: {len(report['duplicates'][key])}")


def load_data(
    data: Dict[str, pd.DataFrame],
    output_prefix: str,
    output_format: str = "csv"
) -> str:
    """Load transformed data to output files."""
    print(f"\nLoading data ({output_format.upper()} format)...")

    try:
        for key, df in data.items():
            if output_format.lower() == "parquet":
                output_file = f"{output_prefix}_{key.replace('.json', '.parquet')}"
                df.to_parquet(output_file, index=False, compression="snappy")
                print(f"  Saved: {output_file}")
            else:
                output_file = f"{output_prefix}_{key.replace('.json', '.csv')}"
                df.to_csv(output_file, index=False)
                print(f"  Saved: {output_file}")

        return f"Data loaded successfully in {output_format.upper()} format"
    except Exception as e:
        print(f"  Error loading data: {str(e)}")
        return None


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Basic ETL pipeline example")
    parser.add_argument(
        "--data-dir",
        type=str,
        default="./data",
        help="Directory containing JSON files to process (default: ./data)"
    )
    parser.add_argument(
        "--output-prefix",
        type=str,
        default="output",
        help="Prefix for output files (default: output)"
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["csv", "parquet"],
        default="csv",
        help="Output format (default: csv)"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Basic ETL Pipeline")
    print("=" * 60)

    # Extract
    print("\nSTEP 1: Extract")
    print("-" * 60)
    data_frames = extract_local_json_files(args.data_dir)

    if not data_frames:
        print("\nNo data extracted. Exiting.")
        sys.exit(1)

    # Assess quality
    assess_quality(data_frames)

    # Transform
    print("\nSTEP 2: Transform")
    print("-" * 60)
    transformed_data = transform_data(data_frames)

    if not transformed_data:
        print("\nNo data transformed. Exiting.")
        sys.exit(1)

    # Load
    print("\nSTEP 3: Load")
    print("-" * 60)
    result = load_data(transformed_data, args.output_prefix, args.format)

    if result:
        print(f"\n{result}")
        print("\n" + "=" * 60)
        print("Pipeline completed successfully!")
        print("=" * 60)
    else:
        print("\nPipeline failed during load step.")
        sys.exit(1)


if __name__ == "__main__":
    main()
