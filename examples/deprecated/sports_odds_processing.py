"""Sports odds data processing example."""

import json
import os
import sys
from typing import Dict, Any, List

import pandas as pd
from datetime import datetime, timezone

from serenade_flow import pipeline
from serenade_flow.quality import DataQualityAssessor


def validate_sports_event_data(data: Dict[str, Any]) -> bool:
    """Validate sports event data structure."""
    required_fields = [
        "id",
        "sport_key",
        "sport_title",
        "home_team",
        "away_team",
        "commence_time",
        "bookmakers",
    ]

    if not isinstance(data, dict):
        return False

    for field in required_fields:
        if field not in data or data[field] is None:
            return False

    bookmakers = data.get("bookmakers", [])
    if not isinstance(bookmakers, list):
        return False

    # Empty bookmakers list is valid
    if not bookmakers:
        return True

    # Validate each bookmaker structure
    for bookmaker in bookmakers:
        if not _validate_bookmaker(bookmaker):
            return False

    return True


def _validate_bookmaker(bookmaker: dict) -> bool:
    """Validate a single bookmaker structure."""
    if not isinstance(bookmaker, dict):
        return False
    if "key" not in bookmaker or "title" not in bookmaker:
        return False
    markets = bookmaker.get("markets", [])
    if not isinstance(markets, list):
        return False
    for market in markets:
        if not _validate_market(market):
            return False
    return True


def _validate_market(market: dict) -> bool:
    """Validate a single market structure."""
    if not isinstance(market, dict):
        return False
    if "key" not in market or "last_update" not in market:
        return False
    outcomes = market.get("outcomes", [])
    if not isinstance(outcomes, list):
        return False
    for outcome in outcomes:
        if not _validate_outcome(outcome):
            return False
    return True


def _validate_outcome(outcome: dict) -> bool:
    """Validate a single outcome structure."""
    if not isinstance(outcome, dict):
        return False
    if "name" not in outcome or "price" not in outcome:
        return False
    return True


def flatten_sports_event_record(record: dict) -> List[Dict[str, Any]]:
    """Flatten a sports event record into multiple rows."""
    flattened_records = []

    for bookmaker in record.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            for outcome in market.get("outcomes", []):
                flattened_record = {
                    "id": record.get("id"),
                    "sport_key": record.get("sport_key"),
                    "sport_title": record.get("sport_title"),
                    "commence_time": record.get("commence_time"),
                    "home_team": record.get("home_team"),
                    "away_team": record.get("away_team"),
                    "bookmaker_key": bookmaker.get("key"),
                    "bookmaker_title": bookmaker.get("title"),
                    "market_key": market.get("key"),
                    "market_last_update": market.get("last_update"),
                    "outcome_name": outcome.get("name"),
                    "outcome_price": outcome.get("price"),
                    "outcome_point": outcome.get("point"),
                }
                flattened_records.append(flattened_record)

    return flattened_records


def process_sports_odds_json(data: Any, filename: str) -> pd.DataFrame:
    """Process sports odds JSON data into a DataFrame."""
    flattened_records = []

    if isinstance(data, list):
        for record in data:
            if validate_sports_event_data(record):
                flattened_records.extend(flatten_sports_event_record(record))
    elif isinstance(data, dict):
        if validate_sports_event_data(data):
            flattened_records.extend(flatten_sports_event_record(data))

    if flattened_records:
        return pd.DataFrame(flattened_records)
    return pd.DataFrame()


def extract_sports_odds_from_local(data_directory: str) -> Dict[str, pd.DataFrame]:
    """Extract sports odds data from local JSON files."""
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
                    df = process_sports_odds_json(data, filename)

                    if not df.empty:
                        data_frames[filename] = df
                        print(f"  Extracted {len(df)} records")
                    else:
                        print("  No valid records found")
            except Exception as e:
                print(f"  Error processing {filename}: {str(e)}")

    return data_frames


def transform_sports_odds_data(data_frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Transform sports odds data."""
    transformed_data: Dict[str, pd.DataFrame] = {}

    print("\nTransforming sports odds data...")

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

            df["processed_at"] = datetime.now(timezone.utc)
            df["source_file"] = key

            transformed_data[key] = df
            print(f"  Transformed {len(df)} records")

        except Exception as e:
            print(f"  Error transforming {key}: {str(e)}")
            continue

    return transformed_data


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Sports odds data processing example")
    parser.add_argument(
        "--data-dir",
        type=str,
        default="./data",
        help="Directory containing sports odds JSON files (default: ./data)"
    )
    parser.add_argument(
        "--output-prefix",
        type=str,
        default="sports_odds",
        help="Prefix for output files (default: sports_odds)"
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
    print("Sports Odds Data Processing")
    print("=" * 60)

    # Extract
    print("\nSTEP 1: Extract")
    print("-" * 60)
    data_frames = extract_sports_odds_from_local(args.data_dir)

    if not data_frames:
        print("\nNo data extracted. Exiting.")
        sys.exit(1)

    # Assess quality
    print("\nAssessing data quality...")
    assessor = DataQualityAssessor()
    for key, df in data_frames.items():
        report = assessor.assess({key: df})
        print(f"  {key}: Score {report['score']}/100")

    # Transform
    print("\nSTEP 2: Transform")
    print("-" * 60)
    transformed_data = transform_sports_odds_data(data_frames)

    if not transformed_data:
        print("\nNo data transformed. Exiting.")
        sys.exit(1)

    # Load
    print("\nSTEP 3: Load")
    print("-" * 60)
    result = pipeline.load(transformed_data, args.output_prefix, args.format)

    if result:
        print(f"\n{result}")
        print("\n" + "=" * 60)
        print("Processing completed successfully!")
        print("=" * 60)
    else:
        print("\nPipeline failed during load step.")
        sys.exit(1)


if __name__ == "__main__":
    main()
