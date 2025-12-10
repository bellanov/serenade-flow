"""
ETL Pipeline Implementation.

Extract, Load, and Transform data from local or remote data sources.

Note: Domain-specific logic should be implemented in examples rather than here.
See examples/ for complete ETL workflow recipes.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
import requests

from serenade_flow.plugins import PluginRegistry
from serenade_flow.quality import DataQualityAssessor

# Global configuration dictionary
CONFIG: Dict[str, Any] = {}
PLUGIN_REGISTRY: Optional[PluginRegistry] = None

# Configure Logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)-15s %(levelname)-8s %(message)s"
)

# Initialize Logging
logger = logging.getLogger("serenade-flow")

# Schema for validating sports event data
# Note: Domain-specific schemas should be implemented in examples
SPORTS_EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "sport_key": {"type": "string"},
        "sport_title": {"type": "string"},
        "commence_time": {"type": "string", "format": "date-time"},
        "home_team": {"type": "string"},
        "away_team": {"type": "string"},
        "bookmakers": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "key": {"type": "string"},
                    "title": {"type": "string"},
                    "markets": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "key": {"type": "string"},
                                "last_update": {
                                    "type": "string",
                                    "format": "date-time",
                                },
                                "outcomes": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "price": {"type": "number"},
                                            "point": {"type": "number"},
                                        },
                                        "required": ["name", "price"],
                                    },
                                },
                            },
                            "required": ["key", "last_update", "outcomes"],
                        },
                    },
                },
                "required": ["key", "title", "markets"],
            },
        },
    },
    "required": [
        "id",
        "sport_key",
        "commence_time",
        "home_team",
        "away_team",
        "bookmakers",
    ],
}


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


def validate_data(data: Dict[str, Any]) -> bool:
    """Validate the structure of the data."""
    try:
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
        if not bookmakers:  # Empty bookmakers list is valid
            return True
        for bookmaker in bookmakers:
            if not _validate_bookmaker(bookmaker):
                return False
        return True
    except Exception:
        return False


def transform_datetime(date_str: str) -> datetime:
    """Transform datetime string to datetime object."""
    try:
        return pd.to_datetime(date_str, utc=True).to_pydatetime()
    except Exception as e:
        logging.error(f"Error parsing datetime '{date_str}': {str(e)}")
        raise


def configure(config: dict) -> dict:
    """Configure the pipeline with the given settings and load plugins if present."""
    global PLUGIN_REGISTRY
    CONFIG.update(config)
    if "plugins" in config:
        PLUGIN_REGISTRY = PluginRegistry()
        PLUGIN_REGISTRY.load_from_config(config)
    return CONFIG


def _flatten_record(record: dict) -> list:
    """Flatten a single record into multiple rows."""
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


def _process_json_data(data: Any, filename: str) -> pd.DataFrame:
    """Process JSON data and return a DataFrame."""
    if isinstance(data, list):
        flattened_records = []
        for record in data:
            if validate_data(record):
                flattened_records.extend(_flatten_record(record))
        if flattened_records:
            return pd.DataFrame(flattened_records)
    elif isinstance(data, dict):
        if validate_data(data):
            flattened_records = _flatten_record(data)
            if flattened_records:
                return pd.DataFrame(flattened_records)
    return pd.DataFrame()


def extract_local_data(data_directory: str) -> dict:
    """Extract data from local JSON files."""
    data_frames: Dict[str, pd.DataFrame] = {}
    if not os.path.isdir(data_directory):
        logging.error(f"Directory not found: {data_directory}")
        return data_frames
    for filename in os.listdir(data_directory):
        if filename.endswith(".json"):
            file_path = os.path.join(data_directory, filename)
            try:
                with open(file_path, "r") as file:
                    data = json.load(file)
                    df = _process_json_data(data, filename)
                    if not df.empty:
                        data_frames[filename] = df
            except Exception as e:
                logging.error(f"Error processing {filename}: {str(e)}")
    return data_frames


def extract_remote_data() -> dict:
    """Extract data from remote JSON source."""
    data_frames: Dict[str, pd.DataFrame] = {}
    data_source_path = CONFIG.get("data_source_path")
    if not data_source_path:
        logging.error("No remote data source path configured")
        return data_frames
    try:
        response = requests.get(data_source_path)
        if response.status_code == 200:
            data = response.json()
            df = _process_json_data(data, "remote_data.json")
            if not df.empty:
                data_frames["remote_data.json"] = df
    except Exception as e:
        logging.error(f"Error fetching remote data: {str(e)}")
    return data_frames


def extract() -> dict:
    """Extract data from the configured source and assess quality."""
    data_source = CONFIG.get("data_source")
    if data_source == "local":
        data_source_path = CONFIG.get("data_source_path")
        data_frames = extract_local_data(data_source_path)
    elif data_source == "remote":
        data_frames = extract_remote_data()
    else:
        logging.error(f"Unknown data source: {data_source}")
        return {}

    assessor = DataQualityAssessor()
    for key, df in data_frames.items():
        report = assessor.assess(df)
        logging.info(f"Quality report for {key}: {report}")
    return data_frames


def transform(data_frames: dict) -> dict:
    """Transform the extracted data."""
    transformed_data: Dict[str, pd.DataFrame] = {}
    for key, df in data_frames.items():
        if not df.empty:
            try:
                required_columns = [
                    "home_team",
                    "away_team",
                    "commence_time",
                    "market_last_update",
                    "outcome_point",
                ]
                missing_columns = [
                    col for col in required_columns if col not in df.columns
                ]
                if missing_columns:
                    logging.warning(f"Missing columns in {key}: {missing_columns}")
                    continue

                df["home_team"] = df["home_team"].str.title()
                df["away_team"] = df["away_team"].str.title()

                try:
                    df["commence_time"] = pd.to_datetime(df["commence_time"])
                    df["market_last_update"] = pd.to_datetime(df["market_last_update"])
                except Exception as e:
                    logging.error(
                        f"Error converting datetime fields in {key}: {str(e)}"
                    )
                    continue

                df["outcome_point"] = pd.to_numeric(
                    df["outcome_point"], errors="coerce"
                )
                df["processed_at"] = datetime.now(timezone.utc)
                df["source_file"] = key
                transformed_data[key] = df
            except Exception as e:
                logging.error(f"Error transforming {key}: {str(e)}")
                continue
    return transformed_data


# Look into script typing
# def load(data: Dict[str, pd.DataFrame], output_prefix: str, output_format: str = "csv") -> Optional[str]:
def load(data: dict, output_prefix: str, output_format: str = "csv") -> str:
    """Load transformed data into files.

    Args:
        data: Dictionary of DataFrames to save
        output_prefix: Prefix for output filenames
        output_format: Output format - 'csv' or 'parquet' (default: 'csv')

    Returns:
        Success message or None if error occurs
    """
    try:
        for key, df in data.items():
            if output_format.lower() == "parquet":
                output_file = f"{output_prefix}_{key.replace('.json', '.parquet')}"
                df.to_parquet(output_file, index=False, compression="snappy")
                logging.info(f"Data saved to {output_file} (Parquet format)")
            else:
                output_file = f"{output_prefix}_{key.replace('.json', '.csv')}"
                df.to_csv(output_file, index=False)
                logging.info(f"Data saved to {output_file} (CSV format)")

        if output_format.lower() == "parquet":
            return "Data loaded successfully in PARQUET format"
        else:
            return "Data loaded successfully in CSV format"
    except Exception as e:
        logging.error(f"Error loading data: {str(e)}")
        return None
