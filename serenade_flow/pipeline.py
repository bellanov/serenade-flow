"""
ETL Pipeline Implementation.

Extract, Load, and Transform data from local or remote data sources.

Typical usage example:

  # TODO: Add example usage

"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
import requests

from serenade_flow.plugins.registry import PluginRegistry
from serenade_flow.quality.assessor import DataQualityAssessor

# Global configuration dictionary
CONFIG: Dict[str, Any] = {}
PLUGIN_REGISTRY: Optional[PluginRegistry] = None

# Configure Logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)-15s %(levelname)-8s %(message)s"
)

# Initialize Logging
logger = logging.getLogger("serenade-flow")


# TODO: Consider moving to a utility module
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


def _process_json_data(data: Any, filename: str) -> pd.DataFrame:
    """Process JSON data and return a DataFrame."""
    if isinstance(data, list):
        flattened_records = []
        for record in data:
            flattened_records.extend(record)
        if flattened_records:
            return pd.DataFrame(flattened_records)
    elif isinstance(data, dict):
        flattened_records = data
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
        logging.info(f"Processing file: {filename}")
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
