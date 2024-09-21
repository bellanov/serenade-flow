"""
ETL Pipeline Implementation.

Extract, Load, and Transform data from local or remote data sources.
"""
from concurrent.futures import ThreadPoolExecutor
import logging

import pandas as pd

# Pipeline Configuration
CONFIG = {}

# Configure Loggiing
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)-15s %(levelname)-8s %(message)s"
)

# Initialize Logging
logger = logging.getLogger("serenade-flow")


def configure(config: dict) -> dict:
    """Configure the ETL Pipeline."""
    logging.info("Configuring Pipeline")

    # TODO: Harden this block with schema validation
    CONFIG["data_format"] = config["data_format"]
    CONFIG["data_source"] = config["data_source"]
    CONFIG["data_source_path"] = config["data_source_path"]
    return CONFIG


def extract_local_data() -> pd.DataFrame:
    """Extract data from a local data source."""
    logging.info("Extracting Local Data")

    # TODO: Retrieve input from a directory of files
    local_data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35},
    ]
    return pd.DataFrame(local_data)


def extract_remote_data():
    """Extract data from a remote data source."""
    logging.info("Extracting Remote Data")
    return {}


def extract() -> pd.DataFrame:
    """Extract."""
    data_future = None
    data_payload = None

    with ThreadPoolExecutor() as executor:
        if CONFIG["data_source"] == "local":
            data_future = executor.submit(extract_local_data)
        elif CONFIG["data_source"] == "remote":
            data_future = executor.submit(extract_remote_data)

        data_payload = data_future.result()

    return data_payload


def load(data: pd.DataFrame, output_prefix: str):
    """Export data to CSV and JSON files."""
    logging.info("Loading Data")
    data.to_csv(f'{output_prefix}.csv', index=False)
    data.to_json(f'{output_prefix}.json', orient='records')
    return "Data loaded successfully"
