"""
ETL Pipeline Implementation.

Extract, Load, and Transform data from both local and remote data sources.
"""
from concurrent.futures import ThreadPoolExecutor
import logging
import os

import pandas as pd
import requests

# Data Sources
DATA_SOURCE_URL = os.getenv("DATA_SOURCE_URL")
DATA_SOURCE_PATH = os.getenv("DATA_SOURCE_PATH")

# Configure string format for consumption into logging platforms.
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)-15s %(levelname)-8s %(message)s"
)

# Initialize Logging
logger = logging.getLogger("etl-pipeline")


def extract_local_data() -> pd.DataFrame:
    logging.info("Extracting local data")
    local_data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35},
    ]
    return pd.DataFrame(local_data)


def extract_remote_data():
    try:
        response = requests.get(DATA_SOURCE_URL, timeout=10)
        response.raise_for_status()
        return response.json()
    except ConnectionError as e:
        logging.error(f"Failed to connect to {DATA_SOURCE_URL}: {e}")
        return None
    except requests.RequestException as e:
        if DATA_SOURCE_URL is None:
            logging.critical("Environment variable DATA_SOURCE_URL is not defined.")
        else:
            logging.error(f"API request failed: {e}.")
        return None


def extract() -> pd.DataFrame:
    with ThreadPoolExecutor() as executor:
        local_future = executor.submit(extract_local_data)
        remote_future = executor.submit(extract_remote_data)

        dummy_data = local_future.result()
        api_data = remote_future.result()

    if api_data is None:
        return dummy_data
    return pd.concat([dummy_data, pd.DataFrame(api_data)], ignore_index=True)


def etl_pipeline():
    """ETL Pipeline Execution."""
    logger.info("Executing ETL Pipeline")

    try:
        # Extract
        raw_data = extract()
        logging.info(f"Extracted {len(raw_data)} records")
    except Exception as e:
        logging.error(f"ETL pipeline failed: {str(e)}")
        raise

    return "Extract, Transform, and Load!!!"


if __name__ == "__main__":
    etl_pipeline()
