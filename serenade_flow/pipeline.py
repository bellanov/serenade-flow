"""
ETL Pipeline Implementation.

Extract, Load, and Transform data from local or remote data sources.
"""
from concurrent.futures import ThreadPoolExecutor
import logging
import os
import json
import requests
from datetime import datetime, timezone
from typing import Dict, Any

import pandas as pd
from jsonschema import validate

# Pipeline Configuration
CONFIG = {}

# Configure Loggiing
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)-15s %(levelname)-8s %(message)s"
)

# Initialize Logging
logger = logging.getLogger("serenade-flow")

# Schema for validating sports event data
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
                                "last_update": {"type": "string", "format": "date-time"},
                                "outcomes": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "price": {"type": "number"},
                                            "point": {"type": "number"}
                                        },
                                        "required": ["name", "price"]
                                    }
                                }
                            },
                            "required": ["key", "last_update", "outcomes"]
                        }
                    }
                },
                "required": ["key", "title", "markets"]
            }
        }
    },
    "required": ["id", "sport_key", "commence_time", "home_team", "away_team", "bookmakers"]
}

def validate_data(data: Dict[str, Any]) -> bool:
    """Validate data against schema."""
    try:
        validate(instance=data, schema=SPORTS_EVENT_SCHEMA)
        return True
    except Exception as e:
        logging.warning(f"Data validation failed: {str(e)}")
        return False

def transform_datetime(date_str: str) -> datetime:
    """Transform datetime string to UTC datetime object."""
    try:
        return pd.to_datetime(date_str, utc=True)
    except Exception as e:
        logging.error(f"DateTime transformation failed: {str(e)}")
        return None

def configure(config: dict) -> dict:
    """Configure the ETL Pipeline."""
    logging.info("Configuring Pipeline")
    CONFIG.update(config)
    return CONFIG


def extract_local_data(data_directory: str) -> dict:
    """Extract data from local JSON files."""
    data_frames = {}
    for filename in os.listdir(data_directory):
        if filename.endswith('.json'):
            with open(os.path.join(data_directory, filename), 'r') as file:
                try:
                    data = json.load(file)
                    if isinstance(data, list):
                        valid_data = []
                        for record in data:
                            if validate_data(record):
                                # Flatten the nested 'bookmakers' data
                                flattened_records = []
                                for bookmaker in record.get('bookmakers', []):
                                    for market in bookmaker.get('markets', []):
                                        for outcome in market.get('outcomes', []):
                                            flattened_record = {
                                                'id': record.get('id'),
                                                'sport_key': record.get('sport_key'),
                                                'sport_title': record.get('sport_title'),
                                                'commence_time': record.get('commence_time'),
                                                'home_team': record.get('home_team'),
                                                'away_team': record.get('away_team'),
                                                'bookmaker_key': bookmaker.get('key'),
                                                'bookmaker_title': bookmaker.get('title'),
                                                'market_key': market.get('key'),
                                                'market_last_update': market.get('last_update'),
                                                'outcome_name': outcome.get('name'),
                                                'outcome_price': outcome.get('price'),
                                                'outcome_point': outcome.get('point')
                                            }
                                            flattened_records.append(flattened_record)
                                valid_data.extend(flattened_records)
                        if valid_data:
                            data_frames[filename] = pd.DataFrame(valid_data)
                    elif isinstance(data, dict):
                        if validate_data(data):
                            # Flatten the nested 'bookmakers' data for a single record
                            flattened_records = []
                            for bookmaker in data.get('bookmakers', []):
                                for market in bookmaker.get('markets', []):
                                    for outcome in market.get('outcomes', []):
                                        flattened_record = {
                                            'id': data.get('id'),
                                            'sport_key': data.get('sport_key'),
                                            'sport_title': data.get('sport_title'),
                                            'commence_time': data.get('commence_time'),
                                            'home_team': data.get('home_team'),
                                            'away_team': data.get('away_team'),
                                            'bookmaker_key': bookmaker.get('key'),
                                            'bookmaker_title': bookmaker.get('title'),
                                            'market_key': market.get('key'),
                                            'market_last_update': market.get('last_update'),
                                            'outcome_name': outcome.get('name'),
                                            'outcome_price': outcome.get('price'),
                                            'outcome_point': outcome.get('point')
                                        }
                                        flattened_records.append(flattened_record)
                            if flattened_records:
                                data_frames[filename] = pd.DataFrame(flattened_records)
                except Exception as e:
                    logging.error(f"Error processing {filename}: {str(e)}")
    return data_frames


def extract_remote_data() -> dict:
    """Extract data from a remote data source."""
    logging.info("Extracting Remote Data")
    data_frames = {}
    
    try:
        response = requests.get(CONFIG["data_source_path"])
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                valid_data = []
                for record in data:
                    if validate_data(record):
                        flattened_records = []
                        for bookmaker in record.get('bookmakers', []):
                            for market in bookmaker.get('markets', []):
                                for outcome in market.get('outcomes', []):
                                    flattened_record = {
                                        'id': record.get('id'),
                                        'sport_key': record.get('sport_key'),
                                        'sport_title': record.get('sport_title'),
                                        'commence_time': record.get('commence_time'),
                                        'home_team': record.get('home_team'),
                                        'away_team': record.get('away_team'),
                                        'bookmaker_key': bookmaker.get('key'),
                                        'bookmaker_title': bookmaker.get('title'),
                                        'market_key': market.get('key'),
                                        'market_last_update': market.get('last_update'),
                                        'outcome_name': outcome.get('name'),
                                        'outcome_price': outcome.get('price'),
                                        'outcome_point': outcome.get('point')
                                    }
                                    flattened_records.append(flattened_record)
                        valid_data.extend(flattened_records)
                if valid_data:
                    data_frames["remote_data.json"] = pd.DataFrame(valid_data)
            elif isinstance(data, dict):
                if validate_data(data):
                    flattened_records = []
                    for bookmaker in data.get('bookmakers', []):
                        for market in bookmaker.get('markets', []):
                            for outcome in market.get('outcomes', []):
                                flattened_record = {
                                    'id': data.get('id'),
                                    'sport_key': data.get('sport_key'),
                                    'sport_title': data.get('sport_title'),
                                    'commence_time': data.get('commence_time'),
                                    'home_team': data.get('home_team'),
                                    'away_team': data.get('away_team'),
                                    'bookmaker_key': bookmaker.get('key'),
                                    'bookmaker_title': bookmaker.get('title'),
                                    'market_key': market.get('key'),
                                    'market_last_update': market.get('last_update'),
                                    'outcome_name': outcome.get('name'),
                                    'outcome_price': outcome.get('price'),
                                    'outcome_point': outcome.get('point')
                                }
                                flattened_records.append(flattened_record)
                    if flattened_records:
                        data_frames["remote_data.json"] = pd.DataFrame(flattened_records)
        else:
            logging.error(f"Failed to fetch remote data: {response.status_code}")
    except Exception as e:
        logging.error(f"Error fetching remote data: {str(e)}")
    
    return data_frames


def extract() -> dict:
    """Extract data based on the configured source."""
    data_future = None
    data_payload = None

    with ThreadPoolExecutor() as executor:
        if CONFIG["data_source"] == "local":
            data_future = executor.submit(extract_local_data, CONFIG["data_source_path"])
        elif CONFIG["data_source"] == "remote":
            data_future = executor.submit(extract_remote_data)

        data_payload = data_future.result()

    return data_payload


def transform(data_frames: dict) -> dict:
    """Transform the extracted data."""
    for key, df in data_frames.items():
        try:
            # Standardize datetime to UTC
            if 'commence_time' in df.columns:
                df['commence_time'] = df['commence_time'].apply(transform_datetime)
            if 'market_last_update' in df.columns:
                df['market_last_update'] = df['market_last_update'].apply(transform_datetime)
            
            # Standardize team names
            if 'home_team' in df.columns:
                df['home_team'] = df['home_team'].str.title()
            if 'away_team' in df.columns:
                df['away_team'] = df['away_team'].str.title()
            
            # Convert outcome_point to numeric, handling potential NaNs
            if 'outcome_point' in df.columns:
                df['outcome_point'] = pd.to_numeric(df['outcome_point'], errors='coerce')
            
            # Add metadata using the new UTC-aware datetime
            df['processed_at'] = datetime.now(timezone.utc)
            df['source_file'] = key
            
            # Remove any rows with invalid datetime
            df = df.dropna(subset=['commence_time'])
            
            data_frames[key] = df
        except Exception as e:
            logging.error(f"Error transforming {key}: {str(e)}")
    return data_frames


def load(data: dict, output_prefix: str) -> str:
    """Load transformed data into CSV files."""
    try:
        for key, df in data.items():
            output_file = f"{output_prefix}_{key.replace('.json', '.csv')}"
            df.to_csv(output_file, index=False)
            logging.info(f"Data saved to {output_file}")
        return "Data loaded successfully"
    except Exception as e:
        logging.error(f"Error loading data: {str(e)}")
        return None