
import pandas as pd
import logging
import requests
from requests.exceptions import ConnectionError
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import os
from dotenv import load_dotenv

API_URL = os.getenv('API_URL', 'https://api.example.com/data')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Dummy data
dummy_data = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25},
    {"id": 3, "name": "Charlie", "age": 35},
]

def extract_dummy_data() -> pd.DataFrame:
    logging.info("Extracting dummy data")
    return pd.DataFrame(dummy_data)

def extract_api_data():
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        return response.json()
    except ConnectionError as e:
        logging.error(f"Failed to connect to {API_URL}: {e}")
        return None
    except requests.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None


def extract() -> pd.DataFrame:
    with ThreadPoolExecutor() as executor:
        dummy_future = executor.submit(extract_dummy_data)
        api_future = executor.submit(extract_api_data)
        
        dummy_data = dummy_future.result()
        api_data = api_future.result()
    
    if api_data is None:
        return dummy_data
    return pd.concat([dummy_data, pd.DataFrame(api_data)], ignore_index=True)

def transform(data: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transforming data")
    if 'name' in data.columns:
        data['name'] = data['name'].str.upper()
    if 'age' in data.columns:
        data['age'] = pd.to_numeric(data['age'], errors='coerce')
        data['age'] = data['age'].fillna(data['age'].mean()).astype(int)
        data['birth_year'] = 2023 - data['age']
    return data

def load(data: pd.DataFrame, output_prefix: str):
    logging.info("Loading data")
    data.to_csv(f'{output_prefix}.csv', index=False)
    data.to_json(f'{output_prefix}.json', orient='records')
    logging.info("Data loaded successfully")

def data_entry(data: pd.DataFrame) -> pd.DataFrame:
    logging.info("Performing manual data entry")
    new_entry = pd.DataFrame([{"id": len(data) + 1, "name": "David", "age": 28}])
    return pd.concat([data, new_entry], ignore_index=True)

def quality_assurance(data):
    logging.info("Performing quality assurance checks")
    for col in data.columns:
        if data[col].dtype == 'object':
            data[col] = data[col].apply(lambda x: str(x) if isinstance(x, dict) else x)
    
    data = data.drop_duplicates()
    return data

def etl_pipeline():
    try:
        # Extract
        raw_data = extract()
        logging.info(f"Extracted {len(raw_data)} records")
        
        # Transform
        transformed_data = transform(raw_data)
        logging.info("Data transformed successfully")
        
        # Data Entry
        updated_data = data_entry(transformed_data)
        logging.info("Manual data entry completed")
        
        # Quality Assurance
        clean_data = quality_assurance(updated_data)
        logging.info(f"Quality assurance completed. {len(clean_data)} records remaining")
        
        # Load
        load(clean_data, 'output')
        
        logging.info("ETL pipeline completed successfully")
    except Exception as e:
        logging.error(f"ETL pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    etl_pipeline()