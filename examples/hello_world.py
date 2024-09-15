"""Example SerenadeFlow Usage."""

import pandas as pd
from serenade_flow import pipeline


def transform(data: pd.DataFrame) -> pd.DataFrame:
    """Transform Raw Data."""
    if 'name' in data.columns:
        data['name'] = data['name'].str.upper()
    if 'age' in data.columns:
        data['age'] = pd.to_numeric(data['age'], errors='coerce')
        data['age'] = data['age'].fillna(data['age'].mean()).astype(int)
        data['birth_year'] = 2023 - data['age']
    return data


# Configure ETL Pipeline
pipeline.configure({
    "data_source": "local",
    "data_source_path": "/path/to/directory",
    "data_format" : "csv"
})

# Extract
raw_data = pipeline.extract()
print(f"Raw Data:\n {raw_data} \n")

# Transform
data = transform(raw_data)
print(f"Transfomred Data:\n {data} \n")

# Load
pipeline.load(data, "output")
