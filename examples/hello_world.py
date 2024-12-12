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


def quality_assurance(data):
    """Perform quality assurance checks."""
    for col in data.columns:
        if data[col].dtype == 'object':
            data[col] = data[col].apply(lambda x: str(x) if isinstance(x, dict) else x)
   
    data = data.drop_duplicates()
    return data


print("\nExecuting Hello World Example\n")

# Configure ETL Pipeline
pipeline.configure({
    "data_source": "local",
    "data_source_path": "/path/to/directory",
    "data_format": "csv"
})

# Extract
raw_data = pipeline.extract()
print(f"Raw Data:\n {raw_data} \n")

# Transform
data = transform(raw_data)
print(f"Transfomred Data:\n {data} \n")

# Quality Assurance
data = quality_assurance(data)

# Load
pipeline.load(data, "hello_world")
