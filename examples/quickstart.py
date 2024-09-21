"""Example SerenadeFlow Usage."""

import pandas as pd
from serenade_flow import pipeline


print("\nExecuting Quickstart Example\n")

# Configure ETL Pipeline
pipeline.configure({
    "data_source": "local",
    "data_source_path": "/path/to/directory",
    "data_format" : "csv"
})

# Extract
raw_data = pipeline.extract()
print(f"Raw Data:\n {raw_data} \n")

# Load
pipeline.load(raw_data, "quickstart")
