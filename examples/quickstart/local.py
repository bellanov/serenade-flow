"""Example SerenadeFlow Usage."""

from serenade_flow import pipeline


print("\nExecuting Quickstart Example\n")

# Configure ETL Pipeline
#
# To use the remote data source, you need to:
# 1. Set the `data_source` to "remote".
# 2. Provide a public HTTP(S) URL to your data in `data_source_path`.
#    (e.g., a public GCS bucket file link, not a gs:// path)
# 3. No authentication or credentials are required for public data.
#
# Note: Firestore integration will be supported via HTTP API in the future.
pipeline.configure({
    "data_source": "remote",
    "data_format": "json",
    "data_source_path": [
        "data/file_1",
        "data_file_2"
    ]
})

# Extract
raw_data = pipeline.extract()
print(f"Raw Data:\n {raw_data} \n")

# Load
pipeline.load(raw_data, "quickstart")
