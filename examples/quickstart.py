"""Example SerenadeFlow Usage."""

from serenade_flow import pipeline


print("\nExecuting Quickstart Example\n")

# Configure ETL Pipeline
#
# To use the remote data source, you need to:
# 1. Set the `data_source` to "remote".
# 2. Provide the GCS path to your data in `data_source_path`.
# 3. Ensure you have authenticated with GCP. For example, by setting
#    the GOOGLE_APPLICATION_CREDENTIALS environment variable.
pipeline.configure({
    "data_source": "remote",
    "data_source_path": "gs://your-bucket-name/df/",
    "data_format": "json",
    "load_destination": "firestore",
    "project_id": "your-gcp-project-id",
    "collection_name": "sports_events_test"
})

# Extract
raw_data = pipeline.extract()
print(f"Raw Data:\n {raw_data} \n")

# Load
pipeline.load(raw_data, "quickstart")
