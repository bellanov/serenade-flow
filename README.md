# SerenadeFlow

**SerenadeFlow** is a powerful and flexible *ETL (Extract, Transform, Load)* pipeline framework designed to streamline data processing from both **local** and **remote** sources.

It *Extracts* data from diverse sources, *Transforms* it through customizable and reusable operations, and *Loads* it into your desired destination with minimal configuration.

Built to be the Swiss Army Knife of ETL solutions, SerenadeFlow offers a simple yet extensible architecture that makes data movement and transformation intuitive—whether you're a data engineer, analyst, or developer.

## Requirements

The project is configured to operate in _Python >= 3.8_ enviornments.

## Quickstart

The best way to get started with SerenadeFlow is to explore the examples. Each example is a self-contained recipe that demonstrates a specific use case:

### Basic ETL Pipeline

```bash
python3 examples/basic_etl_pipeline.py --data-dir ./data --output-prefix output
```

This example demonstrates the core ETL workflow: extracting data from local JSON files, transforming it, and loading it to output files.

### Remote Data Extraction

```bash
python3 examples/quickstart.py
```

This example shows how to extract data from a remote JSON API endpoint.

### Using Plugins

See the [Examples Documentation](examples/README.md) for more recipes and use cases.

## Examples and Recipes

SerenadeFlow is built around **examples** (recipes) that demonstrate how to use the framework. Each example is a self-contained script showing a specific ETL pattern:

- **Basic Examples**: Core ETL workflows (`basic_etl_pipeline.py`, `hello_world.py`, `quickstart.py`)
- **Cloud Integrations**: Examples for GCS, Firestore, and other cloud services
- **Plugin Examples**: Demonstrations of using community plugins

See the [Examples Documentation](examples/README.md) for a complete guide to available examples and how to create your own.

## Data Source Configuration

SerenadeFlow supports extracting data from various sources. The `data_source` and `data_source_path` parameters in the pipeline configuration determine where the data is extracted from.

### Local Files (JSON and Parquet)

To extract data from local files, set `data_source` to `local` and `data_source_path` to the directory containing your files. The pipeline will read all `.json` and `.parquet` files within the specified directory.

Example `config.json` for local data:

```json
{
    "data_source": "local",
    "data_source_path": "./df",
    "data_format": "json"
}
```

### Remote JSON API

To extract data from a remote JSON API, set `data_source` to `remote` and `data_source_path` to the URL of the API endpoint. The pipeline expects a JSON response from the specified URL.

Example `config.json` for remote data:

```json
{
    "data_source": "remote",
    "data_source_path": "https://api.example.com/data",
    "data_format": "json"
}
```

## Output Formats

SerenadeFlow supports multiple output formats for your processed data:

### CSV Format (Default)
The traditional CSV format is the default output format, providing wide compatibility with various tools and applications.

### Parquet Format
Parquet is a column-oriented storage format that offers compression and better performance for analytics workloads.

To use Parquet output format:

```python
from serenade_flow.pipeline import configure, extract, transform, load

# Configure and process data
configure({"data_source": "local", "data_source_path": "./data"})
data_frames = extract()
transformed_data = transform(data_frames)

# Load as Parquet files
load(transformed_data, "output_prefix", "parquet")
```

## Plugin System

SerenadeFlow now supports a robust plugin system for custom extract, transform, and load steps.

### Using a Community Plugin

To use the GCS Data Extractor plugin:

```python
from serenade_flow.plugins import PluginRegistry

config = {
    "plugins": {
        "gcs_data_extractor": {
            "module": "serenade_flow.community.gcs_data_extractor_plugin",
            "class": "GCSDataExtractorPlugin",
            "enabled": True
        }
    }
}
pipeline.configure(config)
plugin = pipeline.PLUGIN_REGISTRY.get("gcs_data_extractor")
plugin.configure(bucket_url="https://storage.googleapis.com/odds-data-samples-4vuoq93m/")
result = plugin.extract_with_retry("odds/american/event_008740fcf1af65b0cc9e79.json")
```

### FantasyAce Cloud Functions Plugin

Use Cloud Functions to fetch sports, events, and event odds.

```python
from serenade_flow import pipeline

config = {
    "plugins": {
        "fantasyace_cf": {
            "module": "serenade_flow.community.fantasyace_cloud_functions_plugin",
            "class": "FantasyAceCloudFunctionsPlugin",
            "enabled": True,
        }
    }
}

pipeline.configure(config)
plugin = pipeline.PLUGIN_REGISTRY.get("fantasyace_cf")
plugin.configure(
    base_url_sports="https://getsports-twqu2g763q-uc.a.run.app/",
    base_url_events="https://getevents-twqu2g763q-uc.a.run.app/",
    base_url_event_odds="https://geteventodds-twqu2g763q-uc.a.run.app/",
)

data_frames = plugin.extract_events_and_odds(sport_key="americanfootball_nfl", limit=50)
transformed = pipeline.transform(data_frames)
pipeline.load(transformed, output_prefix="fantasyace")
```

### Contributing Plugins

See `serenade_flow/community/PLUGIN_TEMPLATE.md` for how to document and contribute your own plugins.

## Architecture

SerenadeFlow follows a plugin-based architecture where:

- **Core Utilities**: The `pipeline` module provides basic utilities for data processing
- **Plugins**: Extensible components for specific data sources and transformations
- **Examples**: Self-contained recipes that demonstrate complete ETL workflows

This architecture allows you to:
- Use existing examples as templates for your own pipelines
- Extend functionality through plugins
- Build custom ETL workflows by combining plugins and utilities

For new projects, we recommend starting with an example that matches your use case and customizing it as needed.
