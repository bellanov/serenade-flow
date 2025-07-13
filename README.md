# SerenadeFlow

*SerenadeFlow* is a powerful and flexible ETL (Extract, Transform, Load) pipeline framework designed to streamline data processing from both local and remote sources.

It Extracts data from diverse sources, Transforms it through customizable and reusable operations, and Loads it into your desired destination with minimal configuration.

Built to be the Swiss Army Knife of ETL solutions, SerenadeFlow offers a simple yet extensible architecture that makes data movement and transformation intuitive—whether you're a data engineer, analyst, or developer.

## Requirements

The project is configured to operate in _Python >= 3.8_ enviornments.

## Quickstart

Coming Soon.

## Data Source Configuration

SerenadeFlow supports extracting data from various sources. The `data_source` and `data_source_path` parameters in the pipeline configuration determine where the data is extracted from.

### Local JSON Files

To extract data from local JSON files, set `data_source` to `local` and `data_source_path` to the directory containing your JSON files. The pipeline will read all `.json` files within the specified directory.

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
