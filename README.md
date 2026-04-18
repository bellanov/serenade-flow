# SerenadeFlow

**SerenadeFlow** is a powerful and flexible *ETL (Extract, Transform, Load)* pipeline framework designed to streamline data processing from both **local** and **remote** sources.

It *Extracts* data from diverse sources, *Transforms* it through customizable and reusable operations, and *Loads* it into your desired destination with minimal configuration.

Built to be the Swiss Army Knife of ETL solutions, SerenadeFlow offers a simple yet extensible architecture that makes data movement and transformation intuitive—whether you're a data engineer, analyst, or developer.

## Requirements

The project is configured to operate in _Python >= 3.8_ enviornments.

## Output Formats

SerenadeFlow supports multiple output formats for your processed data:

### CSV Format (Default)

The traditional CSV format is the default output format, providing wide compatibility with various tools and applications.

### Parquet Format
Parquet is a column-oriented storage format that offers compression and better performance for analytics workloads.
