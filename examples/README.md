# SerenadeFlow Examples

Examples are recipes that demonstrate how to use SerenadeFlow with different plugins, data sources, and configurations.

## Example Structure

Each example should be:
- Self-contained and runnable independently
- Configurable via command-line arguments
- Testable

## Example Categories

### Basic Examples
- `hello_world.py`: Simple introduction
- `quickstart.py`: Remote data extraction
- `basic_etl_pipeline.py`: Core ETL pipeline
- `sports_odds_processing.py`: Sports betting data processing

### Cloud Integrations
- `cloud_integrations/gcs/`: Google Cloud Storage integration examples
- `cloud_integrations/Firestore/`: Firestore integration examples

## Running Examples

Most examples can be run directly:

```bash
python3 examples/basic_etl_pipeline.py
```

Some examples accept command-line arguments:

```bash
python3 examples/cloud_integrations/Firestore/fantasyace_cf_example.py --sport-key americanfootball_nfl --limit 10
```

## Contributing Examples

When contributing a new example:
1. Place it in the appropriate directory
2. Add command-line argument support where useful
3. Add tests in `tests/test_examples.py`

