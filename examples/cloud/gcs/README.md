> **Note:**
> These examples demonstrate advanced usage, prototyping, and custom workflows for GCS extraction and batch processing. Core GCS extraction and data quality logic are now integrated into the main pipeline and available as plugins. Use these examples for learning, inspiration, or building more complex workflows.

# Google Cloud Storage (GCS) Integration Examples

This directory contains examples for integrating SerenadeFlow with Google Cloud Storage, demonstrating how to extract data from public GCS buckets with organized data structures.

## Examples Overview

### 1. GCS Data Extraction (`gcs_public_bucket_example.py`)
- **Purpose**: Demonstrates data extraction with validation, retry logic, and custom processing patterns
- **Features**:
  - Retry logic with configurable attempts and delays
  - Data structure validation
  - Comprehensive error handling
  - Real-time analytics and metrics
  - Data preview and quality assessment

### 2. Batch Processing (`gcs_batch_processing_example.py`)
- **Purpose**: Demonstrates sophisticated batch processing capabilities with data quality metrics and analytics
- **Features**:
  - Parallel processing simulation
  - Data quality assessment and scoring
  - Comprehensive analytics and reporting
  - Error rate tracking
  - Throughput optimization

## Bucket Structure

The examples work with the organized bucket structure at:
```
https://storage.googleapis.com/odds-data-samples-4vuoq93m/
```

**Organized Structure:**
- `odds/` prefix for all event JSON files
- Files follow pattern: `odds/event_*.json`
- Hundreds of event files available for processing

## Usage

### Running Individual Examples

```bash
# Data extraction with retry logic
python examples/cloud_integrations/gcs/gcs_public_bucket_example.py

# Batch processing with analytics
python examples/cloud_integrations/gcs/gcs_batch_processing_example.py
```

### Key Features Demonstrated

1. **Robust Error Handling**
   - Retry logic for network issues
   - Validation of data structure
   - Graceful handling of empty/invalid data

2. **Data Quality Metrics**
   - Missing value detection
   - Duplicate record identification
   - Quality scoring (0-100)
   - Data type analysis

3. **Performance Analytics**
   - Processing time tracking
   - Throughput calculation (records/sec)
   - Success/failure rate analysis
   - Resource utilization metrics

4. **Production-Ready Patterns**
   - Configurable retry parameters
   - Comprehensive logging
   - Error reporting and recovery
   - Scalable batch processing

## Data Structure

The examples process sports betting data with the following structure:
- **Event Information**: Teams, sport type, commence time
- **Bookmaker Data**: Multiple bookmakers with markets
- **Outcome Data**: Betting odds and points
- **Quality Metrics**: Data completeness and validity scores

## Next Steps

These examples can be extended for:
- AWS S3 integration
- Azure Blob Storage integration
- Firestore HTTP API integration
- Multi-cloud data processing
- Real-time data streaming 