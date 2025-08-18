# Cloud Integrations Examples

This directory contains self-contained examples for integrating SerenadeFlow with various cloud services (e.g., Google Cloud Storage, AWS S3, Azure Blob Storage, Firestore via HTTP API, etc.).

**Key Principles:**
- All cloud-specific logic is isolated here as examples.
- Use public endpoints or HTTP APIs for remote data access.
- Each example should be self-contained and not affect the core pipeline or other examples.
- Organized by cloud provider for better maintainability.

---

## Cloud Provider Examples

### Google Cloud Storage (GCS)
- **Directory:** `gcs/`
- **Purpose:** Demonstrates GCS integration with organized bucket structures
- **Examples:**
  - `gcs_public_bucket_example.py` - Data extraction with retry logic and validation
  - `gcs_batch_processing_example.py` - Batch processing with data quality metrics and analytics
- **Features:**
  - Works with organized bucket structure using `odds/` prefix
  - Robust error handling and retry logic
  - Data quality assessment and analytics
  - Production-ready patterns for batch processing

### FantasyAce Cloud Functions
- **Directory:** `fantasyace/`
- **Purpose:** Demonstrates integration with FantasyAce Cloud Functions for real-time sports odds data
- **Examples:**
  - `fantasyace_cf_example.py` - Data extraction from Cloud Functions with event-by-event or sport-based queries
- **Features:**
  - Real-time data from Cloud Functions endpoints
  - Robust error handling and retry logic
  - Support for both event-specific and sport-based queries
  - Data normalization and flattening for pipeline compatibility

### AWS S3 (Coming Soon)
- **Directory:** `aws/` (planned)
- **Purpose:** Will demonstrate S3 integration with similar patterns
- **Planned Examples:**
  - S3 public bucket access
  - Batch processing with S3
  - Data quality metrics for S3 data

### Azure Blob Storage (Coming Soon)
- **Directory:** `azure/` (planned)
- **Purpose:** Will demonstrate Azure Blob Storage integration
- **Planned Examples:**
  - Azure Blob public access
  - Batch processing with Azure
  - Cross-cloud data processing

### Firestore HTTP API (Coming Soon)
- **Directory:** `firestore/` (planned)
- **Purpose:** Will demonstrate Firestore integration via HTTP API
- **Planned Examples:**
  - Firestore data extraction
  - Real-time data processing
  - Document-based analytics

---

## Usage

### Running GCS Examples

```bash
# GCS data extraction
python examples/cloud_integrations/gcs/gcs_public_bucket_example.py

# GCS batch processing
python examples/cloud_integrations/gcs/gcs_batch_processing_example.py

# FantasyAce Cloud Functions
python examples/cloud_integrations/fantasyace/fantasyace_cf_example.py \
    --event-id a03f89d3aaadf737deb09be24986d5bf --output-prefix fantasyace
```

### Running All Examples

```bash
# Execute all available examples
./scripts/examples.sh
```

---

## Architecture Benefits

### 1. **Organized Structure**
- Each cloud provider has its own directory
- Consistent patterns across providers
- Easy to add new cloud integrations

### 2. **Robust Features**
- Retry logic and error handling
- Data quality assessment
- Performance analytics
- Production-ready patterns

### 3. **Scalable Design**
- Modular approach for adding new providers
- Reusable patterns and utilities
- Consistent API across examples

### 4. **Real-World Focus**
- Handles real data inconsistencies
- Demonstrates robust error handling
- Shows production-ready thinking

---

## Contributing

To add new cloud provider examples:

1. Create a new directory: `examples/cloud_integrations/{provider}/`
2. Add provider-specific examples
3. Create a `README.md` for the provider
4. Update this main README
5. Add examples to `scripts/examples.sh`

Follow the established patterns for consistency across all cloud integrations. 