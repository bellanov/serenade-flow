# FantasyAce Cloud Functions Integration

This directory contains examples for integrating SerenadeFlow with FantasyAce Cloud Functions for real-time sports odds data.

**Purpose:** Demonstrates integration with FantasyAce Cloud Functions endpoints for faster odds information retrieval than existing storage methods.

**Examples:**
- `fantasyace_cf_example.py` - Data extraction from Cloud Functions with event-by-event or sport-based queries

**Features:**
- Real-time data from Cloud Functions endpoints
- Robust error handling and retry logic
- Support for both event-specific and sport-based queries
- Data normalization and flattening for pipeline compatibility

---

## Usage

### Fetch Specific Event Odds

```bash
python examples/cloud_integrations/fantasyace/fantasyace_cf_example.py \
    --event-id a03f89d3aaadf737deb09be24986d5bf \
    --output-prefix fantasyace
```

### Fetch Events by Sport and Limit

```bash
python examples/cloud_integrations/fantasyace/fantasyace_cf_example.py \
    --sport-key americanfootball_nfl --limit 10 --output-prefix fantasyace
```

---

## Endpoints

- **Sports Query:** `https://getsports-twqu2g763q-uc.a.run.app/`
- **Events Query:** `https://getevents-twqu2g763q-uc.a.run.app/?sportKey={sport}&limit={limit}`
- **Event Odds Query:** `https://geteventodds-twqu2g763q-uc.a.run.app/?eventId={eventId}`

---

## Data Flow

1. **Extract:** Fetch data from Cloud Functions endpoints
2. **Transform:** Normalize and flatten nested JSON structures
3. **Load:** Output to CSV or Parquet format

The plugin automatically handles the `{"data": [...]}` wrapper structure returned by the Cloud Functions and normalizes the data to match the pipeline's expected schema.
