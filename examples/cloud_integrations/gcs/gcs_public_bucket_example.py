"""Example: Extracting Data from a Public GCS Bucket with SerenadeFlow.

This example demonstrates data extraction with validation, retry logic,
and custom processing patterns for the organized bucket structure.
"""

from serenade_flow import pipeline
import time
from typing import Dict, Any
from dataclasses import dataclass
from datetime import datetime


@dataclass
class DataExtractionResult:
    """Custom result container for extraction operations."""
    success: bool
    file_path: str
    record_count: int
    extraction_time: float
    error_message: str = None
    data_preview: Dict[str, Any] = None


class GCSDataExtractor:
    """Data extractor with validation and retry logic."""

    def __init__(self, bucket_url: str, max_retries: int = 3, retry_delay: float = 1.0):
        self.bucket_url = bucket_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.extraction_history = []

    def validate_data_structure(self, data: Dict[str, Any]) -> bool:
        """Validate extracted data structure."""
        if not data:
            return False
        for key, value in data.items():
            if hasattr(value, 'columns') and hasattr(value, 'shape'):
                if value.shape[0] > 0:
                    return True
        return False

    def extract_with_retry(self, file_path: str) -> DataExtractionResult:
        """Extract data with retry logic and validation."""
        start_time = time.time()
        for attempt in range(self.max_retries):
            try:
                print(f"🔄 Attempt {attempt + 1}/{self.max_retries} for {file_path}")
                pipeline.configure({
                    "data_source": "remote",
                    "data_source_path": self.bucket_url + file_path,
                    "data_format": "json"
                })
                data = pipeline.extract()
                if self.validate_data_structure(data):
                    extraction_time = time.time() - start_time
                    record_count = sum(
                        len(df) for df in data.values() if hasattr(df, 'shape')
                    )
                    data_preview = {}
                    for key, df in data.items():
                        if hasattr(df, 'head'):
                            data_preview[key] = {
                                'shape': df.shape,
                                'columns': list(df.columns),
                                'sample_data': (
                                    df.head(2).to_dict('records') if len(df) > 0 else []
                                )
                            }
                    result = DataExtractionResult(
                        success=True,
                        file_path=file_path,
                        record_count=record_count,
                        extraction_time=extraction_time,
                        data_preview=data_preview
                    )
                    print(
                        f"✅ Successfully extracted {record_count} records in "
                        f"{extraction_time:.2f}s"
                    )
                    return result
                else:
                    raise ValueError("Invalid data structure extracted")
            except Exception as e:
                error_msg = f"Attempt {attempt + 1} failed: {str(e)}"
                print(f"❌ {error_msg}")
                if attempt < self.max_retries - 1:
                    print(f"⏳ Retrying in {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                else:
                    extraction_time = time.time() - start_time
                    return DataExtractionResult(
                        success=False,
                        file_path=file_path,
                        record_count=0,
                        extraction_time=extraction_time,
                        error_message=str(e)
                    )

    def process_event_files(self, file_paths: list) -> Dict[str, DataExtractionResult]:
        """Process multiple event files with comprehensive logging."""
        print(
            f"\n🚀 Starting batch extraction at "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        print(f"📊 Processing {len(file_paths)} files with retry logic...")
        results = {}
        total_records = 0
        successful_extractions = 0
        for file_path in file_paths:
            print(f"\n{'=' * 60}")
            result = self.extract_with_retry(file_path)
            results[file_path] = result
            self.extraction_history.append(result)
            if result.success:
                successful_extractions += 1
                total_records += result.record_count
                if result.data_preview:
                    print(f"📋 Data Preview for {file_path}:")
                    for key, preview in result.data_preview.items():
                        print(
                            f"  - {key}: {preview['shape'][0]} records, "
                            f"{len(preview['columns'])} columns"
                        )
            else:
                print(f"💥 Failed to extract {file_path}: {result.error_message}")
        print(f"\n{'=' * 60}")
        print("📈 EXTRACTION SUMMARY")
        print(f"   Files processed: {len(file_paths)}")
        print(f"   Successful: {successful_extractions}")
        print(f"   Failed: {len(file_paths) - successful_extractions}")
        print(f"   Total records: {total_records}")
        avg_time = sum(r.extraction_time for r in results.values()) / len(results)
        print(f"   Average extraction time: {avg_time:.2f}s")
        total_time = sum(r.extraction_time for r in results.values())
        print(f"   Total processing time: {total_time:.2f}s")
        return results


# Public GCS bucket base URL
BUCKET_BASE_URL = "https://storage.googleapis.com/odds-data-samples-4vuoq93m/"

# --- NEW: Utility to list all odds files by format ---
def get_all_odds_files_by_format():
    """Return a dict mapping odds format to list of file paths (relative to bucket root)."""
    # In production, you would list files from the bucket using an API or manifest.
    # For this example, we simulate with static lists (update as needed).
    return {
        "american": [
            "odds/american/event_008740fcf1af65b0cc9e79.json",
            "odds/american/event_0089bc8773d8ce4ce20f9df90723cac9.json",
            # ... add more as needed ...
        ],
        "decimal": [
            "odds/decimal/event_0089bc8773d8ce4ce20f9df90723cac9.json",
            # ... add more as needed ...
        ]
    }

# --- REPLACE EVENT_FILES with new odds file discovery ---
odds_files_by_format = get_all_odds_files_by_format()

def get_tagged_odds_file_list():
    tagged = []
    for odds_format, file_list in odds_files_by_format.items():
        for file_path in file_list:
            tagged.append({"file_path": file_path, "odds_format": odds_format})
    return tagged


# --- UPDATE demonstrate_extraction to process both formats ---
def demonstrate_extraction():
    """Demonstrate extraction capabilities for both odds formats."""
    print("\n🎯 GCS Data Extraction with SerenadeFlow (American & Decimal Odds)")
    print("=" * 60)
    extractor = GCSDataExtractor(
        bucket_url=BUCKET_BASE_URL,
        max_retries=3,
        retry_delay=1.5
    )
    tagged_files = get_tagged_odds_file_list()
    all_results = {}
    for odds_format in odds_files_by_format:
        format_files = [f["file_path"] for f in tagged_files if f["odds_format"] == odds_format]
        print(f"\n🎯 Extracting {odds_format.upper()} Odds Format")
        print("-" * 50)
        results = extractor.process_event_files(format_files)
        all_results[odds_format] = results
    print("\n🔍 EXTRACTION SUMMARY BY FORMAT")
    for odds_format, results in all_results.items():
        successful = sum(1 for r in results.values() if r.success)
        print(f"   {odds_format.upper()}: {successful}/{len(results)} files succeeded")
    return all_results


if __name__ == "__main__":
    extraction_results = demonstrate_extraction()
    print("\n💡 NEXT STEPS")
    print("   # pipeline.load(extraction_results, 'gcs_extraction')")
    print("   # Further data processing and analysis...")
