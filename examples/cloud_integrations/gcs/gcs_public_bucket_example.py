"""Example: Extracting Data from a Public GCS Bucket with SerenadeFlow.

This example demonstrates data extraction with validation, retry logic,
and custom processing patterns for the organized bucket structure.
"""

from serenade_flow import pipeline
import time
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class DataExtractionResult:
    """Custom result container for extraction operations."""
    success: bool
    file_path: str
    record_count: int
    extraction_time: float
    error_message: Optional[str] = None
    data_preview: Optional[Dict[str, Any]] = None

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
        
        # Check if data contains expected DataFrame structure
        for key, value in data.items():
            if hasattr(value, 'columns') and hasattr(value, 'shape'):
                if value.shape[0] > 0:  # Has records
                    return True
        return False
    
    def extract_with_retry(self, file_path: str) -> DataExtractionResult:
        """Extract data with retry logic and validation."""
        start_time = time.time()
        
        for attempt in range(self.max_retries):
            try:
                print(f"🔄 Attempt {attempt + 1}/{self.max_retries} for {file_path}")
                
                # Configure pipeline for this file
                pipeline.configure({
                    "data_source": "remote",
                    "data_source_path": self.bucket_url + file_path,
                    "data_format": "json"
                })
                
                # Extract data
                data = pipeline.extract()
                
                # Validate extracted data
                if self.validate_data_structure(data):
                    extraction_time = time.time() - start_time
                    record_count = sum(len(df) for df in data.values() if hasattr(df, 'shape'))
                    
                    # Create data preview
                    data_preview = {}
                    for key, df in data.items():
                        if hasattr(df, 'head'):
                            data_preview[key] = {
                                'shape': df.shape,
                                'columns': list(df.columns),
                                'sample_data': df.head(2).to_dict('records') if len(df) > 0 else []
                            }
                    
                    result = DataExtractionResult(
                        success=True,
                        file_path=file_path,
                        record_count=record_count,
                        extraction_time=extraction_time,
                        data_preview=data_preview
                    )
                    
                    print(f"✅ Successfully extracted {record_count} records in {extraction_time:.2f}s")
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
        print(f"\n🚀 Starting batch extraction at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Processing {len(file_paths)} files with retry logic...")
        
        results = {}
        total_records = 0
        successful_extractions = 0
        
        for file_path in file_paths:
            print(f"\n{'='*60}")
            result = self.extract_with_retry(file_path)
            results[file_path] = result
            self.extraction_history.append(result)
            
            if result.success:
                successful_extractions += 1
                total_records += result.record_count
                
                # Show data preview for successful extractions
                if result.data_preview:
                    print(f"📋 Data Preview for {file_path}:")
                    for key, preview in result.data_preview.items():
                        print(f"  - {key}: {preview['shape'][0]} records, {len(preview['columns'])} columns")
            else:
                print(f"💥 Failed to extract {file_path}: {result.error_message}")
        
        # Summary statistics
        print(f"\n{'='*60}")
        print(f"📈 EXTRACTION SUMMARY")
        print(f"   Files processed: {len(file_paths)}")
        print(f"   Successful: {successful_extractions}")
        print(f"   Failed: {len(file_paths) - successful_extractions}")
        print(f"   Total records: {total_records}")
        print(f"   Average extraction time: {sum(r.extraction_time for r in results.values()) / len(results):.2f}s")
        
        return results

# Public GCS bucket base URL
BUCKET_BASE_URL = "https://storage.googleapis.com/odds-data-samples-4vuoq93m/"

# Example files from the organized bucket structure
EVENT_FILES = [
    "odds/event_96395d8faab66cf7b72830844f66eda7.json",
    "odds/event_968d201306e35699b83b5bb24289914c.json",
    "odds/event_96a3cf0917accb689197ad21378e1efb.json"
]

def demonstrate_extraction():
    """Demonstrate extraction capabilities."""
    print("\n🎯 GCS Data Extraction with SerenadeFlow")
    print("=" * 60)
    
    # Initialize extractor
    extractor = GCSDataExtractor(
        bucket_url=BUCKET_BASE_URL,
        max_retries=3,
        retry_delay=1.5
    )
    
    # Process event files with features
    results = extractor.process_event_files(EVENT_FILES)
    
    # Demonstrate data analysis capabilities
    print(f"\n🔍 DATA ANALYSIS")
    successful_results = [r for r in results.values() if r.success]
    
    if successful_results:
        avg_records = sum(r.record_count for r in successful_results) / len(successful_results)
        fastest_extraction = min(successful_results, key=lambda x: x.extraction_time)
        
        print(f"   Average records per file: {avg_records:.1f}")
        print(f"   Fastest extraction: {fastest_extraction.file_path} ({fastest_extraction.extraction_time:.2f}s)")
        print(f"   Total processing time: {sum(r.extraction_time for r in results.values()):.2f}s")
    
    return results

if __name__ == "__main__":
    # Run extraction demonstration
    extraction_results = demonstrate_extraction()
    
    # Show how to use the results for further processing
    print(f"\n💡 NEXT STEPS")
    print("   # pipeline.load(extraction_results, 'gcs_extraction')")
    print("   # Further data processing and analysis...") 