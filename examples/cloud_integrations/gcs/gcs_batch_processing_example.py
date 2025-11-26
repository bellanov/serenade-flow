"""Example: Batch Processing with Data Quality Metrics and Analytics.

This example demonstrates sophisticated batch processing capabilities with
parallel processing simulation, data quality metrics, and analytics
for the organized bucket structure.
"""

from serenade_flow import pipeline
import time
from typing import Dict, List, Any
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict
import statistics


@dataclass
class ProcessingMetrics:
    """Comprehensive metrics for batch processing operations."""
    total_files: int
    successful_files: int
    failed_files: int
    total_records: int
    processing_time: float
    avg_records_per_file: float
    data_quality_score: float
    error_rate: float
    throughput: float  # records per second


@dataclass
class DataQualityReport:
    """Data quality assessment for extracted data."""
    file_path: str
    record_count: int
    column_count: int
    missing_values: int
    duplicate_records: int
    data_types: Dict[str, str]
    quality_score: float
    issues: List[str] = field(default_factory=list)


class BatchProcessor:
    """Batch processor with quality metrics and analytics."""

    def __init__(self, bucket_url: str, max_concurrent: int = 3):
        self.bucket_url = bucket_url
        self.max_concurrent = max_concurrent
        self.processing_history = []
        self.quality_reports = []

    def _extract_dataframe(self, data: Dict[str, Any]) -> Any:
        """Extract DataFrame from data dictionary."""
        if not data:
            return None
        for key, value in data.items():
            if hasattr(value, 'shape') and value.shape[0] > 0:
                return value
        return None

    def _analyze_data_types(self, df: Any) -> Dict[str, str]:
        """Analyze data types of DataFrame columns."""
        data_types = {}
        for col in df.columns:
            if df[col].dtype == 'object':
                data_types[col] = 'string'
            elif 'int' in str(df[col].dtype):
                data_types[col] = 'integer'
            elif 'float' in str(df[col].dtype):
                data_types[col] = 'float'
            else:
                data_types[col] = str(df[col].dtype)
        return data_types

    def _calculate_quality_score(
        self, record_count: int, column_count: int,
        missing_values: int, duplicate_records: int
    ) -> tuple[float, List[str]]:
        """Calculate quality score and identify issues."""
        quality_score = 100.0
        issues = []

        if missing_values > 0:
            missing_rate = missing_values / (record_count * column_count)
            quality_score -= missing_rate * 30
            issues.append(f"Missing values: {missing_values}")

        if duplicate_records > 0:
            duplicate_rate = duplicate_records / record_count
            quality_score -= duplicate_rate * 20
            issues.append(f"Duplicate records: {duplicate_records}")

        if record_count == 0:
            quality_score = 0
            issues.append("No records found")

        quality_score = max(0, quality_score)
        return quality_score, issues

    def assess_data_quality(
        self, data: Dict[str, Any], file_path: str
    ) -> DataQualityReport:
        """Assess data quality of extracted data."""
        df = self._extract_dataframe(data)

        if df is None:
            return DataQualityReport(
                file_path=file_path,
                record_count=0,
                column_count=0,
                missing_values=0,
                duplicate_records=0,
                data_types={},
                quality_score=0.0,
                issues=["No valid DataFrame found"]
            )

        record_count = len(df)
        column_count = len(df.columns)
        missing_values = df.isnull().sum().sum()
        duplicate_records = len(df) - len(df.drop_duplicates())
        data_types = self._analyze_data_types(df)
        quality_score, issues = self._calculate_quality_score(
            record_count, column_count, missing_values, duplicate_records
        )

        return DataQualityReport(
            file_path=file_path,
            record_count=record_count,
            column_count=column_count,
            missing_values=missing_values,
            duplicate_records=duplicate_records,
            data_types=data_types,
            quality_score=quality_score,
            issues=issues
        )

    def process_file_with_analytics(self, file_path: str) -> Dict[str, Any]:
        """Process a single file with comprehensive analytics."""
        start_time = time.time()
        try:
            print(f"🔍 Processing: {file_path}")
            pipeline.configure({
                "data_source": "remote",
                "data_source_path": self.bucket_url + file_path,
                "data_format": "json"
            })
            data = pipeline.extract()
            quality_report = self.assess_data_quality(data, file_path)
            self.quality_reports.append(quality_report)
            processing_time = time.time() - start_time
            result = {
                'success': True,
                'file_path': file_path,
                'processing_time': processing_time,
                'record_count': quality_report.record_count,
                'quality_score': quality_report.quality_score,
                'data': data,
                'quality_report': quality_report
            }
            print(
                f"  ✅ {quality_report.record_count} records, "
                f"{quality_report.column_count} columns"
            )
            print(f"  📊 Quality Score: {quality_report.quality_score:.1f}/100")
            if quality_report.issues:
                print(f"  ⚠️  Issues: {', '.join(quality_report.issues)}")
            return result
        except Exception as e:
            processing_time = time.time() - start_time
            print(f"  ❌ Error: {str(e)}")
            return {
                'success': False,
                'file_path': file_path,
                'processing_time': processing_time,
                'error': str(e),
                'record_count': 0,
                'quality_score': 0
            }

    def process_batch_with_analytics(self, file_paths: List[str]) -> ProcessingMetrics:
        """Process batch with analytics and parallel simulation."""
        print("\n🚀 Batch Processing with Analytics")
        print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Processing {len(file_paths)} files with quality assessment...")
        print("=" * 70)
        start_time = time.time()
        results = []
        for i, file_path in enumerate(file_paths):
            print(f"\n📁 File {i + 1}/{len(file_paths)}")
            result = self.process_file_with_analytics(file_path)
            results.append(result)
            if i < len(file_paths) - 1:
                time.sleep(0.5)
        total_time = time.time() - start_time
        successful_files = sum(1 for r in results if r['success'])
        failed_files = len(results) - successful_files
        total_records = sum(r['record_count'] for r in results if r['success'])
        quality_scores = [r['quality_score'] for r in results if r['success']]
        avg_quality = statistics.mean(quality_scores) if quality_scores else 0
        throughput = total_records / total_time if total_time > 0 else 0
        metrics = ProcessingMetrics(
            total_files=len(file_paths),
            successful_files=successful_files,
            failed_files=failed_files,
            total_records=total_records,
            processing_time=total_time,
            avg_records_per_file=(
                total_records / successful_files if successful_files > 0 else 0
            ),
            data_quality_score=avg_quality,
            error_rate=failed_files / len(file_paths) * 100,
            throughput=throughput
        )
        self._print_analytics_summary(metrics, results)
        return metrics

    def _print_analytics_summary(self, metrics: ProcessingMetrics, results: List[Dict]):
        """Print comprehensive analytics summary."""
        print(f"\n{'=' * 70}")
        print("📈 ANALYTICS SUMMARY")
        print(f"{'=' * 70}")
        print("📊 Processing Metrics:")
        print(f"   • Total Files: {metrics.total_files}")
        print(f"   • Successful: {metrics.successful_files}")
        print(f"   • Failed: {metrics.failed_files}")
        success_rate = (metrics.successful_files / metrics.total_files) * 100
        print(f"   • Success Rate: {success_rate:.1f}%")
        print(f"   • Error Rate: {metrics.error_rate:.1f}%")
        print("\n📈 Performance Metrics:")
        print(f"   • Total Records: {metrics.total_records:,}")
        print(f"   • Avg Records/File: {metrics.avg_records_per_file:.1f}")
        print(f"   • Processing Time: {metrics.processing_time:.2f}s")
        print(f"   • Throughput: {metrics.throughput:.1f} records/sec")
        print("\n🔍 Quality Metrics:")
        print(f"   • Avg Quality Score: {metrics.data_quality_score:.1f}/100")
        quality_scores = [r['quality_score'] for r in results if r['success']]
        if quality_scores:
            print(
                f"   • Quality Range: {min(quality_scores):.1f} - "
                f"{max(quality_scores):.1f}"
            )
            if len(quality_scores) > 1:
                print(
                    f"   • Quality Std Dev: {statistics.stdev(quality_scores):.1f}"
                )
        if self.quality_reports:
            data_types = defaultdict(int)
            for report in self.quality_reports:
                for dtype in report.data_types.values():
                    data_types[dtype] += 1
            print("\n📋 Data Type Distribution:")
            for dtype, count in data_types.items():
                print(f"   • {dtype}: {count} columns")


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


# --- REPLACE CATEGORIES with new odds file discovery ---
odds_files_by_format = get_all_odds_files_by_format()


# Flatten for batch processing, tagging each with its format
def get_tagged_odds_file_list():
    tagged = []
    for odds_format, file_list in odds_files_by_format.items():
        for file_path in file_list:
            tagged.append({"file_path": file_path, "odds_format": odds_format})
    return tagged


def demonstrate_batch_processing():
    """Demonstrate batch processing with analytics for both odds formats."""
    print("\n🎯 Batch Processing with SerenadeFlow (American & Decimal Odds)")
    print("=" * 70)
    processor = BatchProcessor(
        bucket_url=BUCKET_BASE_URL,
        max_concurrent=3
    )
    tagged_files = get_tagged_odds_file_list()
    all_metrics = {}
    for odds_format in odds_files_by_format:
        format_files = [f["file_path"] for f in tagged_files if f["odds_format"] == odds_format]
        print(f"\n🎯 Processing {odds_format.upper()} Odds Format")
        print("-" * 50)
        metrics = processor.process_batch_with_analytics(format_files)
        all_metrics[odds_format] = metrics
    print("\n🏆 OVERALL PERFORMANCE SUMMARY")
    print("=" * 70)
    total_files = sum(m.total_files for m in all_metrics.values())
    total_records = sum(m.total_records for m in all_metrics.values())
    total_time = sum(m.processing_time for m in all_metrics.values())
    avg_quality = statistics.mean([m.data_quality_score for m in all_metrics.values()])
    print(f"📊 Total Files Processed: {total_files}")
    print(f"📈 Total Records Extracted: {total_records:,}")
    print(f"⏱️  Total Processing Time: {total_time:.2f}s")
    print(f"🔍 Overall Quality Score: {avg_quality:.1f}/100")
    print(f"🚀 Overall Throughput: {total_records / total_time:.1f} records/sec")
    return all_metrics


if __name__ == "__main__":
    processing_metrics = demonstrate_batch_processing()
    print("\n💡 NEXT STEPS FOR DATA PROCESSING")
    print("   # pipeline.load(processing_metrics, 'batch_processing')")
    print("   # Export quality reports and analytics...")
