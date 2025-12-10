import time
from typing import Any, Dict

from serenade_flow import pipeline


class GCSDataExtractorPlugin:
    """

    Community plugin for extracting data from a public GCS bucket with validation and retry logic.

    """

    def __init__(
        self, bucket_url: str = None, max_retries: int = 3, retry_delay: float = 1.0
    ):

        self.bucket_url = bucket_url

        self.max_retries = max_retries

        self.retry_delay = retry_delay

    def configure(
        self, bucket_url: str = None, max_retries: int = None, retry_delay: float = None
    ):

        if bucket_url is not None:

            self.bucket_url = bucket_url

        if max_retries is not None:

            self.max_retries = max_retries

        if retry_delay is not None:

            self.retry_delay = retry_delay

    def extract_with_retry(self, file_path: str) -> Dict[str, Any]:
        """Extract data with retry logic and validation."""

        start_time = time.time()

        for attempt in range(self.max_retries):

            try:

                pipeline.configure(
                    {
                        "data_source": "remote",
                        "data_source_path": (self.bucket_url or "") + file_path,
                        "data_format": "json",
                    }
                )

                data = pipeline.extract()

                if self.validate_data_structure(data):

                    extraction_time = time.time() - start_time

                    record_count = sum(
                        len(df) for df in data.values() if hasattr(df, "shape")
                    )

                    return {
                        "success": True,
                        "file_path": file_path,
                        "record_count": record_count,
                        "extraction_time": extraction_time,
                        "data": data,
                    }

                else:

                    raise ValueError("Invalid data structure extracted")

            except Exception as e:

                if attempt < self.max_retries - 1:

                    time.sleep(self.retry_delay)

                else:

                    extraction_time = time.time() - start_time

                    return {
                        "success": False,
                        "file_path": file_path,
                        "record_count": 0,
                        "extraction_time": extraction_time,
                        "error_message": str(e),
                    }

    def validate_data_structure(self, data: Dict[str, Any]) -> bool:

        if not data:

            return False

        for value in data.values():

            if hasattr(value, "columns") and hasattr(value, "shape"):

                if value.shape[0] > 0:

                    return True

        return False
