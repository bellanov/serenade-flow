"""Test Template."""

import pytest

from src import pipeline


@pytest.mark.unit
def test_app():
    """Validate pipeline execution."""
    assert pipeline.etl_pipeline() == "Extract, Transform, and Load!!!"
