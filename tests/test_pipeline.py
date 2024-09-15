"""Test Template."""

import pytest

from serenade_flow import pipeline


@pytest.mark.unit
def test_etl_pipeline():
    """Validate pipeline execution."""
    assert pipeline.etl_pipeline() == "Extract, Transform, and Load!!!"
