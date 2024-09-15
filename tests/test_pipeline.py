"""Test Template."""

import pytest

from serenade_flow import pipeline


@pytest.mark.unit
def test_extact_local():
    """Test Local Extraction."""
    pipeline.configure({
        "data_source": "local",
        "data_source_path": "/path/to/local/directory",
        "data_format" : "csv"
    })
    data = pipeline.extract()
    assert len(data) == 3


@pytest.mark.unit
def test_extact_remote():
    """Test Remote Extraction."""
    pipeline.configure({
        "data_source": "remote",
        "data_source_path": "http://path/to/storage/bucket",
        "data_format" : "csv"
    })
    data = pipeline.extract()
    assert data == {}


@pytest.mark.unit
def test_load():
    """Test Loading Data."""
    pipeline.configure({
        "data_source": "local",
        "data_source_path": "http://path/to/storage/bucket",
        "data_format" : "csv"
    })
    data = pipeline.extract()
    assert pipeline.load(data, "output") == "Data loaded successfully"
