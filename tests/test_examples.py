"""Tests for SerenadeFlow examples.

This module tests that examples can be executed and produce expected results.
Examples should be self-contained and testable.
"""

import pytest
import json
import os
import tempfile
from pathlib import Path
import sys

# Add examples directory to path for imports
examples_dir = Path(__file__).parent.parent / "examples"
sys.path.insert(0, str(examples_dir))


@pytest.fixture
def sample_data_directory(tmp_path):
    """Create a temporary directory with sample JSON files."""
    data = [
        {
            "id": "1",
            "sport_key": "basketball_nba",
            "sport_title": "NBA",
            "home_team": "team a",
            "away_team": "team b",
            "commence_time": "2025-02-04T00:10:00Z",
            "bookmakers": [
                {
                    "key": "bookmaker1",
                    "title": "Bookmaker 1",
                    "markets": [
                        {
                            "key": "h2h",
                            "last_update": "2025-02-04T00:05:00Z",
                            "outcomes": [
                                {"name": "team a", "price": 1.5},
                                {"name": "team b", "price": 2.5},
                            ],
                        }
                    ],
                }
            ],
        }
    ]
    json_file = tmp_path / "test_data.json"
    json_file.write_text(json.dumps(data))
    return str(tmp_path)


@pytest.mark.functional
def test_basic_etl_pipeline_example(sample_data_directory, tmp_path, monkeypatch):
    """Test that basic_etl_pipeline example."""
    # Import the example module
    from basic_etl_pipeline import (
        extract_local_json_files,
        transform_data,
        load_data,
        assess_quality,
    )
    
    # Test extraction
    data_frames = extract_local_json_files(sample_data_directory)
    assert len(data_frames) > 0
    assert "test_data.json" in data_frames
    
    # Test quality assessment
    assess_quality(data_frames)
    
    # Test transformation
    transformed = transform_data(data_frames)
    assert len(transformed) > 0
    assert "test_data.json" in transformed
    
    # Verify transformation applied
    df = transformed["test_data.json"]
    assert "processed_at" in df.columns
    assert "source_file" in df.columns
    
    # Test loading (to a temp directory)
    with monkeypatch.context() as m:
        # Change to temp directory for output
        original_cwd = os.getcwd()
        m.chdir(tmp_path)
        try:
            result = load_data(transformed, "test_output", "csv")
            assert result is not None
            assert "CSV" in result
            
            # Verify file was created
            output_file = tmp_path / "test_output_test_data.csv"
            assert output_file.exists()
        finally:
            os.chdir(original_cwd)


@pytest.mark.functional
def test_basic_etl_pipeline_parquet_output(sample_data_directory, tmp_path, monkeypatch):
    """Test basic_etl_pipeline with Parquet output."""
    from basic_etl_pipeline import extract_local_json_files, transform_data, load_data
    
    data_frames = extract_local_json_files(sample_data_directory)
    transformed = transform_data(data_frames)
    
    with monkeypatch.context() as m:
        original_cwd = os.getcwd()
        m.chdir(tmp_path)
        try:
            result = load_data(transformed, "test_output", "parquet")
            assert result is not None
            assert "PARQUET" in result
            
            # Verify parquet file was created
            output_file = tmp_path / "test_output_test_data.parquet"
            assert output_file.exists()
        finally:
            os.chdir(original_cwd)


@pytest.mark.functional
def test_basic_etl_pipeline_empty_directory(tmp_path):
    """Test basic_etl_pipeline with empty directory."""
    from basic_etl_pipeline import extract_local_json_files
    
    data_frames = extract_local_json_files(str(tmp_path))
    assert len(data_frames) == 0


@pytest.mark.functional
def test_basic_etl_pipeline_invalid_json(tmp_path):
    """Test basic_etl_pipeline with invalid JSON file."""
    from basic_etl_pipeline import extract_local_json_files
    
    # Create a file with invalid JSON
    invalid_file = tmp_path / "invalid.json"
    invalid_file.write_text("{not: valid json}")
    
    data_frames = extract_local_json_files(str(tmp_path))
    # Should handle gracefully without crashing
    assert "invalid.json" not in data_frames or data_frames.get("invalid.json") is None


@pytest.mark.functional
def test_quickstart_example_structure():
    """Test that quickstart example has expected structure."""
    quickstart_path = examples_dir / "quickstart.py"
    assert quickstart_path.exists()
    
    # Read and verify it imports pipeline
    content = quickstart_path.read_text()
    assert "from serenade_flow import pipeline" in content
    assert "pipeline.configure" in content
    assert "pipeline.extract" in content


@pytest.mark.functional
def test_hello_world_example_structure():
    """Test that hello_world example has expected structure."""
    hello_world_path = examples_dir / "hello_world.py"
    assert hello_world_path.exists()
    
    # Read and verify it imports pipeline
    content = hello_world_path.read_text()
    assert "from serenade_flow import pipeline" in content


@pytest.mark.functional
def test_sports_odds_processing_example(sample_data_directory, tmp_path, monkeypatch):
    """Test that sports_odds_processing example works correctly."""
    from sports_odds_processing import (
        extract_sports_odds_from_local,
        transform_sports_odds_data,
        validate_sports_event_data,
        flatten_sports_event_record,
    )
    
    # Test extraction
    data_frames = extract_sports_odds_from_local(sample_data_directory)
    assert len(data_frames) > 0
    assert "test_data.json" in data_frames
    
    # Test validation
    with open(os.path.join(sample_data_directory, "test_data.json"), "r") as f:
        data = json.load(f)
        assert validate_sports_event_data(data[0]) is True
    
    # Test flattening
    flattened = flatten_sports_event_record(data[0])
    assert len(flattened) > 0
    assert "outcome_name" in flattened[0]
    assert "outcome_price" in flattened[0]
    
    # Test transformation
    transformed = transform_sports_odds_data(data_frames)
    assert len(transformed) > 0
    assert "test_data.json" in transformed
    
    # Verify transformation applied
    df = transformed["test_data.json"]
    assert "processed_at" in df.columns
    assert "source_file" in df.columns


@pytest.mark.functional
def test_sports_odds_processing_validation():
    """Test sports odds validation functions."""
    from sports_odds_processing import (
        validate_sports_event_data,
        _validate_bookmaker,
        _validate_market,
        _validate_outcome,
    )
    
    # Test valid data
    valid_data = {
        "id": "1",
        "sport_key": "basketball_nba",
        "sport_title": "NBA",
        "home_team": "Team A",
        "away_team": "Team B",
        "commence_time": "2025-02-04T00:10:00Z",
        "bookmakers": [
            {
                "key": "bookmaker1",
                "title": "Bookmaker 1",
                "markets": [
                    {
                        "key": "h2h",
                        "last_update": "2025-02-04T00:05:00Z",
                        "outcomes": [
                            {"name": "Team A", "price": 1.5},
                            {"name": "Team B", "price": 2.5},
                        ],
                    }
                ],
            }
        ],
    }
    assert validate_sports_event_data(valid_data) is True
    
    # Test invalid data (missing required field)
    invalid_data = {"id": "1", "sport_key": "basketball_nba"}
    assert validate_sports_event_data(invalid_data) is False
    
    # Test outcome validation
    assert _validate_outcome({"name": "Team A", "price": 1.5}) is True
    assert _validate_outcome({"name": "Team A"}) is False  # Missing price

