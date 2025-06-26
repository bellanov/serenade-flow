"""Test Template."""

import pytest
import pandas as pd
import json
from serenade_flow.pipeline import extract_local_data, transform, load
from unittest.mock import patch
import os

from serenade_flow import pipeline


@pytest.mark.unit
def test_extract_local(sample_data_directory):
    """Test Local Extraction."""
    # Configure the pipeline to use local data
    pipeline.configure({
        "data_source": "local",
        "data_source_path": sample_data_directory,
        "data_format": "json"
    })

    # Extract data
    data = pipeline.extract()

    # Check that the data contains the expected number of records
    assert len(data) > 0  # Ensure that data is extracted
    assert isinstance(data, dict)  # Ensure the data is a dictionary
    # Ensure all values are DataFrames
    assert all(isinstance(df, pd.DataFrame) for df in data.values())
    assert 'outcome_name' in data["Events_NBA.json"].columns
    assert 'outcome_price' in data["Events_NBA.json"].columns
    assert 'outcome_point' in data["Events_NBA.json"].columns


@pytest.mark.unit
def test_extract_remote():
    """Test Remote Extraction."""
    # Skip this test if not running in a GCP environment
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        pytest.skip("Skipping remote extraction test: not running in a GCP environment.")
    # Mock data that we expect from the remote API
    mock_response_data = [
        {
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
                                {"name": "Team B", "price": 2.5}
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "id": "2",
            "sport_key": "basketball_nba",
            "sport_title": "NBA",
            "home_team": "Team C",
            "away_team": "Team D",
            "commence_time": "2025-02-04T00:20:00Z",
            "bookmakers": [
                {
                    "key": "bookmaker2",
                    "title": "Bookmaker 2",
                    "markets": [
                        {
                            "key": "h2h",
                            "last_update": "2025-02-04T00:15:00Z",
                            "outcomes": [
                                {"name": "Team C", "price": 1.2},
                                {"name": "Team D", "price": 3.0}
                            ]
                        }
                    ]
                }
            ]
        }
    ]

    # Create a mock response object
    class MockResponse:
        def __init__(self):
            self.status_code = 200

        def json(self):
            return mock_response_data

    # Mock the requests.get call
    with patch('requests.get', return_value=MockResponse()):
        pipeline.configure({
            "data_source": "remote",
            "data_source_path": "http://remote-api-endpoint/data",
            "data_format": "json"
        })

        data = pipeline.extract()
        assert isinstance(data, dict)
        assert len(data) > 0
        assert all(isinstance(df, pd.DataFrame) for df in data.values())
        # Verify the mock data was processed correctly
        # Expected 2 records * 2 outcomes per record = 4 flattened records
        assert len(data["remote_data.json"]) == 4
        df = data["remote_data.json"]
        assert 'bookmaker_key' in df.columns
        assert 'market_key' in df.columns
        assert 'outcome_name' in df.columns
        assert 'outcome_price' in df.columns
        assert 'outcome_point' in df.columns


@pytest.mark.unit
def test_load(sample_data_directory):
    """Test Loading Data."""
    # Configure the pipeline to use local data
    pipeline.configure({
        "data_source": "local",
        "data_source_path": sample_data_directory,
        "data_format": "json"
    })

    # Extract data
    data = pipeline.extract()

    # Print the data to verify its structure
    print(data)  # Add this line to inspect the data structure

    # Load the data and check for success message
    result = pipeline.load(data, "output")
    assert result.endswith('.csv')
    assert os.path.exists(result)


@pytest.fixture
def sample_data_directory(tmp_path):
    """Fixture to create a temporary directory with sample JSON files."""
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
                                {"name": "team b", "price": 2.5}
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "id": "2",
            "sport_key": "basketball_nba",
            "sport_title": "NBA",
            "home_team": "team c",
            "away_team": "team d",
            "commence_time": "2025-02-04T00:20:00Z",
            "bookmakers": [
                {
                    "key": "bookmaker2",
                    "title": "Bookmaker 2",
                    "markets": [
                        {
                            "key": "h2h",
                            "last_update": "2025-02-04T00:15:00Z",
                            "outcomes": [
                                {"name": "team c", "price": 1.2},
                                {"name": "team d", "price": 3.0}
                            ]
                        }
                    ]
                }
            ]
        }
    ]
    json_file = tmp_path / "Events_NBA.json"
    json_file.write_text(json.dumps(data))
    return str(tmp_path)


@pytest.mark.unit
def test_extract_local_data(sample_data_directory):
    """Test the extraction of data from JSON files."""
    data_frames = extract_local_data(sample_data_directory)
    assert "Events_NBA.json" in data_frames
    assert isinstance(data_frames["Events_NBA.json"], pd.DataFrame)
    # Each original record has 2 outcomes, so 2*2=4 flattened records
    assert len(data_frames["Events_NBA.json"]) == 4
    df = data_frames["Events_NBA.json"]
    assert 'bookmaker_key' in df.columns
    assert 'market_key' in df.columns
    assert 'outcome_name' in df.columns
    assert 'outcome_price' in df.columns
    assert 'outcome_point' in df.columns


@pytest.mark.unit
def test_transform_data(sample_data_directory):
    """Test the transformation of extracted data."""
    data_frames = extract_local_data(sample_data_directory)
    transformed_data = transform(data_frames)
    # Check capitalization
    assert transformed_data["Events_NBA.json"]["home_team"].iloc[0] == "Team A"
    # Check datetime conversion
    assert pd.to_datetime(transformed_data["Events_NBA.json"]["commence_time"].iloc[0])
    # Check market_last_update datetime conversion
    assert pd.to_datetime(transformed_data["Events_NBA.json"]["market_last_update"].iloc[0])
    # Check outcome_point is numeric
    assert pd.api.types.is_numeric_dtype(transformed_data["Events_NBA.json"]["outcome_point"])


@pytest.mark.unit
def test_load_data(sample_data_directory, tmp_path):
    """Test the loading of transformed data into CSV files."""
    data_frames = extract_local_data(sample_data_directory)
    transformed_data = transform(data_frames)
    result = load(transformed_data, str(tmp_path / "processed_data"))
    # Check if a CSV file with the correct prefix was created
    assert result.endswith('.csv')
    assert os.path.exists(result)


@pytest.mark.unit
def test_remote_load():
    """Test Loading Remote Data."""
    # Skip this test if not running in a GCP environment
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        pytest.skip("Skipping remote load test: not running in a GCP environment.")
    # Similar mock setup as above
    mock_response_data = [
        {
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
                                {"name": "Team B", "price": 2.5}
                            ]
                        }
                    ]
                }
            ]
        }
    ]

    class MockResponse:
        def __init__(self):
            self.status_code = 200

        def json(self):
            return mock_response_data

    with patch('requests.get', return_value=MockResponse()):
        pipeline.configure({
            "data_source": "remote",
            "data_source_path": "http://remote-api-endpoint/data",
            "data_format": "json"
        })

        data = pipeline.extract()
        assert isinstance(data, dict)
        assert len(data) > 0
        assert all(isinstance(df, pd.DataFrame) for df in data.values())


@pytest.mark.unit
def test_extract_empty_file(tmp_path):
    """Test extraction from an empty JSON file."""
    empty_file = tmp_path / "Empty.json"
    empty_file.write_text("")
    data_frames = extract_local_data(str(tmp_path))
    # Should not raise, and should not include the empty file
    assert "Empty.json" not in data_frames or data_frames["Empty.json"].empty


@pytest.mark.unit
def test_extract_malformed_json(tmp_path, caplog):
    """Test extraction from a malformed JSON file."""
    malformed_file = tmp_path / "Malformed.json"
    malformed_file.write_text("{not: valid json}")
    with caplog.at_level("ERROR"):
        data_frames = extract_local_data(str(tmp_path))
    # Should not raise, and should not include the malformed file
    assert "Malformed.json" not in data_frames or data_frames["Malformed.json"].empty
    assert any("Error processing Malformed.json" in msg for msg in caplog.text.splitlines())


@pytest.mark.unit
def test_extract_missing_fields(tmp_path):
    """Test extraction from a file with missing required fields."""
    data = [
        {"id": "1", "sport_key": "basketball_nba"}  # Missing most required fields
    ]
    file = tmp_path / "MissingFields.json"
    file.write_text(json.dumps(data))
    data_frames = extract_local_data(str(tmp_path))
    # Should not raise, and should not include any valid records
    assert "MissingFields.json" not in data_frames or data_frames["MissingFields.json"].empty


@pytest.mark.unit
def test_extract_invalid_types(tmp_path):
    """Test extraction from a file with invalid data types."""
    data = [
        {
            "id": "1",
            "sport_key": "basketball_nba",
            "sport_title": "NBA",
            "home_team": "Team A",
            "away_team": "Team B",
            "commence_time": "2025-02-04T00:10:00Z",
            "bookmakers": "not_a_list"
        }
    ]
    file = tmp_path / "InvalidTypes.json"
    file.write_text(json.dumps(data))
    data_frames = extract_local_data(str(tmp_path))
    # Should not raise, and should not include any valid records
    assert "InvalidTypes.json" not in data_frames or data_frames["InvalidTypes.json"].empty
