"""Test Template."""

import pytest
import pandas as pd
import os
import json
from serenade_flow.pipeline import extract_local_data, transform, load
from unittest.mock import patch

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
    assert all(isinstance(df, pd.DataFrame) for df in data.values())  # Ensure all values are DataFrames
    assert 'outcome_name' in data["Events_NBA.json"].columns
    assert 'outcome_price' in data["Events_NBA.json"].columns
    assert 'outcome_point' in data["Events_NBA.json"].columns

@pytest.mark.unit
def test_extract_remote():
    """Test Remote Extraction."""
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
        assert len(data["remote_data.json"]) == 4 # Expected 2 records * 2 outcomes per record = 4 flattened records
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
    assert pipeline.load(data, "output") == "Data loaded successfully"  # Check for success message

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
    assert len(data_frames["Events_NBA.json"]) == 4  # Each original record has 2 outcomes, so 2*2=4 flattened records
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
    assert transformed_data["Events_NBA.json"]["home_team"].iloc[0] == "Team A"  # Check capitalization
    assert pd.to_datetime(transformed_data["Events_NBA.json"]["commence_time"].iloc[0])  # Check datetime conversion
    assert pd.to_datetime(transformed_data["Events_NBA.json"]["market_last_update"].iloc[0])  # Check market_last_update datetime conversion
    assert pd.api.types.is_numeric_dtype(transformed_data["Events_NBA.json"]["outcome_point"])  # Check outcome_point is numeric


@pytest.mark.unit
def test_load_data(sample_data_directory, tmp_path):
    """Test the loading of transformed data into CSV files."""
    data_frames = extract_local_data(sample_data_directory)
    transformed_data = transform(data_frames)
    load(transformed_data, str(tmp_path / "processed_data"))
    assert (tmp_path / "processed_data_Events_NBA.csv").exists()  # Check if file was created


@pytest.mark.unit
def test_remote_load():
    """Test Loading Remote Data."""
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
        assert pipeline.load(data, "output") == "Data loaded successfully"
