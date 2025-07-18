"""Test Template."""

import pytest

import pandas as pd

import json

from serenade_flow import pipeline

from serenade_flow.quality.assessor import DataQualityAssessor


# --- Fixtures ---


@pytest.fixture
def sample_data_directory(tmp_path):
    """Create a temporary directory with sample NBA event JSON files."""

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
                                {"name": "team d", "price": 3.0},
                            ],
                        }
                    ],
                }
            ],
        },
    ]

    json_file = tmp_path / "Events_NBA.json"

    json_file.write_text(json.dumps(data))

    return str(tmp_path)


@pytest.fixture
def local_config(sample_data_directory):
    """Pipeline config for local data extraction."""

    return {
        "data_source": "local",
        "data_source_path": sample_data_directory,
        "data_format": "json",
    }


@pytest.fixture
def remote_config():
    """Pipeline config for remote data extraction."""

    return {
        "data_source": "remote",
        "data_source_path": "http://remote-api-endpoint/data",
        "data_format": "json",
    }


@pytest.fixture
def gcs_plugin():
    """Load and configure the GCSDataExtractorPlugin for plugin tests."""

    config = {
        "plugins": {
            "gcs_data_extractor": {
                "module": "serenade_flow.community.gcs_data_extractor_plugin",
                "class": "GCSDataExtractorPlugin",
                "enabled": True,
            }
        }
    }

    pipeline.configure(config)

    plugin = pipeline.PLUGIN_REGISTRY.get("gcs_data_extractor")

    plugin.configure(
        bucket_url="https://storage.googleapis.com/odds-data-samples-4vuoq93m/"
    )

    return plugin


# --- Helpers ---


def assert_valid_extraction(data):
    """Assert that extracted data is a non-empty dict of DataFrames."""

    assert isinstance(data, dict)

    assert all(isinstance(df, pd.DataFrame) for df in data.values())

    for df in data.values():

        assert not df.empty


def assert_quality_report(report):
    """Assert that a quality report contains all required keys."""

    assert "score" in report

    assert "missing_values" in report

    assert "schema_validation" in report

    assert "duplicates" in report


# --- Tests ---


def test_extract_local(local_config):
    """Test local data extraction and structure."""

    pipeline.configure(local_config)

    data = pipeline.extract()

    assert_valid_extraction(data)


def test_extract_remote(remote_config, monkeypatch):
    """Test remote data extraction and structure with mocked API."""

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
                                {"name": "Team B", "price": 2.5},
                            ],
                        }
                    ],
                }
            ],
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
                                {"name": "Team D", "price": 3.0},
                            ],
                        }
                    ],
                }
            ],
        },
    ]

    class MockResponse:

        def __init__(self):

            self.status_code = 200

        def json(self):

            return mock_response_data

    monkeypatch.setattr("requests.get", lambda url: MockResponse())

    pipeline.configure(remote_config)

    data = pipeline.extract()

    assert_valid_extraction(data)


def test_load(local_config):
    """Test loading extracted data to CSV files."""

    pipeline.configure(local_config)

    data = pipeline.extract()

    assert pipeline.load(data, "output") == "Data loaded successfully"


def test_extract_local_data(sample_data_directory):
    """Test extraction from local JSON files using extract_local_data."""

    data_frames = pipeline.extract_local_data(sample_data_directory)

    assert "Events_NBA.json" in data_frames

    assert isinstance(data_frames["Events_NBA.json"], pd.DataFrame)

    assert len(data_frames["Events_NBA.json"]) == 4

    df = data_frames["Events_NBA.json"]

    assert "bookmaker_key" in df.columns

    assert "market_key" in df.columns

    assert "outcome_name" in df.columns

    assert "outcome_price" in df.columns

    assert "outcome_point" in df.columns


def test_transform_data(sample_data_directory):
    """Test transformation logic on extracted data."""

    data_frames = pipeline.extract_local_data(sample_data_directory)

    transformed_data = pipeline.transform(data_frames)

    assert transformed_data["Events_NBA.json"]["home_team"].iloc[0] == "Team A"

    assert pd.to_datetime(transformed_data["Events_NBA.json"]["commence_time"].iloc[0])

    assert pd.to_datetime(
        transformed_data["Events_NBA.json"]["market_last_update"].iloc[0]
    )

    assert pd.api.types.is_numeric_dtype(
        transformed_data["Events_NBA.json"]["outcome_point"]
    )


def test_load_data(sample_data_directory, tmp_path):
    """Test loading transformed data to CSV files."""

    data_frames = pipeline.extract_local_data(sample_data_directory)

    transformed_data = pipeline.transform(data_frames)

    pipeline.load(transformed_data, str(tmp_path / "processed_data"))

    assert (tmp_path / "processed_data_Events_NBA.csv").exists()


def test_remote_load(remote_config, monkeypatch):
    """Test loading remote data with mocked API."""

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
                                {"name": "Team B", "price": 2.5},
                            ],
                        }
                    ],
                }
            ],
        }
    ]

    class MockResponse:

        def __init__(self):

            self.status_code = 200

        def json(self):

            return mock_response_data

    monkeypatch.setattr("requests.get", lambda url: MockResponse())

    pipeline.configure(remote_config)

    data = pipeline.extract()

    assert_valid_extraction(data)


def test_extract_empty_file(tmp_path):
    """Test extraction from an empty JSON file."""

    empty_file = tmp_path / "Empty.json"

    empty_file.write_text("")

    data_frames = pipeline.extract_local_data(str(tmp_path))

    assert "Empty.json" not in data_frames or data_frames["Empty.json"].empty


def test_extract_malformed_json(tmp_path, caplog):
    """Test extraction from a malformed JSON file."""

    malformed_file = tmp_path / "Malformed.json"

    malformed_file.write_text("{not: valid json}")

    with caplog.at_level("ERROR"):

        data_frames = pipeline.extract_local_data(str(tmp_path))

    assert "Malformed.json" not in data_frames or data_frames["Malformed.json"].empty

    assert any(
        "Error processing Malformed.json" in msg for msg in caplog.text.splitlines()
    )


def test_extract_missing_fields(tmp_path):
    """Test extraction from a file with missing required fields."""

    data = [{"id": "1", "sport_key": "basketball_nba"}]

    file = tmp_path / "MissingFields.json"

    file.write_text(json.dumps(data))

    data_frames = pipeline.extract_local_data(str(tmp_path))

    assert "MissingFields.json" not in data_frames or data_frames["MissingFields.json"].empty


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
            "bookmakers": "not_a_list",
        }
    ]

    file = tmp_path / "InvalidTypes.json"

    file.write_text(json.dumps(data))

    data_frames = pipeline.extract_local_data(str(tmp_path))

    assert (
        "InvalidTypes.json" not in data_frames or data_frames["InvalidTypes.json"].empty
    )


def test_plugin_registry_loading(tmp_path, monkeypatch):
    """Test that plugins are loaded and accessible via PLUGIN_REGISTRY."""

    from serenade_flow import pipeline

    plugin_code = """

class DummyPlugin:

    def __init__(self):

        self.hello = "world"

"""

    plugin_dir = tmp_path / "serenade_flow_dummy"

    plugin_dir.mkdir()

    plugin_file = plugin_dir / "dummy_plugin.py"

    plugin_file.write_text(plugin_code)

    import sys

    sys.path.insert(0, str(tmp_path))

    config = {
        "plugins": {
            "dummy": {
                "module": "serenade_flow_dummy.dummy_plugin",
                "class": "DummyPlugin",
                "enabled": True,
            }
        }
    }

    pipeline.configure(config)

    assert pipeline.PLUGIN_REGISTRY is not None

    plugin = pipeline.PLUGIN_REGISTRY.get("dummy")

    assert plugin is not None

    assert getattr(plugin, "hello", None) == "world"

    sys.path.pop(0)


def test_community_gcs_data_extractor_plugin(gcs_plugin, monkeypatch):
    """Test loading and using the GCSDataExtractorPlugin community plugin."""
    def mock_extract():
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        return {"file.json": df}
    monkeypatch.setattr(pipeline, "extract", mock_extract)
    result = gcs_plugin.extract_with_retry("odds/american/event_008740fcf1af65b0cc9e79.json")
    assert result["success"] is True
    assert result["file_path"] == "odds/american/event_008740fcf1af65b0cc9e79.json"
    assert result["record_count"] == 2


def test_data_quality_assessor_missing_and_duplicates():
    """Test missing value and duplicate detection in DataQualityAssessor."""
    df = pd.DataFrame({
        'a': [1, 2, None, 2],
        'b': [None, 2, 3, 2],
        'c': [1, 2, 3, 2]
    })
    data = {'file1': df}
    assessor = DataQualityAssessor()
    result = assessor.assess(data)
    assert result['missing_values']['file1']['total_missing'] == 2
    assert result['duplicates']['file1'] == [3]
    assert result['score'] < 100


def test_data_quality_assessor_schema_validation():
    """Test schema validation in DataQualityAssessor."""
    df = pd.DataFrame({
        'a': [1, 2],
        'b': [3, 4]
    })
    data = {'file1': df}
    schema = {'a': df['a'].dtype, 'b': df['b'].dtype}
    assessor = DataQualityAssessor()
    result = assessor.assess(data, schema)
    assert result['schema_validation']['file1'] is True
    schema2 = {'a': df['a'].dtype, 'b': df['b'].dtype, 'c': 'int64'}
    result2 = assessor.assess(data, schema2)
    assert result2['schema_validation']['file1'] is False
