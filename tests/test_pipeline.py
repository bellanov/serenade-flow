"""Test Template."""

import json
from unittest.mock import patch

import pandas as pd
import pytest

from serenade_flow import pipeline
from serenade_flow.community.fantasyace_cloud_functions_plugin import (
    FantasyAceCloudFunctionsPlugin,
)
from serenade_flow.pipeline import extract_local_data, transform

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
    return {
        "data_source": "local",
        "data_source_path": sample_data_directory,
        "data_format": "json",
    }


@pytest.fixture
def remote_config():
    return {
        "data_source": "remote",
        "data_source_path": "http://remote-api-endpoint/data",
        "data_format": "json",
    }


# --- Helpers ---


def assert_valid_extraction(data):
    assert isinstance(data, dict)
    assert all(isinstance(df, pd.DataFrame) for df in data.values())
    for df in data.values():
        assert not df.empty


# --- Core Tests (deduplicated) ---


@pytest.mark.unit
def test_extract_local(sample_data_directory):
    pipeline.configure(
        {
            "data_source": "local",
            "data_source_path": sample_data_directory,
            "data_format": "json",
        }
    )
    data = pipeline.extract()
    assert len(data) > 0
    assert isinstance(data, dict)
    assert all(isinstance(df, pd.DataFrame) for df in data.values())
    assert "outcome_name" in data["Events_NBA.json"].columns
    assert "outcome_price" in data["Events_NBA.json"].columns
    assert "outcome_point" in data["Events_NBA.json"].columns


@pytest.mark.unit
def test_extract_remote():
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

    with patch("serenade_flow.pipeline.requests.get", return_value=MockResponse()):
        pipeline.configure(
            {
                "data_source": "remote",
                "data_source_path": "http://remote-api-endpoint/data",
                "data_format": "json",
            }
        )
        data = pipeline.extract()

    assert isinstance(data, dict)
    assert len(data) > 0
    assert all(isinstance(df, pd.DataFrame) for df in data.values())
    assert len(data["remote_data.json"]) == 2
    df = data["remote_data.json"]
    assert "bookmaker_key" in df.columns
    assert "market_key" in df.columns
    assert "outcome_name" in df.columns
    assert "outcome_price" in df.columns
    assert "outcome_point" in df.columns


@pytest.mark.unit
def test_load(sample_data_directory):
    pipeline.configure(
        {
            "data_source": "local",
            "data_source_path": sample_data_directory,
            "data_format": "json",
        }
    )
    data = pipeline.extract()
    assert pipeline.load(data, "output") == "Data loaded successfully in CSV format"


@pytest.mark.unit
def test_load_parquet(sample_data_directory):
    """Test loading data in Parquet format."""
    pipeline.configure(
        {
            "data_source": "local",
            "data_source_path": sample_data_directory,
            "data_format": "json",
        }
    )
    data = pipeline.extract()
    result = pipeline.load(data, "output", "parquet")
    assert result == "Data loaded successfully in PARQUET format"

    # Verify parquet file was created
    import os

    parquet_file = "output_Events_NBA.parquet"
    assert os.path.exists(parquet_file)

    # Verify we can read the parquet file back
    df = pd.read_parquet(parquet_file)
    assert not df.empty
    assert len(df) == 4  # Same as original data

    # Clean up
    os.remove(parquet_file)


@pytest.mark.unit
def test_load_invalid_format(sample_data_directory):
    """Test loading with invalid format parameter."""
    pipeline.configure(
        {
            "data_source": "local",
            "data_source_path": sample_data_directory,
            "data_format": "json",
        }
    )
    data = pipeline.extract()
    # Should default to CSV for invalid format
    result = pipeline.load(data, "output", "invalid_format")
    assert result == "Data loaded successfully in CSV format"


@pytest.mark.unit
def test_extract_local_data(sample_data_directory):
    data_frames = extract_local_data(sample_data_directory)
    assert "Events_NBA.json" in data_frames
    assert isinstance(data_frames["Events_NBA.json"], pd.DataFrame)
    assert len(data_frames["Events_NBA.json"]) == 4
    df = data_frames["Events_NBA.json"]
    assert "bookmaker_key" in df.columns
    assert "market_key" in df.columns
    assert "outcome_name" in df.columns
    assert "outcome_price" in df.columns
    assert "outcome_point" in df.columns


@pytest.mark.unit
def test_transform_data(sample_data_directory):
    data_frames = extract_local_data(sample_data_directory)
    transformed_data = transform(data_frames)
    assert transformed_data["Events_NBA.json"]["home_team"].iloc[0] == "Team A"
    assert pd.to_datetime(transformed_data["Events_NBA.json"]["commence_time"].iloc[0])
    assert pd.to_datetime(
        transformed_data["Events_NBA.json"]["market_last_update"].iloc[0]
    )
    assert pd.api.types.is_numeric_dtype(
        transformed_data["Events_NBA.json"]["outcome_point"]
    )


# --- Edge/Negative/Utility Tests (unchanged) ---


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
    assert any(
        "Error processing Malformed.json" in msg for msg in caplog.text.splitlines()
    )


@pytest.mark.unit
def test_extract_missing_fields(tmp_path):
    """Test extraction from a file with missing required fields."""
    data = [{"id": "1", "sport_key": "basketball_nba"}]  # Missing most required fields
    file = tmp_path / "MissingFields.json"
    file.write_text(json.dumps(data))
    data_frames = extract_local_data(str(tmp_path))
    # Should not raise, and should not include any valid records
    assert (
        "MissingFields.json" not in data_frames
        or data_frames["MissingFields.json"].empty
    )


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
            "bookmakers": "not_a_list",
        }
    ]
    file = tmp_path / "InvalidTypes.json"
    file.write_text(json.dumps(data))
    data_frames = extract_local_data(str(tmp_path))
    # Should not raise, and should not include any valid records
    assert (
        "InvalidTypes.json" not in data_frames or data_frames["InvalidTypes.json"].empty
    )


@pytest.mark.unit
def test_validate_outcome_negative():
    from serenade_flow.pipeline import _validate_outcome

    # Not a dict
    assert not _validate_outcome(["not", "a", "dict"])
    # Missing keys
    assert not _validate_outcome({"name": "A"})
    assert not _validate_outcome({"price": 1.5})
    # Empty dict
    assert not _validate_outcome({})


@pytest.mark.unit
def test_validate_market_negative():
    from serenade_flow.pipeline import _validate_market

    # Not a dict
    assert not _validate_market(["not", "a", "dict"])
    # Missing keys
    assert not _validate_market({"key": "h2h"})
    assert not _validate_market({"last_update": "2025-01-01"})
    # Outcomes not a list
    assert not _validate_market(
        {"key": "h2h", "last_update": "2025-01-01", "outcomes": "notalist"}
    )
    # Nested invalid outcome
    assert not _validate_market(
        {"key": "h2h", "last_update": "2025-01-01", "outcomes": [{"name": "A"}]}
    )


@pytest.mark.unit
def test_validate_bookmaker_negative():
    from serenade_flow.pipeline import _validate_bookmaker

    # Not a dict
    assert not _validate_bookmaker(["not", "a", "dict"])
    # Missing keys
    assert not _validate_bookmaker({"key": "bk1"})
    assert not _validate_bookmaker({"title": "Bookmaker 1"})
    # Markets not a list
    assert not _validate_bookmaker(
        {"key": "bk1", "title": "Bookmaker 1", "markets": "notalist"}
    )
    # Nested invalid market
    assert not _validate_bookmaker(
        {"key": "bk1", "title": "Bookmaker 1", "markets": [{"key": "h2h"}]}
    )


@pytest.mark.unit
def test_validate_data_negative():
    from serenade_flow.pipeline import validate_data

    # Not a dict
    assert not validate_data(["not", "a", "dict"])
    # Missing required fields
    assert not validate_data({"id": "1"})
    # Bookmakers not a list
    base = {
        "id": "1",
        "sport_key": "nba",
        "sport_title": "NBA",
        "home_team": "A",
        "away_team": "B",
        "commence_time": "2025",
        "bookmakers": "notalist",
    }
    assert not validate_data(base)
    # Bookmakers list with invalid bookmaker
    base["bookmakers"] = ["notadict"]
    assert not validate_data(base)
    # Bookmakers list with invalid nested structure
    base["bookmakers"] = [{"key": "bk1"}]
    assert not validate_data(base)
    # Exception path: pass object that raises in __getitem__

    class BadDict(dict):

        def __getitem__(self, key):
            raise Exception("fail")

    assert not validate_data(BadDict())


@pytest.mark.unit
def test_transform_datetime_error(caplog):
    from serenade_flow.pipeline import transform_datetime

    with caplog.at_level("ERROR"):
        with pytest.raises(Exception):
            transform_datetime("not-a-date")
    assert any("Error parsing datetime" in msg for msg in caplog.text.splitlines())


@pytest.mark.unit
def test_configure_updates_and_returns():
    from serenade_flow.pipeline import CONFIG, configure

    # Save original config
    orig = CONFIG.copy()
    new_conf = {"data_source": "test", "foo": "bar"}
    result = configure(new_conf)
    for k, v in new_conf.items():
        assert result[k] == v
        assert CONFIG[k] == v
    # Restore original config
    CONFIG.clear()
    CONFIG.update(orig)


@pytest.mark.unit
def test_flatten_record_edge_cases():
    from serenade_flow.pipeline import _flatten_record

    # Empty bookmakers
    record = {"bookmakers": []}
    assert _flatten_record(record) == []
    # Bookmaker with missing markets
    record = {"bookmakers": [{"key": "bk1", "title": "B1"}]}
    assert _flatten_record(record) == []
    # Market with missing outcomes
    record = {
        "bookmakers": [
            {
                "key": "bk1",
                "title": "B1",
                "markets": [{"key": "m1", "last_update": "now"}],
            }
        ]
    }
    assert _flatten_record(record) == []
    # Outcome with missing fields (should still include, but with None)
    record = {
        "id": "1",
        "sport_key": "nba",
        "sport_title": "NBA",
        "commence_time": "now",
        "home_team": "A",
        "away_team": "B",
        "bookmakers": [
            {
                "key": "bk1",
                "title": "B1",
                "markets": [{"key": "m1", "last_update": "now", "outcomes": [{}]}],
            }
        ],
    }
    rows = _flatten_record(record)
    assert len(rows) == 1
    assert rows[0]["outcome_name"] is None and rows[0]["outcome_price"] is None


@pytest.mark.unit
def test_transform_missing_columns(caplog):
    import pandas as pd

    from serenade_flow.pipeline import transform

    # DataFrame missing required columns
    df = pd.DataFrame({"foo": [1], "bar": [2]})
    data_frames = {"file.json": df}
    with caplog.at_level("WARNING"):
        result = transform(data_frames)
    assert "Missing columns" in caplog.text
    assert result == {}


@pytest.mark.unit
def test_transform_exception(monkeypatch, caplog):
    import pandas as pd

    from serenade_flow.pipeline import transform

    # DataFrame that raises in __getitem__

    class BadDF(pd.DataFrame):

        def __getitem__(self, key):
            raise Exception("fail")

    inner_dict = {
        "home_team": ["A"],
        "away_team": ["B"],
        "commence_time": ["now"],
        "market_last_update": ["now"],
        "outcome_point": [1],
    }
    data_frames = {"file.json": BadDF(inner_dict)}
    with caplog.at_level("ERROR"):
        result = transform(data_frames)
    assert "Error transforming" in caplog.text
    assert result == {}


@pytest.mark.unit
def test_load_to_csv_exception(monkeypatch, caplog):
    import pandas as pd

    from serenade_flow.pipeline import load

    # DataFrame that raises on to_csv

    class BadDF(pd.DataFrame):

        def to_csv(self, *a, **k):
            raise Exception("fail")

    data = {"file.json": BadDF({"a": [1]})}
    with caplog.at_level("ERROR"):
        result = load(data, "output")
    assert "Error loading data" in caplog.text
    assert result is None


@pytest.mark.unit
def test_process_json_data_all_invalid():
    from serenade_flow.pipeline import _process_json_data

    # List of all invalid records
    data = ["notadict", 123, None]
    df = _process_json_data(data, "file.json")
    import pandas as pd

    assert isinstance(df, pd.DataFrame)
    assert df.empty


@pytest.mark.unit
def test_configure_with_plugins():
    """Test configure function with plugins configuration."""
    from serenade_flow.pipeline import CONFIG, configure

    # Save original config
    orig = CONFIG.copy()

    # Test with plugins config
    config_with_plugins = {
        "data_source": "test",
        "plugins": {
            "test_plugin": {
                "module": "test.module",
                "class": "TestPlugin",
                "enabled": True,
            }
        },
    }

    result = configure(config_with_plugins)

    # Verify config was updated
    assert result["data_source"] == "test"
    assert "plugins" in result

    # Restore original config
    CONFIG.clear()
    CONFIG.update(orig)


@pytest.mark.unit
def test_process_json_data_dict_input():
    """Test _process_json_data with dict input."""
    from serenade_flow.pipeline import _process_json_data

    # Valid dict input
    data = {
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

    df = _process_json_data(data, "test.json")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2  # 2 outcomes
    assert "outcome_name" in df.columns


@pytest.mark.unit
def test_process_json_data_invalid_dict():
    """Test _process_json_data with invalid dict input."""
    from serenade_flow.pipeline import _process_json_data

    # Invalid dict (missing required fields)
    data = {"id": "1", "sport_key": "basketball_nba"}

    df = _process_json_data(data, "test.json")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


@pytest.mark.unit
def test_process_json_data_non_list_dict():
    """Test _process_json_data with non-list/dict input."""
    from serenade_flow.pipeline import _process_json_data

    # String input
    df = _process_json_data("not a list or dict", "test.json")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


@pytest.mark.unit
def test_fantasyace_plugin_unwraps_data_structure(monkeypatch):
    plugin = FantasyAceCloudFunctionsPlugin(
        base_url_sports="https://example/getsports",
        base_url_events="https://example/getevents",
        base_url_event_odds="https://example/geteventodds",
    )

    # Simulate endpoints returning {"data": [...]} and {"data": {...}}
    def fake_get_with_retry(url, params=None):
        if "getsports" in url:
            return {"data": [{"key": "basketball_nba", "title": "NBA"}]}
        if "getevents" in url:
            return {"data": [{"id": "evt1", "sport_key": "basketball_nba"}]}
        if "geteventodds" in url:
            return {
                "data": [
                    {
                        "id": "evt1",
                        "sport_key": "basketball_nba",
                        "sport_title": "NBA",
                        "home_team": "Team A",
                        "away_team": "Team B",
                        "commence_time": "2025-02-04T00:10:00Z",
                        "bookmakers": [
                            {
                                "key": "bk",
                                "title": "Book",
                                "markets": [
                                    {
                                        "key": "h2h",
                                        "last_update": "2025-02-04T00:05:00Z",
                                        "outcomes": [
                                            {"name": "Team A", "price": 1.1},
                                            {"name": "Team B", "price": 2.2},
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        return {}

    monkeypatch.setattr(plugin, "_get_with_retry", fake_get_with_retry)

    # Ensure list_sports and list_events unwrap data
    sports = plugin.list_sports()
    assert isinstance(sports, list) and sports and sports[0]["key"] == "basketball_nba"

    events = plugin.list_events("basketball_nba", limit=1)
    assert isinstance(events, list) and events and events[0]["id"] == "evt1"

    # Ensure get_event_odds returns a dict (first element of data list)
    odds = plugin.get_event_odds("evt1")
    assert isinstance(odds, dict) and odds.get("id") == "evt1"

    # Ensure pipeline can flatten odds data
    df = pipeline._process_json_data([odds], "fantasyace.json")
    import pandas as pd

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "outcome_name" in df.columns


@pytest.mark.unit
def test_fantasyace_plugin_configure():
    """Test FantasyAce plugin configuration."""
    plugin = FantasyAceCloudFunctionsPlugin()
    plugin.configure(
        base_url_sports="https://example/getsports",
        base_url_events="https://example/getevents",
        base_url_event_odds="https://example/geteventodds",
        max_retries=5,
        retry_delay=2.0,
        request_timeout_seconds=3.0,
    )
    assert plugin.base_url_sports == "https://example/getsports"
    assert plugin.base_url_events == "https://example/getevents"
    assert plugin.base_url_event_odds == "https://example/geteventodds"
    assert plugin.max_retries == 5
    assert plugin.retry_delay == 2.0
    assert plugin.request_timeout_seconds == 3.0


@pytest.mark.unit
def test_fantasyace_plugin_extract_end_to_end(monkeypatch):
    """Test FantasyAce plugin extract_events_and_odds end-to-end."""
    plugin = FantasyAceCloudFunctionsPlugin(
        base_url_sports="https://example/getsports",
        base_url_events="https://example/getevents",
        base_url_event_odds="https://example/geteventodds",
    )

    def fake_get_with_retry(url, params=None):
        if "getsports" in url:
            return {"data": [{"key": "basketball_nba", "title": "NBA"}]}
        if "getevents" in url:
            return {"data": [{"id": "evt1", "sport_key": "basketball_nba"}]}
        if "geteventodds" in url:
            return {
                "data": [
                    {
                        "id": "evt1",
                        "sport_key": "basketball_nba",
                        "sport_title": "NBA",
                        "home_team": "Team A",
                        "away_team": "Team B",
                        "commence_time": "2025-02-04T00:10:00Z",
                        "bookmakers": [
                            {
                                "key": "bk",
                                "title": "Book",
                                "markets": [
                                    {
                                        "key": "h2h",
                                        "last_update": "2025-02-04T00:05:00Z",
                                        "outcomes": [
                                            {"name": "Team A", "price": 1.1},
                                            {"name": "Team B", "price": 2.2},
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        return {}

    monkeypatch.setattr(plugin, "_get_with_retry", fake_get_with_retry)

    frames = plugin.extract_events_and_odds("basketball_nba", limit=1)
    assert "fantasyace.json" in frames
    df = frames["fantasyace.json"]
    import pandas as pd

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2  # 2 outcomes
    assert "outcome_name" in df.columns
    assert "outcome_price" in df.columns


@pytest.mark.unit
def test_fantasyace_plugin_empty_responses(monkeypatch):
    """Test FantasyAce plugin with empty API responses."""
    plugin = FantasyAceCloudFunctionsPlugin(
        base_url_sports="https://example/getsports",
        base_url_events="https://example/getevents",
        base_url_event_odds="https://example/geteventodds",
    )

    def fake_get_with_retry(url, params=None):
        if "getsports" in url:
            return {"data": []}
        if "getevents" in url:
            return {"data": []}
        return {}

    monkeypatch.setattr(plugin, "_get_with_retry", fake_get_with_retry)

    frames = plugin.extract_events_and_odds("basketball_nba", limit=1)
    assert frames == {}


@pytest.mark.unit
def test_process_json_data_none_input():
    """Test _process_json_data with None input."""
    df = pipeline._process_json_data(None, "test.json")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


@pytest.mark.unit
def test_validate_data_exception_path():
    """Test validate_data function exception handling."""
    from serenade_flow.pipeline import validate_data

    # Create an object that raises an exception when accessed
    class ExceptionDict:
        def __getitem__(self, key):
            raise Exception("Test exception")

    # This should return False due to exception
    assert not validate_data(ExceptionDict())
