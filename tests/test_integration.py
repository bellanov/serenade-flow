import os
import pandas as pd
import pytest
from dotenv import load_dotenv
from serenade_flow import pipeline
from unittest.mock import patch
import json

# Load environment variables from .env file
load_dotenv()

@pytest.mark.integration
def test_integration_events_and_odds():
    """Integration test for extracting events and their odds."""
    sport = "basketball_nba"
    # Mock remote response data
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

    class MockResponse:
        def __init__(self):
            self.status_code = 200
        def json(self):
            return mock_response_data

    with patch('requests.get', return_value=MockResponse()):
        pipeline.configure({
            "data_source": "remote",
            "data_source_path": "http://mock-api-endpoint/data",
            "data_format": "json"
        })
        # Extract all data
        all_data = pipeline.extract()
        # Find the first event for the given sport
        events_df = None
        for key, df in all_data.items():
            if 'sport_key' in df.columns and sport in df['sport_key'].unique():
                events_df = df[df['sport_key'] == sport]
                break
        assert events_df is not None, "No events found for the given sport"
        assert not events_df.empty, "DataFrame is empty"
        # Extract odds for the first event
        event_id = events_df['id'].iloc[0]
        odds_data = pipeline.extract_event_odds(sport, event_id)
        # Assertions for odds data
        assert isinstance(odds_data, pd.DataFrame), "Output is not a DataFrame"
        assert not odds_data.empty, "DataFrame is empty"

if __name__ == "__main__":
    pytest.main()