import time
from typing import Any, Dict, List, Optional

import requests

from serenade_flow import pipeline


class FantasyAceCloudFunctionsPlugin:
    """
    Plugin for extracting FantasyAce sports, events, and event odds from
    Google Cloud Functions endpoints and normalizing them for the pipeline.
    """

    def __init__(
        self,
        base_url_sports: Optional[str] = None,
        base_url_events: Optional[str] = None,
        base_url_event_odds: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        request_timeout_seconds: float = 10.0,
    ):
        self.base_url_sports = base_url_sports
        self.base_url_events = base_url_events
        self.base_url_event_odds = base_url_event_odds
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.request_timeout_seconds = request_timeout_seconds

    def configure(
        self,
        base_url_sports: Optional[str] = None,
        base_url_events: Optional[str] = None,
        base_url_event_odds: Optional[str] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
        request_timeout_seconds: Optional[float] = None,
    ):
        if base_url_sports is not None:
            self.base_url_sports = base_url_sports
        if base_url_events is not None:
            self.base_url_events = base_url_events
        if base_url_event_odds is not None:
            self.base_url_event_odds = base_url_event_odds
        if max_retries is not None:
            self.max_retries = max_retries
        if retry_delay is not None:
            self.retry_delay = retry_delay
        if request_timeout_seconds is not None:
            self.request_timeout_seconds = request_timeout_seconds

    def _get_with_retry(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    url, params=params or {}, timeout=self.request_timeout_seconds
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise e
        raise last_error if last_error else RuntimeError("Unknown HTTP error")

    def list_sports(self) -> List[Dict[str, Any]]:
        if not self.base_url_sports:
            raise ValueError("base_url_sports is not configured")
        data = self._get_with_retry(self.base_url_sports)
        if isinstance(data, dict):
            if "sports" in data:
                return data["sports"]
            if "data" in data and isinstance(data["data"], list):
                return data["data"]
        if isinstance(data, list):
            return data
        return []

    def list_events(self, sport_key: str, limit: int = 50) -> List[Dict[str, Any]]:
        if not self.base_url_events:
            raise ValueError("base_url_events is not configured")
        params = {"sportKey": sport_key, "limit": limit}
        data = self._get_with_retry(self.base_url_events, params=params)
        if isinstance(data, dict):
            if "events" in data:
                return data["events"]
            if "data" in data and isinstance(data["data"], list):
                return data["data"]
        if isinstance(data, list):
            return data
        return []

    def get_event_odds(self, event_id: str) -> Dict[str, Any]:
        if not self.base_url_event_odds:
            raise ValueError("base_url_event_odds is not configured")
        params = {"eventId": event_id}
        data = self._get_with_retry(self.base_url_event_odds, params=params)
        # Some endpoints wrap results in {"data": [...]} structure
        if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
            return data["data"][0] if data["data"] else {}
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict):
            return data
        return {}

    def _normalize_event(
        self,
        event_stub: Dict[str, Any],
        odds: Dict[str, Any],
        sports_map: Dict[str, str],
        sport_key: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        # Attempt to extract fields from odds response first, fall back to event stub
        event_id = (
            odds.get("id")
            or event_stub.get("id")
            or event_stub.get("event_id")
            or odds.get("event_id")
        )
        if not event_id:
            return None

        effective_sport_key = (
            odds.get("sport_key") or event_stub.get("sport_key") or sport_key
        )
        commence_time = odds.get("commence_time") or event_stub.get("commence_time")
        home_team = odds.get("home_team") or event_stub.get("home_team")
        away_team = odds.get("away_team") or event_stub.get("away_team")
        bookmakers = odds.get("bookmakers") or []

        # sport title lookup from sports_map
        sport_title = odds.get("sport_title") or sports_map.get(
            effective_sport_key or "", None
        )

        normalized = {
            "id": event_id,
            "sport_key": effective_sport_key,
            "sport_title": sport_title,
            "commence_time": commence_time,
            "home_team": home_team,
            "away_team": away_team,
            "bookmakers": bookmakers,
        }

        return normalized

    def extract_events_and_odds(
        self, sport_key: str, limit: int = 50
    ) -> Dict[str, "pipeline.pd.DataFrame"]:
        # Build sport key -> title map for enrichment
        sports = self.list_sports()
        sports_map: Dict[str, str] = {}
        for s in sports:
            key = s.get("key") or s.get("sport_key")
            title = s.get("title") or s.get("sport_title")
            if key and title:
                sports_map[key] = title

        events = self.list_events(sport_key=sport_key, limit=limit)

        normalized_events: List[Dict[str, Any]] = []
        for e in events:
            ev_id = e.get("id") or e.get("event_id")
            if not ev_id:
                continue
            odds = self.get_event_odds(event_id=ev_id)
            normalized = self._normalize_event(
                event_stub=e, odds=odds, sports_map=sports_map, sport_key=sport_key
            )
            if normalized is None:
                continue
            # Validate against pipeline rules; drop if invalid
            if pipeline.validate_data(normalized):
                normalized_events.append(normalized)

        if not normalized_events:
            return {}

        # Reuse pipeline processing to flatten records
        df = pipeline._process_json_data(normalized_events, "fantasyace.json")
        if df is None or getattr(df, "empty", True):
            return {}
        return {"fantasyace.json": df}
