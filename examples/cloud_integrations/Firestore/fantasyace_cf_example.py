"""
Example: Fetch and process FantasyAce event odds via Cloud Functions.

Usage:
  python3 examples/cloud_integrations/fantasyace_cf_example.py \
      --event-id a03f89d3aaadf737deb09be24986d5bf \
      --output-prefix fantasyace

Or to fetch by sport key and limit (may return empty if no events):
  python3 examples/cloud_integrations/fantasyace_cf_example.py \
      --sport-key americanfootball_nfl --limit 10 --output-prefix fantasyace
"""

import argparse

from serenade_flow import pipeline
from serenade_flow.community.fantasyace_cloud_functions_plugin import (
    FantasyAceCloudFunctionsPlugin,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sport-key", type=str, default=None)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--event-id", type=str, default=None)
    parser.add_argument("--output-prefix", type=str, default="fantasyace")
    parser.add_argument(
        "--format", type=str, choices=["csv", "parquet"], default="csv"
    )
    args = parser.parse_args()

    plugin = FantasyAceCloudFunctionsPlugin(
        base_url_sports="https://getsports-twqu2g763q-uc.a.run.app/",
        base_url_events="https://getevents-twqu2g763q-uc.a.run.app/",
        base_url_event_odds="https://geteventodds-twqu2g763q-uc.a.run.app/",
    )

    frames = {}
    if args.event_id:
        odds = plugin.get_event_odds(args.event_id)
        df = pipeline._process_json_data([odds], "fantasyace.json")
        if df is not None and not getattr(df, "empty", True):
            frames = {"fantasyace.json": df}
    elif args.sport_key:
        frames = plugin.extract_events_and_odds(args.sport_key, args.limit)

    if not frames:
        print("No data frames to load.")
        return

    transformed = pipeline.transform(frames)
    if not transformed:
        print("No transformed output.")
        return

    result = pipeline.load(transformed, args.output_prefix, args.format)
    print(result or "Load completed with no message")


if __name__ == "__main__":
    main()
 
