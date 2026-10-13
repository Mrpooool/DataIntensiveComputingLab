"""Capture Wikimedia recentchange SSE to Kafka."""

import argparse
import logging
from pathlib import Path

from dic_pipeline.wikipulse.producer import STREAM_URL, capture

RAW_TOPIC = "wikipulse.raw.live"
STATS_INTERVAL_SECONDS = 60


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--state-file", type=Path, default=Path("data/wikipulse/producer_A.json"))
    parser.add_argument("--max-events", type=int)
    args = parser.parse_args()
    if args.max_events is not None and args.max_events < 0:
        parser.error("max-events must be nonnegative")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    capture(args.bootstrap, RAW_TOPIC, args.state_file, stream_url=STREAM_URL,
            stats_interval=STATS_INTERVAL_SECONDS, max_events=args.max_events)


if __name__ == "__main__":
    main()
