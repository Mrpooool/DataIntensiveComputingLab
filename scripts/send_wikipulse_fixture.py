"""Send one malformed raw record for archive smoke testing."""

import argparse
import hashlib
import json
from datetime import datetime, timezone

from kafka import KafkaProducer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="wikipulse.raw.live")
    args = parser.parse_args()
    raw = b"{wikipulse-malformed-smoke"
    key = hashlib.sha256(raw).hexdigest().encode("ascii")
    producer = KafkaProducer(bootstrap_servers=args.bootstrap.split(","), acks="all")
    try:
        metadata = producer.send(args.topic, key=key, value=raw, headers=[
            ("sse_id", b""), ("source_id", b""),
            ("received_at_utc", datetime.now(timezone.utc).isoformat().encode("ascii")),
        ]).get(timeout=60)
    finally:
        producer.close()
    print(json.dumps({"topic": metadata.topic, "partition": metadata.partition,
                      "offset": metadata.offset, "raw_value": raw.decode("ascii")},
                     sort_keys=True))


if __name__ == "__main__":
    main()
