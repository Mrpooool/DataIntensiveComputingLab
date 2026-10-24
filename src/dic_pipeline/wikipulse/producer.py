"""At-least-once Wikimedia EventStreams to Kafka capture."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests
from kafka import KafkaProducer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
LOG = logging.getLogger(__name__)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def event_key(raw: bytes) -> tuple[bytes, str | None, int | None, bool]:
    """Use Wikimedia's stable meta.id, retaining malformed data with a hash key."""
    source_id: str | None = None
    event_time: int | None = None
    try:
        event = json.loads(raw)
    except (ValueError, TypeError):
        return hashlib.sha256(raw).hexdigest().encode("ascii"), None, None, False
    if isinstance(event, dict):
        meta = event.get("meta")
        if isinstance(meta, dict) and meta.get("id") is not None:
            source_id = str(meta["id"])
        timestamp = event.get("timestamp")
        if timestamp is not None:
            try:
                event_time = int(timestamp)
            except (TypeError, ValueError, OverflowError):
                event_time = None
    key = source_id or hashlib.sha256(raw).hexdigest()
    return key.encode("utf-8"), source_id, event_time, True


def sse_events(lines: Iterable[bytes]) -> Iterable[tuple[str | None, str | None, bytes]]:
    """Read SSE frames; a blank line dispatches all data lines in that frame."""
    data: list[bytes] = []
    has_data = False
    event_id = None
    event_type = None
    for line in lines:
        line = line.rstrip(b"\r\n")
        if not line:
            if has_data:
                yield event_id, event_type, b"\n".join(data)
            data, has_data, event_id, event_type = [], False, None, None
            continue
        if line.startswith(b":"):
            continue
        field, _, value = line.partition(b":")
        value = value[1:] if value.startswith(b" ") else value
        if field == b"data":
            data.append(value)
            has_data = True
        elif field == b"id":
            event_id = value.decode("utf-8", "replace")
        elif field == b"event":
            event_type = value.decode("utf-8", "replace")
    if has_data:
        yield event_id, event_type, b"\n".join(data)


def load_state(path: Path) -> str | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"state file must contain an object: {path}")
    value = payload.get("last_event_id")
    if value is not None and not isinstance(value, str):
        raise ValueError(f"state last_event_id must be a string: {path}")
    return value


def save_state(path: Path, event_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        staged.write_text(json.dumps({"last_event_id": event_id}), encoding="utf-8")
        for attempt in range(5):
            try:
                os.replace(staged, path)
                return
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.1 * (attempt + 1))
    finally:
        staged.unlink(missing_ok=True)


def capture(bootstrap: str, topic: str, state_path: Path, *,
            stream_url: str = STREAM_URL, stats_interval: int = 60,
            max_events: int | None = None, ack_every: int = 1) -> None:
    """Capture all project events; reconnect with the last Kafka-acked SSE ID.

    ``ack_every`` > 1 waits for Kafka acknowledgements in groups and saves the SSE ID
    only after the whole group is acknowledged, which a backfill needs for speed.
    """
    if not topic:
        raise ValueError("topic must be nonempty")
    if stats_interval < 0 or (max_events is not None and max_events < 0):
        raise ValueError("stats_interval and max_events must be nonnegative")
    if ack_every < 1:
        raise ValueError("ack_every must be positive")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(","), acks="all", retries=10,
        max_in_flight_requests_per_connection=1, linger_ms=20,
    )
    last_event_id = load_state(state_path)
    counters = {"published": 0, "bad_json": 0, "reconnects": 0,
                "first_source_event_unix": None, "last_source_event_unix": None}
    started = utc_now()
    last_report = time.monotonic()
    backoff = 1.0
    pending: list = []
    pending_id: str | None = None

    def acknowledge() -> None:
        nonlocal last_event_id, pending_id
        for future in pending:
            future.get(timeout=60)
        pending.clear()
        if pending_id:
            save_state(state_path, pending_id)
            last_event_id = pending_id
            pending_id = None

    try:
        while max_events is None or counters["published"] < max_events:
            headers = {"Accept": "text/event-stream", "User-Agent": "WikiPulse/1.0 (course project)"}
            if last_event_id:
                headers["Last-Event-ID"] = last_event_id
            try:
                with requests.get(stream_url, headers=headers, stream=True, timeout=(15, 90)) as response:
                    response.raise_for_status()
                    LOG.info("connected last_event_id=%s at=%s", last_event_id, utc_now())
                    backoff = 1.0
                    for sse_id, sse_type, raw in sse_events(response.iter_lines(chunk_size=65536)):
                        key, source_id, event_time, valid_json = event_key(raw)
                        if not valid_json:
                            counters["bad_json"] += 1
                        received_at = utc_now()
                        metadata = [
                            ("sse_id", (sse_id or "").encode("utf-8")),
                            ("sse_event_type", (sse_type or "").encode("utf-8")),
                            ("source_id", (source_id or "").encode("utf-8")),
                            ("source_event_unix", ("" if event_time is None else str(event_time)).encode("ascii")),
                            ("received_at_utc", received_at.encode("ascii")),
                        ]
                        pending.append(producer.send(topic, key=key, value=raw, headers=metadata))
                        if sse_id:
                            pending_id = sse_id
                        if len(pending) >= ack_every:
                            acknowledge()
                        counters["published"] += 1
                        if event_time is not None:
                            if counters["first_source_event_unix"] is None:
                                counters["first_source_event_unix"] = event_time
                            counters["last_source_event_unix"] = event_time
                        if time.monotonic() - last_report >= stats_interval:
                            LOG.info("stats %s", json.dumps({**counters, "started_at_utc": started,
                                "reported_at_utc": utc_now(), "last_sse_id": last_event_id}))
                            last_report = time.monotonic()
                        if max_events is not None and counters["published"] >= max_events:
                            break
                acknowledge()
            except (requests.RequestException, TimeoutError, OSError) as exc:
                acknowledge()
                LOG.warning("stream interrupted at=%s last_event_id=%s error=%s",
                            utc_now(), last_event_id, exc)
            if max_events is not None and counters["published"] >= max_events:
                break
            counters["reconnects"] += 1
            time.sleep(min(backoff, 60.0) + random.uniform(0, 0.5))
            backoff = min(backoff * 2, 60.0)
    finally:
        acknowledge()
        LOG.info("final stats %s", json.dumps({**counters, "started_at_utc": started,
            "stopped_at_utc": utc_now(), "last_sse_id": last_event_id}))
        producer.flush(timeout=60)
        producer.close()
