"""Consumes the clickstream topic and writes newline-delimited JSON to a
partitioned bronze layer -- the local stand-in for a GCS/MinIO raw landing bucket.

Design choices worth explaining in an interview:
  - Partition layout `dt=YYYY-MM-DD/hour=HH/` mirrors how you'd actually lay out
    a bucket for a downstream tool (Airflow, dbt external tables, Hive-style
    partition discovery) to pick up new data incrementally.
  - Micro-batching (flush every N messages or M seconds, whichever first) trades
    a little latency for far fewer, larger files -- avoids the "millions of
    tiny files" problem that kills object-store-backed query performance.
  - Offsets are committed manually, only *after* a batch is durably written to
    disk. That gives at-least-once delivery: a crash between flush and commit
    means we reprocess a few messages on restart (and would re-append them),
    never silently lose them. Exactly-once would need idempotent writes keyed
    by event_id, which is a natural follow-up conversation.

Run with:  python -m consumer.kafka_to_bronze   (from the repo root, with consumer/.venv active)
"""

from __future__ import annotations

import logging
import os
import signal
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer

from consumer.config import ConsumerConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("consumer.kafka_to_bronze")

_shutdown_requested = False


def _request_shutdown(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True


def _partition_dir(root: str, moment: datetime) -> str:
    return os.path.join(root, f"dt={moment.strftime('%Y-%m-%d')}", f"hour={moment.strftime('%H')}")


def _flush(buffer: list[str], root: str) -> None:
    if not buffer:
        return
    moment = datetime.now(timezone.utc)
    out_dir = _partition_dir(root, moment)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"events-{uuid.uuid4()}.jsonl")
    with open(out_path, "w") as f:
        f.write("\n".join(buffer) + "\n")
    logger.info("Flushed %d events to %s", len(buffer), out_path)


def run() -> None:
    # See simulator/producer.py for why this explicit handler is needed instead
    # of a plain try/except KeyboardInterrupt: confluent-kafka's C library
    # interferes with Python's default SIGINT handling.
    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    config = ConsumerConfig.from_env()
    consumer = Consumer(
        {
            "bootstrap.servers": config.broker,
            "group.id": config.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # we commit manually, only after a durable flush
        }
    )
    consumer.subscribe([config.topic_name])

    buffer: list[str] = []
    last_flush_time = time.monotonic()

    logger.info(
        "Consuming topic '%s' on %s as group '%s', writing to %s",
        config.topic_name,
        config.broker,
        config.group_id,
        config.bronze_output_path,
    )

    while not _shutdown_requested:
        msg = consumer.poll(timeout=1.0)

        if msg is not None and msg.error() is None:
            buffer.append(msg.value().decode("utf-8"))

        should_flush = len(buffer) >= config.batch_size or (
            buffer and time.monotonic() - last_flush_time >= config.flush_interval_seconds
        )
        if should_flush:
            _flush(buffer, config.bronze_output_path)
            consumer.commit(asynchronous=False)
            buffer = []
            last_flush_time = time.monotonic()

    logger.info("Shutting down, flushing remaining buffer...")
    _flush(buffer, config.bronze_output_path)
    if buffer:
        consumer.commit(asynchronous=False)
    consumer.close()
    logger.info("Consumer stopped.")


if __name__ == "__main__":
    run()
