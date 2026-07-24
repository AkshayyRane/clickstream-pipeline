"""Entrypoint: continuously generate clickstream events and produce them to Redpanda.

Run with:  python -m simulator.producer   (from the repo root, with simulator/.venv active)
"""

from __future__ import annotations

import logging
import signal
import time

from confluent_kafka import Producer

from simulator.config import SimulatorConfig
from simulator.event_generator import ClickstreamSimulator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("simulator.producer")

_shutdown_requested = False


def _request_shutdown(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True


def _delivery_callback(err, msg):
    if err is not None:
        logger.error("Delivery failed for event: %s", err)


def run() -> None:
    # confluent-kafka's underlying C library interferes with Python's default
    # SIGINT handling, so a plain try/except KeyboardInterrupt around the loop
    # never fires -- Ctrl+C would otherwise hang or kill -9 would be needed.
    # Registering our own handler and checking a flag each iteration is the
    # documented workaround.
    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    config = SimulatorConfig.from_env()
    producer = Producer({"bootstrap.servers": config.broker})
    simulator = ClickstreamSimulator(num_concurrent_users=config.num_concurrent_users)

    sleep_interval = 1.0 / config.events_per_second
    sent_count = 0
    last_log_time = time.monotonic()

    logger.info(
        "Producing to topic '%s' on %s at ~%.1f events/sec (%d concurrent simulated sessions)",
        config.topic_name,
        config.broker,
        config.events_per_second,
        config.num_concurrent_users,
    )

    while not _shutdown_requested:
        event = simulator.next_event()
        # Keying by user_id keeps all of a user's events on the same partition,
        # so a downstream consumer sees them in order -- matters for sessionization.
        producer.produce(
            config.topic_name,
            key=event.user_id.encode("utf-8"),
            value=event.to_json().encode("utf-8"),
            callback=_delivery_callback,
        )
        producer.poll(0)  # trigger delivery callbacks without blocking the send loop
        sent_count += 1

        if time.monotonic() - last_log_time >= 5:
            logger.info("Sent %d events so far", sent_count)
            last_log_time = time.monotonic()

        time.sleep(sleep_interval)

    logger.info("Shutting down, flushing pending messages...")
    producer.flush(timeout=10)
    logger.info("Producer stopped. Total events sent: %d", sent_count)


if __name__ == "__main__":
    run()
