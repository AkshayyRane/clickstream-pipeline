"""Env-driven settings for the bronze consumer, loaded from .env at the repo root."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class ConsumerConfig:
    broker: str
    topic_name: str
    group_id: str
    bronze_output_path: str
    batch_size: int
    flush_interval_seconds: float

    @classmethod
    def from_env(cls) -> "ConsumerConfig":
        return cls(
            broker=os.environ.get("REDPANDA_BROKER", "localhost:19092"),
            topic_name=os.environ.get("TOPIC_NAME", "clickstream-events"),
            group_id=os.environ.get("CONSUMER_GROUP_ID", "bronze-writer"),
            bronze_output_path=os.environ.get("BRONZE_OUTPUT_PATH", "data/bronze/events"),
            batch_size=int(os.environ.get("CONSUMER_BATCH_SIZE", "100")),
            flush_interval_seconds=float(os.environ.get("CONSUMER_FLUSH_INTERVAL_SECONDS", "10")),
        )
