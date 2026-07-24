"""Env-driven settings for the simulator, loaded from .env at the repo root."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class SimulatorConfig:
    broker: str
    topic_name: str
    events_per_second: float
    num_concurrent_users: int

    @classmethod
    def from_env(cls) -> "SimulatorConfig":
        return cls(
            broker=os.environ.get("REDPANDA_BROKER", "localhost:19092"),
            topic_name=os.environ.get("TOPIC_NAME", "clickstream-events"),
            events_per_second=float(os.environ.get("EVENTS_PER_SECOND", "10")),
            num_concurrent_users=int(os.environ.get("NUM_CONCURRENT_USERS", "50")),
        )
