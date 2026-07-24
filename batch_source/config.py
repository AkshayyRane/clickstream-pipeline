"""Env-driven settings for the batch ingestion scripts, loaded from .env at the repo root."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Resolved from this file's own location, not the current working directory --
# so paths are correct whether these scripts run from the host repo root
# (Phase 1 style) or inside the Airflow container, where the repo is mounted
# at a different absolute path.
PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class BatchConfig:
    raw_dataset_dir: Path
    historical_bronze_output_path: Path
    chunk_size: int

    @classmethod
    def from_env(cls) -> "BatchConfig":
        return cls(
            raw_dataset_dir=PROJECT_ROOT / "batch_source" / "raw",
            historical_bronze_output_path=PROJECT_ROOT
            / os.environ.get("HISTORICAL_BRONZE_OUTPUT_PATH", "data/bronze/events_historical"),
            chunk_size=int(os.environ.get("BATCH_CHUNK_SIZE", "50000")),
        )
