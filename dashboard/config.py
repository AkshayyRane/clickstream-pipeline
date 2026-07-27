"""Env-driven settings for the dashboard, loaded from .env at the repo root."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Resolved from this file's own location, not the current working directory --
# same reasoning as batch_source/config.py: correct regardless of where
# `streamlit run` is invoked from.
PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class DashboardConfig:
    duckdb_path: Path

    @classmethod
    def from_env(cls) -> "DashboardConfig":
        return cls(
            # Same DBT_DUCKDB_PATH warehouse/profiles.yml uses -- the
            # dashboard reads the file dbt already builds, not a copy of it.
            duckdb_path=PROJECT_ROOT / os.environ.get("DBT_DUCKDB_PATH", "warehouse/clickstream.duckdb"),
        )
