"""Pure, unit-testable data quality checks run against a bronze events DataFrame.

Each check takes a pandas DataFrame and either returns None (pass) or raises
AssertionError with a descriptive message (fail). data_quality_dag.py wraps
each of these in a thin PythonOperator -- a failing check shows up as a red
task in the Airflow UI, which *is* the quality gate; no separate
pass/fail-aggregation task is needed. Same architectural pattern as
simulator/event_generator.py: business logic lives in plain, testable
functions, and Airflow/Kafka-specific code is a thin wrapper around it.
"""

from __future__ import annotations

import glob
from pathlib import Path

import pandas as pd

from simulator.schemas import EventType

REQUIRED_FIELDS = ["event_id", "user_id", "event_type", "event_timestamp"]
VALID_EVENT_TYPES = {e.value for e in EventType}


def load_bronze_events(root: Path) -> pd.DataFrame:
    """Reads every .jsonl file under a partitioned bronze root into one DataFrame."""
    files = sorted(glob.glob(str(Path(root) / "**" / "*.jsonl"), recursive=True))
    if not files:
        return pd.DataFrame(columns=REQUIRED_FIELDS)
    return pd.concat((pd.read_json(f, lines=True) for f in files), ignore_index=True)


def check_row_count_positive(df: pd.DataFrame) -> None:
    assert len(df) > 0, "Bronze partition is empty -- expected at least one event"


def check_required_fields_not_null(df: pd.DataFrame) -> None:
    for field in REQUIRED_FIELDS:
        null_count = int(df[field].isna().sum())
        assert null_count == 0, f"{null_count} rows have a null '{field}', which is required"


def check_event_type_values_valid(df: pd.DataFrame) -> None:
    invalid = set(df["event_type"].unique()) - VALID_EVENT_TYPES
    assert not invalid, f"Found event_type values outside the allowed set: {invalid}"


def check_no_duplicate_event_ids(df: pd.DataFrame) -> None:
    duplicate_count = int(df["event_id"].duplicated().sum())
    assert duplicate_count == 0, f"Found {duplicate_count} duplicate event_id values"


ALL_CHECKS = [
    check_row_count_positive,
    check_required_fields_not_null,
    check_event_type_values_valid,
    check_no_duplicate_event_ids,
]


if __name__ == "__main__":
    from batch_source.config import BatchConfig

    config = BatchConfig.from_env()
    events_df = load_bronze_events(config.historical_bronze_output_path)
    for check in ALL_CHECKS:
        check(events_df)
        print(f"PASS: {check.__name__}")
