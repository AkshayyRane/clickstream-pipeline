"""Transforms the RetailRocket events.csv into bronze-layer JSONL, conforming to
the same event contract the streaming path uses (simulator/schemas.py).

Mapping: view -> page_view, addtocart -> add_to_cart, transaction -> purchase.
RetailRocket has no session concept and no click/signup equivalent -- session_id
is left None (inferred later by dbt), and only the three event types above appear.

event_id is a deterministic uuid5 hash of each row's natural key, and output
filenames are deterministic per (dt, chunk index) -- re-running this script
(an Airflow retry, or re-triggering the DAG) overwrites with identical content
rather than accumulating duplicates. That's the opposite of the streaming
consumer's uuid4-suffixed files (consumer/kafka_to_bronze.py), which is correct
there for an always-growing stream but wrong for a static full-dataset
transform like this one.

Run with:  python -m batch_source.transform_to_bronze
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

import pandas as pd

from batch_source.config import BatchConfig
from batch_source.download_dataset import run as ensure_dataset_downloaded

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("batch_source.transform_to_bronze")

EVENT_TYPE_MAP = {
    "view": "page_view",
    "addtocart": "add_to_cart",
    "transaction": "purchase",
}

# Fixed namespace so uuid5(...) is stable across runs and machines.
EVENT_ID_NAMESPACE = uuid.UUID("6f1c1f0e-2b3a-4b8a-9c7a-8e5f6a1d2c3b")


def _row_event_id(row) -> str:
    key = f"{row.visitorid}-{row.timestamp}-{row.event}-{row.itemid}"
    return str(uuid.uuid5(EVENT_ID_NAMESPACE, key))


def _row_properties(row) -> dict:
    properties = {"item_id": str(row.itemid)}
    if pd.notna(row.transactionid):
        properties["transaction_id"] = str(int(row.transactionid))
    return properties


def transform(events_csv_path: Path) -> pd.DataFrame:
    """Reads the raw CSV and returns a DataFrame already shaped like bronze records."""
    raw = pd.read_csv(events_csv_path)
    raw = raw[raw["event"].isin(EVENT_TYPE_MAP)]

    # The raw CSV genuinely contains exact-duplicate rows (same visitor, same
    # millisecond timestamp, same event, same item -- 918 rows / 459 pairs in
    # the real dataset), almost certainly double-fired analytics beacons, a
    # real clickstream phenomenon. Our event_id is a deterministic hash of this
    # same natural key, so left alone these collide and check_no_duplicate_
    # event_ids (quality_checks.py) correctly flags it. Dropping exact
    # duplicates here -- not in the quality check -- is the right layer for
    # this: it's a data-cleaning decision about the source, not a relaxation of
    # what "duplicate" means downstream. The check stays in place as a safety
    # net for any *other* source of duplication (e.g. a bug in this hash).
    natural_key = ["visitorid", "timestamp", "event", "itemid"]
    before = len(raw)
    raw = raw.drop_duplicates(subset=natural_key)
    dropped = before - len(raw)
    if dropped:
        logger.info("Dropped %d exact-duplicate source rows (double-fired events)", dropped)

    # Row-wise .apply(), not vectorized -- 2.75M rows runs in well under a
    # minute this way, and keeping the per-row mapping logic in plain Python
    # functions (_row_event_id, _row_properties) is far more readable than a
    # fully vectorized version. A deliberate readability-over-throughput
    # tradeoff at this data volume, worth flagging as such.
    out = pd.DataFrame(
        {
            "event_id": raw.apply(_row_event_id, axis=1),
            "user_id": "hist-" + raw["visitorid"].astype(str),
            "session_id": None,
            "event_type": raw["event"].map(EVENT_TYPE_MAP),
            "event_properties": raw.apply(_row_properties, axis=1),
            "source": "batch_historical",
        }
    )

    event_dt = pd.to_datetime(raw["timestamp"], unit="ms", utc=True)
    out["event_timestamp"] = event_dt.dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str[:-3] + "Z"
    out["dt"] = event_dt.dt.strftime("%Y-%m-%d")
    return out


def write_bronze(df: pd.DataFrame, output_root: Path, chunk_size: int) -> int:
    """Writes df in ~chunk_size-row files, partitioned by dt. Returns file count."""
    file_count = 0
    for dt, group in df.groupby("dt"):
        out_dir = output_root / f"dt={dt}"
        out_dir.mkdir(parents=True, exist_ok=True)
        records = group.drop(columns=["dt"])
        for start in range(0, len(records), chunk_size):
            chunk = records.iloc[start : start + chunk_size]
            out_path = out_dir / f"part-{start // chunk_size:04d}.jsonl"
            chunk.to_json(out_path, orient="records", lines=True)
            file_count += 1
    return file_count


def run() -> int:
    config = BatchConfig.from_env()
    events_file = ensure_dataset_downloaded()  # no-op if already downloaded

    logger.info("Reading %s ...", events_file)
    df = transform(events_file)
    logger.info("Transformed %d events, writing to %s ...", len(df), config.historical_bronze_output_path)
    file_count = write_bronze(df, config.historical_bronze_output_path, config.chunk_size)
    logger.info("Wrote %d partitioned files under %s", file_count, config.historical_bronze_output_path)
    return file_count


if __name__ == "__main__":
    run()
