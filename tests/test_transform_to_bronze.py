"""Regression test for the exact-duplicate-row dedup in transform().

Found via a real end-to-end run against the actual RetailRocket dataset: 460
rows shared an identical natural key (visitorid, timestamp, event, itemid) --
almost certainly double-fired analytics beacons -- which our uuid5-based
event_id (deterministic per natural key) turned into 460 duplicate event_ids,
correctly caught by check_no_duplicate_event_ids. transform() now drops exact
duplicates before generating event_ids; this test guards against that
regressing silently.
"""

from __future__ import annotations

import pandas as pd

from batch_source.transform_to_bronze import transform


def test_exact_duplicate_rows_are_dropped(tmp_path):
    csv_path = tmp_path / "events.csv"
    csv_path.write_text(
        "timestamp,visitorid,event,itemid,transactionid\n"
        "1433221332117,102019,view,355908,\n"
        "1433221332117,102019,view,355908,\n"  # exact duplicate of the row above
        "1433221333000,102020,addtocart,248455,\n"
    )

    out = transform(csv_path)

    assert len(out) == 2
    assert out["event_id"].is_unique


def test_near_duplicates_with_different_natural_keys_are_kept(tmp_path):
    csv_path = tmp_path / "events.csv"
    csv_path.write_text(
        "timestamp,visitorid,event,itemid,transactionid\n"
        "1433221332117,102019,view,355908,\n"
        "1433221332118,102019,view,355908,\n"  # same visitor/item, different timestamp
    )

    out = transform(csv_path)

    assert len(out) == 2
    assert out["event_id"].is_unique
    assert pd.Series(out["event_id"]).nunique() == 2
