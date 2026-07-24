"""Unit tests for batch_source/quality_checks.py -- both passing and
deliberately broken DataFrames, mirroring how tests/test_event_generator.py
exercises the funnel state machine: prove the checks actually catch bad data,
not just pass on the happy path.
"""

from __future__ import annotations

import pandas as pd
import pytest

from batch_source.quality_checks import (
    check_event_type_values_valid,
    check_no_duplicate_event_ids,
    check_required_fields_not_null,
    check_row_count_positive,
)


def _valid_df(n=3):
    return pd.DataFrame(
        {
            "event_id": [f"e{i}" for i in range(n)],
            "user_id": [f"u{i}" for i in range(n)],
            "event_type": ["page_view"] * n,
            "event_timestamp": ["2026-07-23T00:00:00.000Z"] * n,
        }
    )


def test_row_count_positive_passes_on_nonempty():
    check_row_count_positive(_valid_df())


def test_row_count_positive_fails_on_empty():
    with pytest.raises(AssertionError):
        check_row_count_positive(_valid_df(0))


def test_required_fields_not_null_passes_when_clean():
    check_required_fields_not_null(_valid_df())


def test_required_fields_not_null_fails_on_null_event_id():
    df = _valid_df()
    df.loc[0, "event_id"] = None
    with pytest.raises(AssertionError):
        check_required_fields_not_null(df)


def test_event_type_values_valid_passes_for_known_types():
    df = _valid_df()
    df["event_type"] = ["page_view", "add_to_cart", "purchase"]
    check_event_type_values_valid(df)


def test_event_type_values_valid_fails_for_unknown_type():
    df = _valid_df()
    df.loc[0, "event_type"] = "not_a_real_event_type"
    with pytest.raises(AssertionError):
        check_event_type_values_valid(df)


def test_no_duplicate_event_ids_passes_when_unique():
    check_no_duplicate_event_ids(_valid_df())


def test_no_duplicate_event_ids_fails_on_duplicate():
    df = _valid_df()
    df.loc[1, "event_id"] = df.loc[0, "event_id"]
    with pytest.raises(AssertionError):
        check_no_duplicate_event_ids(df)
