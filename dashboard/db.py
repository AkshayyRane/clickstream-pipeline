"""Cached, read-only access to the dbt warehouse (warehouse/clickstream.duckdb).

Read-only because the dashboard never writes -- and it avoids any lock
conflict with a `dbt run` writing the same file. Query results are wrapped in
st.cache_data so switching pages/toggling the source filter doesn't re-run
identical queries against a multi-million-row fact table on every rerun.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from config import DashboardConfig


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    config = DashboardConfig.from_env()
    return duckdb.connect(str(config.duckdb_path), read_only=True)


@st.cache_data
def list_sources() -> list[str]:
    rows = get_connection().execute("select distinct source from fct_events order by source").fetchall()
    return [row[0] for row in rows]


@st.cache_data
def overview_kpis(source: str) -> dict:
    con = get_connection()
    total_events = con.execute("select count(*) from fct_events where source = ?", [source]).fetchone()[0]
    total_sessions = con.execute("select count(*) from fct_sessions where source = ?", [source]).fetchone()[0]
    min_date, max_date = con.execute(
        "select min(event_date), max(event_date) from fct_events where source = ?", [source]
    ).fetchone()
    return {
        "total_events": total_events,
        "total_sessions": total_sessions,
        "min_date": min_date,
        "max_date": max_date,
    }


@st.cache_data
def event_type_breakdown(source: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute(
        """
        select event_type, count(*) as event_count
        from fct_events
        where source = ?
        group by event_type
        order by event_count desc
        """,
        [source],
    ).df()


@st.cache_data
def funnel(source: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute(
        "select * from mart_funnel_analysis where source = ? order by step_number", [source]
    ).df()


@st.cache_data
def active_users(source: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute(
        "select * from mart_dau_wau_mau where source = ? order by date_day", [source]
    ).df()


@st.cache_data
def retention(source: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute(
        "select * from mart_retention where source = ? order by cohort_week, weeks_since_cohort", [source]
    ).df()
