"""Retention page -- mart_retention, weekly cohorts."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st
from db import retention
from palette import MUTED_INK, SEQUENTIAL_BLUE
from ui import render_source_selector

st.title("Retention")
source = render_source_selector()

df = retention(source)

if source == "stream":
    st.caption(
        "Stream is a single dev day, so this is one cohort with one "
        "weeks_since_cohort=0 cell at 100% -- expected, not a broken chart. "
        "Switch to `batch_historical` for a real multi-week cohort grid."
    )

pivot = df.pivot(index="cohort_week", columns="weeks_since_cohort", values="retention_rate")
colorscale = [[i / (len(SEQUENTIAL_BLUE) - 1), color] for i, color in enumerate(SEQUENTIAL_BLUE)]

fig = go.Figure(
    go.Heatmap(
        z=pivot.values,
        x=[f"W{w}" for w in pivot.columns],
        y=pivot.index.strftime("%Y-%m-%d"),
        colorscale=colorscale,
        zmin=0,
        zmax=100,
        colorbar=dict(title="Retention %"),
        hovertemplate="Cohort %{y}, week %{x}: %{z:.1f}%<extra></extra>",
    )
)
fig.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font_color=MUTED_INK,
    margin=dict(l=10, r=10, t=10, b=10),
    # type="category" on both axes: Plotly infers an axis type from the data,
    # and formatted date *strings* (2015-05-03) still get read as a
    # continuous date axis -- harmless with many rows, but with a single
    # cohort (the stream source) it renders a nonsensical
    # microsecond-precision tick range around that one point instead of one
    # clean label. These axes are inherently discrete categories regardless
    # of row count, so forcing category type is correct either way, not just
    # a single-row patch.
    xaxis=dict(title="Weeks since cohort", showgrid=False, type="category"),
    yaxis=dict(title="Cohort week", autorange="reversed", type="category"),
)
st.plotly_chart(fig, use_container_width=True)

with st.expander("View as table"):
    st.dataframe(
        df.rename(
            columns={
                "cohort_week": "Cohort week",
                "weeks_since_cohort": "Weeks since cohort",
                "cohort_size": "Cohort size",
                "retained_users": "Retained users",
                "retention_rate": "Retention %",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
