"""Funnel analysis page -- mart_funnel_analysis."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from db import funnel
from palette import MUTED_INK, ORDINAL_BLUE
from ui import render_source_selector

st.title("Funnel analysis")
source = render_source_selector()

df = funnel(source)

if source == "stream":
    st.caption(
        "Stream's simulator has a real `click` step, so this funnel has 4 stages. "
        "Historical (RetailRocket) has no click event at all, so it only has 3 -- "
        "computed as separate funnels on purpose, not one combined step list "
        "(see README's Phase 3 notes)."
    )
else:
    st.caption("3 stages -- RetailRocket has no `click`/`signup` event types to build a 4th stage from.")

fig = go.Figure(
    go.Funnel(
        y=df["step_name"],
        x=df["sessions_reached"],
        marker=dict(color=ORDINAL_BLUE[: len(df)]),
        textinfo="value+percent initial+percent previous",
        hovertemplate="%{y}: %{x:,} sessions<extra></extra>",
    )
)
fig.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font_color=MUTED_INK,
    margin=dict(l=10, r=10, t=10, b=10),
)
st.plotly_chart(fig, use_container_width=True)

with st.expander("View as table"):
    st.dataframe(
        df.rename(
            columns={
                "step_number": "Step",
                "step_name": "Event type",
                "sessions_reached": "Sessions reached",
                "pct_of_funnel_start": "% of funnel start",
                "pct_of_previous_step": "% of previous step",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
