"""Overview page."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st
from db import event_type_breakdown, overview_kpis
from palette import CATEGORICAL, GRIDLINE, MUTED_INK
from ui import render_source_selector

st.title("Clickstream Pipeline")
st.caption("Overview of events, sessions, and event-type mix -- built on top of the Phase 3 dbt marts.")

source = render_source_selector()
kpis = overview_kpis(source)

col1, col2 = st.columns(2)
col1.metric("Total events", f"{kpis['total_events']:,}")
col2.metric("Total sessions", f"{kpis['total_sessions']:,}")
# Not a 3rd st.metric -- the widget truncates a value this long ("2015-05-02
# → 2015-09-17") instead of wrapping, so it's a plain caption line instead.
st.caption(f"**Date range:** {kpis['min_date']} → {kpis['max_date']}")

st.subheader("Event-type mix")

breakdown = event_type_breakdown(source)

fig = go.Figure(
    go.Bar(
        x=breakdown["event_type"],
        y=breakdown["event_count"],
        marker_color=CATEGORICAL[0],
        hovertemplate="%{x}: %{y:,}<extra></extra>",
    )
)
fig.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font_color=MUTED_INK,
    margin=dict(l=10, r=10, t=10, b=10),
    xaxis=dict(title=None, showgrid=False),
    yaxis=dict(title="Events", gridcolor=GRIDLINE, zeroline=False),
    bargap=0.3,
)
st.plotly_chart(fig, use_container_width=True)

with st.expander("View as table"):
    st.dataframe(breakdown, use_container_width=True, hide_index=True)

st.divider()
st.page_link("views/funnel.py", label="Funnel analysis", icon="\U0001f9ee")
st.page_link("views/active_users.py", label="Active users (DAU/WAU/MAU)", icon="\U0001f465")
st.page_link("views/retention.py", label="Retention", icon="\U0001f501")
