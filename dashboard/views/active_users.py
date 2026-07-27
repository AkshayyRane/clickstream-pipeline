"""Active users page -- mart_dau_wau_mau."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from db import active_users
from palette import CATEGORICAL, GRIDLINE, MUTED_INK
from ui import render_source_selector

st.title("Active users")
source = render_source_selector()

df = active_users(source)

if source == "stream":
    st.caption(
        "Stream is a single day, so this is one point per series rather than "
        "a trend -- switch to `batch_historical` for a real multi-week view."
    )

is_single_point = len(df) <= 1
# See the xaxis comment below -- a single row needs a formatted string (a
# clean "2026-07-27" category label) rather than the raw Timestamp, which
# renders as an ugly "2026-07-27T00:00:00.000000" when read as a category.
x_values = df["date_day"].dt.strftime("%Y-%m-%d") if is_single_point else df["date_day"]

fig = go.Figure()
for column, label, color in [
    ("dau", "DAU", CATEGORICAL[0]),
    ("wau", "WAU", CATEGORICAL[1]),
    ("mau", "MAU", CATEGORICAL[2]),
]:
    fig.add_trace(
        go.Scatter(
            x=x_values,
            y=df[column],
            mode="lines+markers",
            name=label,
            line=dict(color=color, width=2),
            marker=dict(size=6),
            hovertemplate=f"{label}: %{{y:,}}<extra></extra>",
        )
    )

fig.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font_color=MUTED_INK,
    margin=dict(l=10, r=10, t=10, b=10),
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    # A real date axis (the default) gives nice "May 2015"-style ticks across
    # a real multi-week range -- but with a single row (the stream source)
    # Plotly's date-axis autorange degenerates into a nonsensical
    # microsecond-precision tick band around that one point (same failure
    # mode fixed in views/retention.py's heatmap). Force category type only
    # in that single-point case, where a date axis buys nothing anyway.
    xaxis=dict(title=None, showgrid=False, type="category" if is_single_point else "date"),
    yaxis=dict(title="Distinct users", gridcolor=GRIDLINE, zeroline=False),
)
st.plotly_chart(fig, use_container_width=True)

with st.expander("View as table"):
    st.dataframe(
        df.rename(columns={"date_day": "Date", "dau": "DAU", "wau": "WAU", "mau": "MAU"}),
        use_container_width=True,
        hide_index=True,
    )
