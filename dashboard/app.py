"""Entrypoint -- `streamlit run dashboard/app.py`.

Uses st.navigation/st.Page (not the classic auto-discovered pages/
directory) deliberately: the classic mechanism resets widget-backed
session_state -- including the sidebar source filter every page shares --
on every page switch, which is a documented Streamlit limitation, not a
config option. st.navigation is Streamlit's own fix for exactly this.
"""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Clickstream Pipeline", page_icon="\U0001f4c8", layout="wide")

overview = st.Page("views/overview.py", title="Overview", icon="\U0001f4c8", default=True)
funnel = st.Page("views/funnel.py", title="Funnel", icon="\U0001f9ee")
active_users = st.Page("views/active_users.py", title="Active Users", icon="\U0001f465")
retention = st.Page("views/retention.py", title="Retention", icon="\U0001f501")

pg = st.navigation([overview, funnel, active_users, retention])
pg.run()
