"""Shared sidebar source selector -- rendered on every page.

Each page under st.navigation mounts its selectbox as a fresh widget
instance, so an `index=` computed the same way every time (e.g. always
"batch_historical"'s position) silently overrides whatever the user
previously picked the moment they switch pages -- Streamlit only preserves
a widget's *live* value across reruns of the *same* page, not across a new
page's fresh mount. The fix is the standard one: keep the actual selection
in a plain session_state variable (SOURCE_KEY, not the widget's own key),
seed `index` from that on every render, and sync it via on_change.
"""

from __future__ import annotations

import streamlit as st
from db import list_sources

SOURCE_KEY = "source_value"
_WIDGET_KEY = "_source_widget"


def _sync_source() -> None:
    st.session_state[SOURCE_KEY] = st.session_state[_WIDGET_KEY]


def render_source_selector() -> str:
    sources = list_sources()

    if SOURCE_KEY not in st.session_state:
        st.session_state[SOURCE_KEY] = "batch_historical" if "batch_historical" in sources else sources[0]

    current = st.session_state[SOURCE_KEY]
    default_index = sources.index(current) if current in sources else 0

    st.sidebar.selectbox(
        "Source",
        options=sources,
        index=default_index,
        key=_WIDGET_KEY,
        on_change=_sync_source,
        help=(
            "Marts are computed per source, not combined -- stream and "
            "historical don't share a timeline (see README's Phase 3 notes)."
        ),
    )

    source = st.session_state[SOURCE_KEY]
    if source == "stream":
        st.sidebar.caption(
            "`stream` is a single dev smoke-test day, not an ongoing feed -- "
            "trend charts here are expected to look sparse."
        )
    return source
