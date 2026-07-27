select
    session_id,
    user_id,
    source,
    session_start_at,
    session_end_at,
    event_count,
    datediff('second', session_start_at, session_end_at) as duration_seconds
from {{ ref('int_sessions') }}
