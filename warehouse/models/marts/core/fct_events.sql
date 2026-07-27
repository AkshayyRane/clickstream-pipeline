select
    event_id,
    user_id,
    session_id,
    event_sequence_number,
    event_type,
    event_timestamp,
    event_date,
    element_id,
    url,
    referrer,
    item_id,
    transaction_id,
    source
from {{ ref('int_sessionized_events') }}
