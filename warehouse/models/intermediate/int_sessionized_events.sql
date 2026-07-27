{#
    Attaches a session_id to every event (stream events already have one;
    historical events get the one inferred in int_sessions.sql) and numbers
    events within their session. Historical events are matched to their
    inferred session by user_id + falling inside [session_start_at,
    session_end_at] -- sessions are gap-based and non-overlapping per user by
    construction, so this join is unambiguous.
#}

with events as (
    select * from {{ ref('int_events_unioned') }}
),

sessions as (
    select * from {{ ref('int_sessions') }}
),

stream_sessionized as (
    select
        e.event_id, e.user_id, e.event_type, e.event_timestamp, e.event_date,
        e.element_id, e.url, e.referrer, e.item_id, e.transaction_id, e.source,
        e.session_id
    from events e
    where e.source = 'stream'
),

historical_sessionized as (
    select
        e.event_id, e.user_id, e.event_type, e.event_timestamp, e.event_date,
        e.element_id, e.url, e.referrer, e.item_id, e.transaction_id, e.source,
        s.session_id
    from events e
    inner join sessions s
        on e.user_id = s.user_id
        and e.source = s.source
        and e.event_timestamp between s.session_start_at and s.session_end_at
    where e.source = 'batch_historical'
),

combined as (
    select * from stream_sessionized
    union all
    select * from historical_sessionized
)

select
    *,
    row_number() over (partition by session_id order by event_timestamp) as event_sequence_number
from combined
