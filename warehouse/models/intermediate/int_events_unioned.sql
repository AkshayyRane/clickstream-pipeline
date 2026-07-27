with stream_events as (
    select
        event_id, user_id, session_id, event_type, event_timestamp, event_date,
        element_id, url, referrer, item_id, transaction_id, source
    from {{ ref('stg_events_stream') }}
),

historical_events as (
    select
        event_id, user_id, session_id, event_type, event_timestamp, event_date,
        element_id, url, referrer, item_id, transaction_id, source
    from {{ ref('stg_events_historical') }}
)

select * from stream_events
union all
select * from historical_events
