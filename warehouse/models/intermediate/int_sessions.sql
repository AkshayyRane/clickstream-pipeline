{#
    Two sessionization strategies, unioned into one session grain:

    - stream: session_id already exists (simulator/producer.py assigns one per
      simulated session), so this is a straight aggregation.
    - historical: RetailRocket has no session concept at all (session_id is
      NULL for every row -- see stg_events_historical.sql), so a session_id is
      inferred with the standard gap-based heuristic: a new session starts
      whenever the time since a user's previous event exceeds
      `session_gap_minutes` (default 30, see dbt_project.yml vars).
#}

with stream_sessions as (
    select
        session_id,
        user_id,
        source,
        min(event_timestamp) as session_start_at,
        max(event_timestamp) as session_end_at,
        count(*) as event_count
    from {{ ref('int_events_unioned') }}
    where source = 'stream'
    group by session_id, user_id, source
),

historical_events as (
    select *
    from {{ ref('int_events_unioned') }}
    where source = 'batch_historical'
),

historical_with_gaps as (
    select
        *,
        datediff(
            'minute',
            lag(event_timestamp) over (partition by user_id order by event_timestamp),
            event_timestamp
        ) as minutes_since_prev_event
    from historical_events
),

historical_with_session_flag as (
    select
        *,
        case
            when minutes_since_prev_event is null
                or minutes_since_prev_event > {{ var('session_gap_minutes') }}
            then 1
            else 0
        end as is_new_session
    from historical_with_gaps
),

historical_with_session_num as (
    select
        *,
        sum(is_new_session) over (
            partition by user_id
            order by event_timestamp
            rows between unbounded preceding and current row
        ) as session_num
    from historical_with_session_flag
),

historical_sessions_pre_key as (
    select
        user_id,
        source,
        session_num,
        min(event_timestamp) as session_start_at,
        max(event_timestamp) as session_end_at,
        count(*) as event_count
    from historical_with_session_num
    group by user_id, source, session_num
),

historical_sessions as (
    select
        {{ dbt_utils.generate_surrogate_key(['user_id', 'session_start_at']) }} as session_id,
        user_id,
        source,
        session_start_at,
        session_end_at,
        event_count
    from historical_sessions_pre_key
)

select * from stream_sessions
union all
select * from historical_sessions
