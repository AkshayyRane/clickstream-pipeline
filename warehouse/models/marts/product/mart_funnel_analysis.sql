{#
    Per-source funnel, not a lowest-common-denominator combined one: stream has
    a real 4-step funnel (page_view -> click -> add_to_cart -> purchase),
    historical only has 3 of those steps (no click event type at all -- see
    stg_events_historical.sql). Forcing both onto one step list would mean
    either dropping click entirely or fabricating a step historical can't
    have, so each source gets its own step-ordered funnel instead.

    "Reached a step" = the session has at least one event of that step's
    event_type, not a strict same-order-within-session check -- the simpler,
    more common definition for a funnel-conversion mart.
#}

with funnel_steps as (
    select * from (
        values
            ('stream', 1, 'page_view'),
            ('stream', 2, 'click'),
            ('stream', 3, 'add_to_cart'),
            ('stream', 4, 'purchase'),
            ('batch_historical', 1, 'page_view'),
            ('batch_historical', 2, 'add_to_cart'),
            ('batch_historical', 3, 'purchase')
    ) as t(source, step_number, event_type)
),

sessions_per_step as (
    select
        fs.source,
        fs.step_number,
        fs.event_type as step_name,
        count(distinct e.session_id) as sessions_reached
    from funnel_steps fs
    inner join {{ ref('fct_events') }} e
        on e.source = fs.source
        and e.event_type = fs.event_type
    group by fs.source, fs.step_number, fs.event_type
),

with_conversion as (
    select
        source,
        step_number,
        step_name,
        sessions_reached,
        first_value(sessions_reached) over (
            partition by source order by step_number
        ) as sessions_at_funnel_start,
        lag(sessions_reached) over (
            partition by source order by step_number
        ) as sessions_at_previous_step
    from sessions_per_step
)

select
    source,
    step_number,
    step_name,
    sessions_reached,
    round(100.0 * sessions_reached / nullif(sessions_at_funnel_start, 0), 2) as pct_of_funnel_start,
    round(100.0 * sessions_reached / nullif(sessions_at_previous_step, 0), 2) as pct_of_previous_step
from with_conversion
order by source, step_number
