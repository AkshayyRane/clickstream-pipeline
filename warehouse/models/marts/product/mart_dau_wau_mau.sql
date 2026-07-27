{#
    Grain: one row per (source, date_day) from dim_date's per-source spine.
    WAU/MAU can't be a windowed COUNT(DISTINCT ...) (not valid SQL), so each is
    a join back to fct_events with a date range condition, grouped by the
    spine date -- portable to BigQuery too, no DuckDB-only tricks.
#}

with dates as (
    select source, date_day from {{ ref('dim_date') }}
),

events as (
    select user_id, source, event_date from {{ ref('fct_events') }}
),

dau as (
    select
        source,
        event_date as date_day,
        count(distinct user_id) as dau
    from events
    group by source, event_date
),

wau as (
    select
        d.source,
        d.date_day,
        count(distinct e.user_id) as wau
    from dates d
    inner join events e
        on e.source = d.source
        and e.event_date between cast(d.date_day - interval '6 days' as date) and d.date_day
    group by d.source, d.date_day
),

mau as (
    select
        d.source,
        d.date_day,
        count(distinct e.user_id) as mau
    from dates d
    inner join events e
        on e.source = d.source
        and e.event_date between cast(d.date_day - interval '29 days' as date) and d.date_day
    group by d.source, d.date_day
)

select
    d.source,
    d.date_day,
    coalesce(dau.dau, 0) as dau,
    coalesce(wau.wau, 0) as wau,
    coalesce(mau.mau, 0) as mau
from dates d
left join dau on dau.source = d.source and dau.date_day = d.date_day
left join wau on wau.source = d.source and wau.date_day = d.date_day
left join mau on mau.source = d.source and mau.date_day = d.date_day
order by d.source, d.date_day
