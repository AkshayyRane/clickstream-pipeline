{#
    Weekly cohorts, per source. Grain: one row per (source, cohort_week,
    weeks_since_cohort). Cohort = a user's first-ever activity week; retained
    = the user had >=1 event in a later week. Weekly (not daily) because the
    historical dataset spans ~20 weeks and daily cohort/day cells would be too
    sparse to be meaningful; note the stream source is a single dev
    smoke-test day, so it will only ever show one degenerate
    weeks_since_cohort=0, retention_rate=100% row -- expected, not a bug.
#}

with events as (
    select user_id, source, event_date from {{ ref('fct_events') }}
),

user_activity_weeks as (
    select distinct
        user_id,
        source,
        date_trunc('week', event_date) as activity_week
    from events
),

user_cohorts as (
    select
        user_id,
        source,
        min(activity_week) as cohort_week
    from user_activity_weeks
    group by user_id, source
),

user_activity_with_cohort as (
    select
        a.user_id,
        a.source,
        c.cohort_week,
        datediff('week', c.cohort_week, a.activity_week) as weeks_since_cohort
    from user_activity_weeks a
    inner join user_cohorts c
        on a.user_id = c.user_id
        and a.source = c.source
),

cohort_sizes as (
    select
        source,
        cohort_week,
        count(distinct user_id) as cohort_size
    from user_cohorts
    group by source, cohort_week
),

retention as (
    select
        source,
        cohort_week,
        weeks_since_cohort,
        count(distinct user_id) as retained_users
    from user_activity_with_cohort
    group by source, cohort_week, weeks_since_cohort
)

select
    r.source,
    r.cohort_week,
    r.weeks_since_cohort,
    cs.cohort_size,
    r.retained_users,
    round(100.0 * r.retained_users / nullif(cs.cohort_size, 0), 2) as retention_rate
from retention r
inner join cohort_sizes cs
    on r.source = cs.source
    and r.cohort_week = cs.cohort_week
order by r.source, r.cohort_week, r.weeks_since_cohort
