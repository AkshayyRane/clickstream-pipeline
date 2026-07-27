{#
    Per-source date spine, not one combined timeline: the historical dataset
    spans 2015-05-03..2015-09-18 (139 days) and the stream dataset is a single
    dev smoke-test day (2026-07-23) -- a combined spine would be ~4,100 rows,
    almost all of them a meaningless decade-plus gap between the two. Each
    source gets its own spine scoped to its own min/max event_date instead, so
    mart_dau_wau_mau / mart_retention (which join on source + date_day) stay
    meaningful per source. See warehouse/models/marts/product/_product__models.yml
    for how those two marts consume this.
#}

{% set date_bounds_query %}
    select
        source,
        min(event_date) as min_date,
        max(event_date) as max_date
    from {{ ref('int_events_unioned') }}
    group by source
{% endset %}

{% if execute %}
    {% set source_bounds = run_query(date_bounds_query).rows %}
{% else %}
    {% set source_bounds = [] %}
{% endif %}

{% for row in source_bounds %}
select
    '{{ row["source"] }}' as source,
    cast(date_day as date) as date_day
from (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="(cast('" ~ row["min_date"] ~ "' as date))",
        end_date="(cast('" ~ row["max_date"] ~ "' as date) + interval '1 day')"
    ) }}
)
{% if not loop.last %} union all {% endif %}
{% endfor %}
