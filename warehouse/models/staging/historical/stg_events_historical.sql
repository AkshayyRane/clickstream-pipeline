with source_data as (
    -- Deliberately not named `source` -- the bronze payload itself has a
    -- column literally called `source`, and a same-named CTE shadows it.
    select * from {{ source('bronze_historical', 'events') }}
),

renamed as (
    select
        event_id,
        user_id,
        session_id,
        event_type,
        cast(event_timestamp as timestamptz) as event_timestamp,
        cast(cast(event_timestamp as timestamptz) as date) as event_date,
        cast(null as varchar) as element_id,
        cast(null as varchar) as url,
        cast(null as varchar) as referrer,
        json_extract_string(event_properties, '$.item_id') as item_id,
        json_extract_string(event_properties, '$.transaction_id') as transaction_id,
        -- Hardcoded, not read from the payload -- see stg_events_stream.sql
        -- for why trusting the source tree over the payload field is safer.
        'batch_historical' as source,
        dt as _ingested_dt
    from source_data
)

select * from renamed
