with source_data as (
    -- Deliberately not named `source` -- the bronze payload itself has a
    -- column literally called `source`, and a same-named CTE shadows it.
    select * from {{ source('bronze_stream', 'events') }}
),

renamed as (
    select
        event_id,
        user_id,
        session_id,
        event_type,
        cast(event_timestamp as timestamptz) as event_timestamp,
        cast(cast(event_timestamp as timestamptz) as date) as event_date,
        json_extract_string(event_properties, '$.element_id') as element_id,
        json_extract_string(event_properties, '$.url') as url,
        json_extract_string(event_properties, '$.referrer') as referrer,
        cast(null as varchar) as item_id,
        cast(null as varchar) as transaction_id,
        -- Hardcoded, not read from the payload's own `source` field: which
        -- bronze tree a file lives in already tells you the source, and
        -- trusting it here is more robust than trusting the payload -- the
        -- Phase 1 bronze files on disk (data/bronze/events/dt=2026-07-23/...)
        -- predate `source` being added to the event contract in Phase 2
        -- (simulator/schemas.py, commit 2ecd333) and don't have the key at
        -- all, which silently NULLed this column when read from the payload.
        'stream' as source,
        dt as _ingested_dt,
        hour as _ingested_hour
    from source_data
)

select * from renamed
