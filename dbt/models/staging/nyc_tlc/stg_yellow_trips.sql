-- Staging: rename to project conventions, cast types, drop nothing.
-- Business logic belongs downstream; this layer only makes bronze legible.

with source as (

    select * from {{ source('bronze', 'yellow_trips') }}

),

renamed as (

    select
        -- identifiers
        vendorid                        as vendor_id,
        pulocationid                    as pickup_zone_id,
        dolocationid                    as dropoff_zone_id,
        ratecodeid                      as rate_code_id,

        -- timestamps
        tpep_pickup_datetime            as pickup_at,
        tpep_dropoff_datetime           as dropoff_at,

        -- measures
        passenger_count,
        trip_distance                   as trip_distance_miles,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,

        -- congestion pricing: absent before 2025, so coalesce rather than
        -- assume. See ADR 0004 on schema evolution.
        coalesce(cbd_congestion_fee, 0)  as cbd_congestion_fee,

        -- lineage
        period                          as source_period,
        _source_file,
        _loaded_at

    from source

)

select * from renamed
