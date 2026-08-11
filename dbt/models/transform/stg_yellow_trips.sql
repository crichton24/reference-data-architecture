{{
    config(
        materialized='view'
    )
}}

/*
    Yellow taxi trips, normalized to the shared trip grain.

    Staging does three things and nothing else: rename to snake_case, cast
    types, and resolve timezone. No filtering, no joins, no business logic —
    those belong in marts, where they're visible to anyone reading the
    dimensional model.

    TIMEZONE HANDLING
    TLC records wall-clock Eastern time with no offset, stored as
    TIMESTAMP_NTZ. to_utc_timestamp() interprets it as America/New_York and
    returns a true instant, which is what any cross-day or cross-DST
    comparison needs.

    Both forms are kept. pickup_at is the instant (correct for arithmetic);
    pickup_at_local is the wall clock TLC actually recorded (correct for
    "trips between 5pm and 6pm" style questions, which mean local time).

    DST caveat: 1:30 AM on fall-back day occurs twice and TLC's data can't
    distinguish them. Spark resolves the ambiguity to the earlier instant.
    Affects roughly one hour per year; documented rather than corrected.
*/

with source as (

    select * from {{ source('nyc_tlc', 'yellow_trips') }}

),

renamed as (

    select
        -- surrogate key: yellow and green are separate streams that must not
        -- collide once unioned
        {{ dbt_utils.generate_surrogate_key([
            "'yellow'",
            'vendorid',
            'tpep_pickup_datetime',
            'tpep_dropoff_datetime',
            'pulocationid',
            'dolocationid',
            'passenger_count',
            'trip_distance',
            'total_amount'
        ]) }}                                              as trip_key,

        'Yellow Taxi'                                      as tlc_type,

        -- identifiers
        cast(vendorid as bigint)                           as vendor_id,
        cast(ratecodeid as bigint)                         as rate_code_id,
        cast(payment_type as bigint)                       as payment_type,
        cast(pulocationid as bigint)                       as pickup_location_id,
        cast(dolocationid as bigint)                       as dropoff_location_id,

        -- green-only column, present here so the union aligns
        cast(null as bigint)                               as trip_type,

        -- timestamps
        to_utc_timestamp(
            tpep_pickup_datetime, 'America/New_York'
        )                                                  as pickup_at,
        to_utc_timestamp(
            tpep_dropoff_datetime, 'America/New_York'
        )                                                  as dropoff_at,
        cast(tpep_pickup_datetime as timestamp_ntz)        as pickup_at_local,
        cast(tpep_dropoff_datetime as timestamp_ntz)       as dropoff_at_local,

        -- measures
        cast(passenger_count as bigint)                    as passenger_count,
        cast(trip_distance as double)                      as trip_distance_miles,
        cast(fare_amount as double)                        as fare_amount,
        cast(extra as double)                              as extra_amount,
        cast(mta_tax as double)                            as mta_tax_amount,
        cast(tip_amount as double)                         as tip_amount,
        cast(tolls_amount as double)                       as tolls_amount,
        cast(improvement_surcharge as double)              as improvement_surcharge,
        cast(congestion_surcharge as double)               as congestion_surcharge,
        cast(total_amount as double)                       as total_amount,

        -- yellow-only
        cast(airport_fee as double)                        as airport_fee,
        cast(null as double)                               as ehail_fee,

        -- congestion pricing: column absent before 2025, so mergeSchema
        -- backfills NULL. Coalescing here prevents a NULL from poisoning any
        -- downstream sum.
        coalesce(cast(cbd_congestion_fee as double), 0)    as cbd_congestion_fee,

        store_and_fwd_flag                                 as store_and_forward_flag,

        -- lineage
        period                                             as source_period,
        _loaded_from                                       as source_file,
        _loaded_at                                         as loaded_at

    from source

)

select * from renamed
