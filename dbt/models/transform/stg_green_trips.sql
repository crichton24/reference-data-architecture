{{
    config(
        materialized='view'
    )
}}

/*
    Green taxi trips, normalized to the same grain and column set as
    stg_yellow_trips so the two can be unioned by name in marts.

    Green differs from yellow in four ways:
      - lpep_* timestamp prefix instead of tpep_*
      - has ehail_fee, lacks airport_fee
      - has trip_type (street-hail vs dispatch), yellow does not
      - no airport fee concept

    Columns absent from one side are cast NULL on the other rather than
    dropped, so the union keeps a single stable schema. See stg_yellow_trips
    for the timezone rationale.
*/

with source as (

    select * from {{ source('nyc_tlc', 'green_trips') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            "'green'",
            'vendorid',
            'lpep_pickup_datetime',
            'lpep_dropoff_datetime',
            'pulocationid',
            'dolocationid',
            'passenger_count',
            'trip_distance',
            'total_amount'
        ]) }}                                              as trip_key,

        'Green Taxi'                                       as tlc_type,

        -- identifiers
        cast(vendorid as bigint)                           as vendor_id,
        cast(ratecodeid as bigint)                         as rate_code_id,
        cast(payment_type as bigint)                       as payment_type,
        cast(pulocationid as bigint)                       as pickup_location_id,
        cast(dolocationid as bigint)                       as dropoff_location_id,
        cast(trip_type as bigint)                          as trip_type,

        -- timestamps
        to_utc_timestamp(
            lpep_pickup_datetime, 'America/New_York'
        )                                                  as pickup_at,
        to_utc_timestamp(
            lpep_dropoff_datetime, 'America/New_York'
        )                                                  as dropoff_at,
        cast(lpep_pickup_datetime as timestamp_ntz)        as pickup_at_local,
        cast(lpep_dropoff_datetime as timestamp_ntz)       as dropoff_at_local,

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

        -- green-only / yellow-only alignment
        cast(null as double)                               as airport_fee,
        cast(ehail_fee as double)                          as ehail_fee,

        coalesce(cast(cbd_congestion_fee as double), 0)    as cbd_congestion_fee,

        store_and_fwd_flag                                 as store_and_forward_flag,

        -- lineage
        period                                             as source_period,
        _loaded_from                                       as source_file,
        _loaded_at                                         as loaded_at

    from source

)

select * from renamed
