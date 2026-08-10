{{
    config(
        materialized='incremental',
        unique_key='trip_key',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        database='formal',
        schema='customer',
        tblproperties={'delta.feature.allowColumnDefaults': 'supported'}
    )
}}

/*
    Unified yellow + green trips with descriptive attributes resolved.

    MATERIALIZATION
    Incremental with a merge strategy. A full rebuild across every month is
    expensive and unnecessary — most runs add one new period. The merge on
    trip_key also makes a re-run of an already-loaded period a no-op rather
    than a duplication, which matters because bronze is append-only and a
    restated TLC file can reintroduce rows we've already seen.

    partition_by source_period aligns the physical layout with how the data
    arrives and how it's almost always filtered.

    JOIN STRATEGY
    All four lookups are LEFT joins with coalesced fallbacks. A trip with an
    unrecognized code stays in the fact table with a labelled 'Unknown (n)'
    description. Dropping trips because a lookup is stale would silently
    understate volume — the worst kind of data quality bug, because the
    numbers still look plausible.

    dim_vendor is joined on is_current rather than the validity window. That's
    a deliberate simplification: it gives every trip the vendor's CURRENT
    name. For point-in-time naming, join on
        trip.pickup_at between vendor.valid_from and vendor.valid_to
    instead. Current-name is what most analysis actually wants; the snapshot
    preserves the history either way.
*/

with trips as (
    select * from {{ ref('stg_yellow_trips') }}
    union all
    select * from {{ ref('stg_green_trips') }}

),

filtered as (

    select * from trips

    {% if is_incremental() %}
        -- Only reprocess periods newer than what's already landed. Uses the
        -- partition column so Databricks can prune, rather than scanning
        -- every row to compare timestamps.
        where source_period >= (
            select coalesce(max(source_period), '1900-01')
            from {{ this }}
        )
    {% endif %}

),

joined as (

    select
        -- keys
        filtered.trip_key,
        filtered.tlc_type,

        -- vendor
        filtered.vendor_id,
        coalesce(
            vendor.vendor_name,
            concat('Unknown vendor (', cast(filtered.vendor_id as string), ')')
        )                                                as vendor_name,

        -- rate code
        filtered.rate_code_id,
        coalesce(
            rate_code.rate_code_description,
            concat('Unknown rate code (', cast(filtered.rate_code_id as string), ')')
        )                                                as rate_code_description,

        -- payment
        filtered.payment_type,
        coalesce(
            payment.payment_type_description,
            concat('Unknown payment type (', cast(filtered.payment_type as string), ')')
        )                                                as payment_type_description,

        -- trip type (green only; NULL for yellow is meaningful, not missing)
        filtered.trip_type,
        coalesce(
            trip_type_lookup.trip_type_description,
            concat('Unknown trip type (', cast(filtered.trip_type as string), ')')
        )                                                as trip_type_description,

        -- locations
        filtered.pickup_location_id,
        filtered.dropoff_location_id,

        -- timestamps: instant and local wall clock
        filtered.pickup_at,
        filtered.dropoff_at,
        filtered.pickup_at_local,
        filtered.dropoff_at_local,

        -- derived time attributes, from LOCAL time. "Rush hour" means 5pm in
        -- New York, not 5pm UTC.
        date(filtered.pickup_at_local)                   as pickup_date,
        hour(filtered.pickup_at_local)                   as pickup_hour,
        dayofweek(filtered.pickup_at_local)              as pickup_day_of_week,

        -- duration from the instants, so DST transitions don't produce
        -- negative or inflated durations
        cast(
            (unix_timestamp(filtered.dropoff_at) - unix_timestamp(filtered.pickup_at))
            / 60.0 as double
        )                                                as trip_duration_minutes,

        -- measures
        filtered.passenger_count,
        filtered.trip_distance_miles,
        filtered.fare_amount,
        filtered.extra_amount,
        filtered.mta_tax_amount,
        filtered.tip_amount,
        filtered.tolls_amount,
        filtered.improvement_surcharge,
        filtered.congestion_surcharge,
        filtered.cbd_congestion_fee,
        filtered.airport_fee,
        filtered.ehail_fee,
        filtered.total_amount,

        filtered.store_and_forward_flag,

        -- lineage
        filtered.source_period,
        filtered.source_file,
        filtered.loaded_at,
        cast(current_timestamp() as timestamp)                              as updated_at,
        cast(current_user() as string)                                      as updated_by

    from filtered

    left join {{ ref('dim_vendor') }} as vendor
        on filtered.vendor_id = vendor.vendor_id
        and vendor.is_current

    left join {{ ref('rate_code_lookup') }} as rate_code
        on filtered.rate_code_id = rate_code.rate_code_id

    left join {{ ref('payment_type_lookup') }} as payment
        on filtered.payment_type = payment.payment_type

    left join {{ ref('trip_type_lookup') }} as trip_type_lookup
        on filtered.trip_type = trip_type_lookup.trip_type

)

select * from joined
