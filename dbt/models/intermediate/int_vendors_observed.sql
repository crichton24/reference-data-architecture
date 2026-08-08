{{
    config(
        materialized='view'
    )
}}

/*
    Every vendor_id actually present in trip data, joined to the seeded name
    lookup.

    This is the input to the dim_vendor snapshot. It exists as its own model
    so the snapshot has a stable, testable source, and so "which vendors have
    we seen" is answerable without reading snapshot internals.

    A LEFT join, deliberately. An unrecognized vendor_id gets a placeholder
    name rather than being dropped — TLC has added vendors mid-year before
    (Myle in 2023, Helix in 2024), and silently losing those trips would be
    worse than carrying an ugly label until the seed is updated.

    first_seen_at / last_seen_at are observation facts, not vendor
    attributes. They are deliberately NOT in the snapshot's check_cols —
    last_seen_at changes on every run, which would create a new SCD version
    daily and make the history meaningless.
*/

with observed as (

    select
        vendor_id,
        min(pickup_at) as first_seen_at,
        max(pickup_at) as last_seen_at,
        count(*)       as trip_count
    from (
        select vendor_id, pickup_at from {{ ref('stg_yellow_trips') }}
        union all
        select vendor_id, pickup_at from {{ ref('stg_green_trips') }}
    )
    where vendor_id is not null
    group by vendor_id

),

named as (

    select
        observed.vendor_id,
        coalesce(
            lookup.vendor_name,
            concat('Unknown vendor (', cast(observed.vendor_id as string), ')')
        )                                                as vendor_name,
        lookup.vendor_name is null                       as is_unmapped,
        observed.first_seen_at,
        observed.last_seen_at,
        observed.trip_count
    from observed
    left join {{ ref('vendor_lookup') }} as lookup
        on observed.vendor_id = lookup.vendor_id

)

select * from named
