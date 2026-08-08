{{
    config(
        materialized='table'
    )
}}

/*
    Vendor dimension with SCD Type 2 validity windows.

    Reads the snapshot and presents it in dimensional-model conventions:
    a surrogate key, explicit valid_from / valid_to, and an is_current flag.

    Consumers who want "the vendor as of the trip date" join on the validity
    window. Consumers who just want current names filter is_current = true.

    dbt_valid_to is NULL on the open row; coalescing to a far-future date
    makes BETWEEN-style joins work without special-casing NULL, which is a
    common source of silently dropped rows.
*/

with snapshotted as (

    select * from {{ ref('dim_vendor_snapshot') }}

)

select
    dbt_scd_id                                       as vendor_key,
    vendor_id,
    vendor_name,
    is_unmapped,
    dbt_valid_from                                   as valid_from,
    coalesce(dbt_valid_to, timestamp'9999-12-31')    as valid_to,
    dbt_valid_to is null                             as is_current
from snapshotted
