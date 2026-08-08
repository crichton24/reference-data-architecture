{{
    config(
        materialized='view'
    )
}}

/*
    Zone reference, landed by the nyc_taxi_zone_lookup Airflow DAG.

    TLC ships two sentinel zones (264 'Unknown', 265 'N/A') which are
    legitimate values in trip data, not errors. They're kept and flagged
    so downstream joins don't silently drop those trips.
*/

with source as (

    select * from {{ source('nyc_tlc', 'taxi_zone_lookup') }}

)

select
    cast(locationid as bigint)                as location_id,
    borough,
    zone                                      as zone_name,
    service_zone,
    case
        when cast(locationid as bigint) in (264, 265) then true
        else false
    end                                       as is_unknown_zone,
    _loaded_at                                as loaded_at
from source
