-- Run once, as a metastore admin.

create catalog if not exists nyc_transit
    comment 'NYC transportation lakehouse';

create schema if not exists nyc_transit.bronze
    comment 'Raw landed data. Append-only, schema-on-read, no business logic.';

create schema if not exists nyc_transit.staging
    comment 'dbt staging views. Renamed and typed, still one row per source row.';

create schema if not exists nyc_transit.marts
    comment 'Dimensional models. The only layer consumers should query.';
