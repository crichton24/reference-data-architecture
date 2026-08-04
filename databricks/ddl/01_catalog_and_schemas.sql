-- Run once, as a metastore admin.
use catalog raw;
create schema if not exists nyc_tlc
    comment 'Raw landed data. Append-only, schema-on-read, no business logic.';

-- Apply governed tags to the schema
ALTER SCHEMA nyc_tlc SET TAGS (
    'informationClassification' = 'PUBLIC',
    'complianceClassification' = 'NONE',
    'dataDurationClassification' = 'PERMENANT',
    'updateType' = 'AUTOMATIC',
    'legalHold' = 'NO',
    'dataPrimacy' = 'REPLICA'
);
/*
--GENERATED ... NOT NEEDED
create schema if not exists nyc_transit.staging
    comment 'dbt staging views. Renamed and typed, still one row per source row.';

create schema if not exists nyc_transit.marts
    comment 'Dimensional models. The only layer consumers should query.';
*/