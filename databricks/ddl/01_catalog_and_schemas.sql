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

use catalog staging;
create schema if not exists nyc_transportation
    comment 'Staging schema for NYC transportation data before going to formal.';