-- Run once, as a metastore admin.
-- Create a catalog for raw data, and a schema for NYC TLC data.
use catalog raw;
create schema if not exists nyc_tlc
    comment 'Raw landed data. Append-only, schema-on-read, no business logic.';

-- Apply governed tags to the schema
alter schema nyc_tlc set tags (
    'informationClassification' = 'PUBLIC',
    'complianceClassification' = 'NONE',
    'dataDurationClassification' = 'PERMENANT',
    'updateType' = 'AUTOMATIC',
    'legalHold' = 'NO',
    'dataPrimacy' = 'REPLICA'
);

use catalog transform;
create schema if not exists nyc_transportation
    comment 'Staging schema for NYC transportation data before going to formal.';
