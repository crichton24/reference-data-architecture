-- Bronze targets for the nyc_tlc_load_bronze DAG.
--
-- Three column groups beyond TLC's own schema:
--   period      -- inferred from the dataset=/period= S3 path by Hive-style
--                  partition discovery. Required, or COPY INTO fails.
--   _source_*   -- lineage, captured from Databricks' hidden _metadata column
--   _loaded_at  -- feeds dbt source freshness
--
-- Deliberately not partitioned. At this volume Delta's file statistics handle
-- pruning better than physical partitioning, which would just produce many
-- small files.

create table if not exists nyc_transit.bronze.yellow_trips (
    vendorid                int,
    tpep_pickup_datetime    timestamp,
    tpep_dropoff_datetime   timestamp,
    passenger_count         bigint,
    trip_distance           double,
    ratecodeid              bigint,
    store_and_fwd_flag      string,
    pulocationid            int,
    dolocationid            int,
    payment_type            bigint,
    fare_amount             double,
    extra                   double,
    mta_tax                 double,
    tip_amount              double,
    tolls_amount            double,
    improvement_surcharge   double,
    total_amount            double,
    congestion_surcharge    double,
    airport_fee             double,
    cbd_congestion_fee      double,

    period                  string,
    _source_file            string,
    _source_modified_at     timestamp,
    _loaded_at              timestamp
)
using delta
comment 'Raw yellow taxi trips. Append-only, loaded via COPY INTO.'
tblproperties (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion'   = '2',
    'delta.minWriterVersion'   = '5'
);

create table if not exists nyc_transit.bronze.green_trips (
    vendorid                int,
    lpep_pickup_datetime    timestamp,
    lpep_dropoff_datetime   timestamp,
    store_and_fwd_flag      string,
    ratecodeid              bigint,
    pulocationid            int,
    dolocationid            int,
    passenger_count         bigint,
    trip_distance           double,
    fare_amount             double,
    extra                   double,
    mta_tax                 double,
    tip_amount              double,
    tolls_amount            double,
    ehail_fee               double,
    improvement_surcharge   double,
    total_amount            double,
    payment_type            bigint,
    trip_type               bigint,
    congestion_surcharge    double,
    cbd_congestion_fee      double,

    period                  string,
    _source_file            string,
    _source_modified_at     timestamp,
    _loaded_at              timestamp
)
using delta
comment 'Raw green taxi trips. Append-only, loaded via COPY INTO.'
tblproperties (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion'   = '2',
    'delta.minWriterVersion'   = '5'
);
