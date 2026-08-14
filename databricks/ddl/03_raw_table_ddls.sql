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
use catalog raw;
use schema nyc_tlc;

CREATE TABLE IF NOT EXISTS raw.nyc_tlc.yellow_trips (
  VendorID INT COMMENT 'A code indicating the TPEP provider that provided the record. 1=Creative Mobile Technologies, LLC; 2=VeriFone Inc.',
  tpep_pickup_datetime TIMESTAMP_NTZ COMMENT 'The date and time when the meter was engaged in EST',
  tpep_dropoff_datetime TIMESTAMP_NTZ COMMENT 'The date and time when the meter was disengaged in EST',
  passenger_count BIGINT COMMENT 'The number of passengers in the vehicle. This is a driver-entered value',
  trip_distance DOUBLE COMMENT 'The elapsed trip distance in miles reported by the taximeter',
  RatecodeID BIGINT COMMENT 'The final rate code in effect at the end of the trip. 1=Standard rate; 2=JFK; 3=Newark; 4=Nassau or Westchester; 5=Negotiated fare; 6=Group ride',
  store_and_fwd_flag STRING COLLATE UTF8_BINARY COMMENT 'This flag indicates whether the trip record was held in vehicle memory before sending to the vendor (Y=store and forward trip; N=not a store and forward trip)',
  PULocationID INT COMMENT 'TLC Taxi Zone in which the taximeter was engaged',
  DOLocationID INT COMMENT 'TLC Taxi Zone in which the taximeter was disengaged',
  payment_type BIGINT COMMENT 'A numeric code signifying how the passenger paid for the trip. 1=Credit card; 2=Cash; 3=No charge; 4=Dispute; 5=Unknown; 6=Voided trip',
  fare_amount DOUBLE COMMENT 'The time-and-distance fare calculated by the meter',
  extra DOUBLE COMMENT 'Miscellaneous extras and surcharges. Currently, this only includes the 0.50USD and 1USD rush hour and overnight charges',
  mta_tax DOUBLE COMMENT 'MTA tax that is automatically triggered based on the metered rate in use',
  tip_amount DOUBLE COMMENT 'Tip amount. This field is automatically populated for credit card tips. Cash tips are not included',
  tolls_amount DOUBLE COMMENT 'Total amount of all tolls paid in trip',
  improvement_surcharge DOUBLE COMMENT 'Improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015',
  total_amount DOUBLE COMMENT 'The total amount charged to passengers. Does not include cash tips',
  congestion_surcharge DOUBLE COMMENT 'Total amount collected in trip for NYS congestion surcharge',
  airport_fee DOUBLE COMMENT 'Airport fee for pick-ups only at LaGuardia and John F. Kennedy Airports',

--METADATA
  period string COMMENT 'Inferred from the dataset=/period= S3 path by Hive-style partition discovery',
  _loaded_from STRING COLLATE UTF8_BINARY COMMENT 'The name of the file the record was ingested from',
  _loaded_at TIMESTAMP DEFAULT current_timestamp() COMMENT 'The timestamp when the record was ingested into the table',
  _loaded_by STRING COLLATE UTF8_BINARY DEFAULT current_user() COMMENT 'The name of the user who ingested the record into the table',
  _source_modified_at TIMESTAMP COMMENT 'The timestamp when the source file was last modified'
)
USING delta
COMMENT 'NYC TLC Yellow Taxi trip records containing pickup/dropoff dates/times, locations, distances, fares, payment types, and passenger counts'
tblproperties (
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.feature.timestampNtz' = 'supported'
);

-- Apply table-level governed tags
ALTER TABLE yellow_trips SET TAGS (
  'businessLegalRecord' = 'BUSINESS',
  'informationClassification' = 'PUBLIC',
  'complmianceClassification' = 'NONE',
  'dataDurationClassification' = 'PERMENANT',
  'updateType' = 'AUTOMATIC',
  'legalHold' = 'NO',
  'dataSource' = 'NYC TAXI AND LIMOUSINE COMMISSION',
  'dataOwner' = 'KTF1234',
  'dataPrimacy' = 'REPLICA'
);

CREATE TABLE IF NOT EXISTS green_trips (
    vendorid                int COMMENT 'A code indicating the LPEP provider that provided the record. 1=Creative Mobile Technologies, LLC; 2=VeriFone Inc.',
    lpep_pickup_datetime    TIMESTAMP_NTZ COMMENT 'The date and time when the meter was engaged in EST',
    lpep_dropoff_datetime   TIMESTAMP_NTZ COMMENT 'The date and time when the meter was disengaged in EST',
    store_and_fwd_flag      string COMMENT 'This flag indicates whether the trip record was held in vehicle memory before sending to the vendor (Y=store and forward trip; N=not a store and forward trip)',
    ratecodeid              bigint COMMENT 'The final rate code in effect at the end of the trip. 1=Standard rate; 2=JFK; 3=Newark; 4=Nassau or Westchester; 5=Negotiated fare; 6=Group ride',
    pulocationid            int COMMENT 'TLC Taxi Zone in which the taximeter was engaged',
    dolocationid            int COMMENT 'TLC Taxi Zone in which the taximeter was disengaged',
    passenger_count         bigint COMMENT 'The number of passengers in the vehicle. This is a driver-entered value',
    trip_distance           double COMMENT 'The elapsed trip distance in miles reported by the taximeter',
    fare_amount             double COMMENT 'The time-and-distance fare calculated by the meter',
    extra                   double COMMENT 'Miscellaneous extras and surcharges. Currently, this only includes the 0.50USD and 1USD rush hour and overnight charges',
    mta_tax                 double COMMENT 'MTA tax that is automatically triggered based on the metered rate in use',
    tip_amount              double COMMENT 'Tip amount. This field is automatically populated for credit card tips. Cash tips are not included',
    tolls_amount            double COMMENT 'Total amount of all tolls paid in trip',
    ehail_fee               double COMMENT 'Remote dispatch request flag; 1=trip initiated through e-hail, null=other',
    improvement_surcharge   double COMMENT 'Improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015',
    total_amount            double COMMENT 'The total amount charged to passengers. Does not include cash tips',
    payment_type            bigint COMMENT 'A numeric code signifying how the passenger paid for the trip. 1=Credit card; 2=Cash; 3=No charge; 4=Dispute; 5=Unknown; 6=Voided trip',
    trip_type               bigint COMMENT 'A code indicating whether the trip was a street-hail or a dispatch trip. 1=Street-hail; 2=Dispatch',
    congestion_surcharge    double COMMENT 'Total amount collected in trip for NYS congestion surcharge',
    cbd_congestion_fee      double COMMENT 'CBD congestion surcharge collected during trip',

  --METADATA
    period string COMMENT 'Inferred from the dataset=/period= S3 path by Hive-style partition discovery',
    _loaded_from STRING COLLATE UTF8_BINARY COMMENT 'The name of the file the record was ingested from',
    _loaded_at TIMESTAMP DEFAULT current_timestamp() COMMENT 'The timestamp when the record was ingested into the table',
    _loaded_by STRING COLLATE UTF8_BINARY DEFAULT current_user() COMMENT 'The name of the user who ingested the record into the table',
    _source_modified_at TIMESTAMP COMMENT 'The timestamp when the source file was last modified'
)
using delta
comment 'NYC TLC Green Taxi trip records containing pickup/dropoff dates/times, locations, distances, fares, payment types, and passenger counts'
tblproperties (
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.feature.timestampNtz' = 'supported'
);

-- Apply table-level governed tags
ALTER TABLE yellow_trips SET TAGS (
  'businessLegalRecord' = 'BUSINESS',
  'informationClassification' = 'PUBLIC',
  'complmianceClassification' = 'NONE',
  'dataDurationClassification' = 'PERMENANT',
  'updateType' = 'AUTOMATIC',
  'legalHold' = 'NO',
  'dataSource' = 'NYC TAXI AND LIMOUSINE COMMISSION',
  'dataOwner' = 'KTF1234',
  'dataPrimacy' = 'REPLICA'
);
