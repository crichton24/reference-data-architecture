"""
Shared asset definitions.

Every DAG imports its assets from here rather than declaring its own.

Airflow matches assets by EXACT STRING. A producer emitting
"databricks://raw/nyc_tlc/yellow_trips" and a consumer listening for
"nyc-transit://raw/trips" is not an error — the consumer simply never
runs, silently, with nothing in any log to explain why.

Defining them once makes producer and consumer incapable of drifting.
"""

from __future__ import annotations

from airflow.sdk import Asset

# --- storage ---------------------------------------------------------------
BUCKET = "nyc-tlc-raw-data-105803061132-us-east-2-an"
LANDING_PREFIX = "nyc-tlc"

# --- landing: emitted by nyc_tlc_ingest ------------------------------------
LANDING_ASSET = Asset(f"s3://{BUCKET}/{LANDING_PREFIX}/")

# --- raw: emitted by nyc_tlc_load_raw --------------------------------
RAW_YELLOW_ASSET = Asset("nyc-transit://raw/nyc_tlc/yellow_trips")
RAW_GREEN_ASSET = Asset("nyc-transit://raw/nyc_tlc/green_trips")
RAW_ALL_ASSETS = [RAW_YELLOW_ASSET, RAW_GREEN_ASSET]

# --- reference: emitted by nyc_taxi_zone_lookup ----------------------------
ZONE_LOOKUP_ASSET = Asset("nyc-transit://raw/nyc_tlc/taxi_zone_lookup")

# --- reference: emitted by nyc_taxi_zone_source_to_s3 ---
ZONE_LANDED_ASSET = Asset(f"s3://{BUCKET}/{LANDING_PREFIX}/reference/taxi_zone_lookup/")

# --- formal: emitted by nyc_tlc_transform ----------------------------------
FORMAL_NYC_TRIPS_ASSET = Asset("nyc-transit://formal/customer/nyc_trips")
FORMAL_DIM_VENDOR_ASSET = Asset("nyc-transit://formal/common/dim_vendor")
FORMAL_ALL_ASSETS = [FORMAL_NYC_TRIPS_ASSET, FORMAL_DIM_VENDOR_ASSET]
