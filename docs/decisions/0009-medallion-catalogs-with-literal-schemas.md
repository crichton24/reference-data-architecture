# 0009. Medallion catalogs with literal schema names

Date: 2026-08-12
Status: Accepted

## Context

Unity Catalog has a three-level namespace: catalog, schema, table. The
platform has three logical layers, and datasets within the top layer serve
different domains.

dbt's default behavior concatenates a model's custom schema onto the profile's
schema, producing names like `nyc_transportation_customer`.

## Decision

Encode the medallion layer as the **catalog**, and the domain as the
**schema**:

    raw.nyc_tlc.yellow_trips
    raw.nyc_tlc.green_trips
    raw.nyc_tlc.taxi_zone_lookup
    transform.nyc_transportation.stg_*
    formal.customer.nyc_trips
    formal.common.dim_vendor

Override `generate_schema_name` so custom schema names are used verbatim
rather than concatenated.

## Alternatives considered

**One catalog, layer as schema prefix** (`lakehouse.raw_nyc_tlc`). Rejected —
Unity Catalog grants are per-catalog, so layer-as-catalog makes
"analysts read `formal`, nothing else" a single grant.

**dbt's default concatenation.** Rejected as unreadable at the point of use.
`formal.customer.nyc_trips` states the layer and domain plainly;
`nyc_transportation_customer` states neither.

## Consequences

Governance is simple: `formal` is grantable to consumers as a unit, `raw` and
`transform` are not.

The `generate_schema_name` override is non-obvious. A macro-name typo means
dbt silently falls back to its built-in version and concatenation resumes with
no error. Verify with:

    dbt ls --output json --output-keys name relation_name

Catalogs must be created before dbt runs. dbt creates schemas; it does not
create catalogs.
