{% snapshot dim_vendor_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='vendor_id',
        strategy='check',
        check_cols=['vendor_name'],
        invalidate_hard_deletes=false
    )
}}

/*
    SCD Type 2 history for vendors.

    HOW THIS SATISFIES "look for new vendor IDs and add them"
    dbt snapshots are insert-and-expire, not replace. On each run:
      - a vendor_id not previously seen is INSERTED with a new dbt_valid_from
      - a vendor_id whose vendor_name changed gets its current row EXPIRED
        (dbt_valid_to set) and a new row inserted
      - unchanged rows are left alone

    So new vendors appear automatically, and renames — Verifone becoming
    Curb Mobility, for instance — are preserved as history rather than
    overwriting the past.

    WHY strategy='check' AND NOT 'timestamp'
    A timestamp strategy needs a reliable updated_at column on the source.
    Vendors have no such thing; the source is a derived lookup. The check
    strategy compares the named columns and detects change directly.

    WHY check_cols IS ONLY vendor_name
    It's the only genuine slowly-changing attribute. Including trip_count or
    last_seen_at would create a new version on every single run, turning the
    history into noise and growing the table without bound.

    invalidate_hard_deletes=false: a vendor disappearing from recent trip
    data doesn't mean the vendor ceased to exist, and historical trips still
    reference it. Expiring the row would break those joins.
*/

select
    vendor_id,
    vendor_name,
    is_unmapped
from {{ ref('int_vendors_observed') }}

{% endsnapshot %}
