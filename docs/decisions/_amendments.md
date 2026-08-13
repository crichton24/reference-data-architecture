# Amendments needed to existing ADRs

Two of the original records predate the catalog rename (bronze -> raw) and the
DAG split. Neither decision changed, so these are wording corrections rather
than supersessions — edit in place.

## 0004 — Load raw with COPY INTO

- Replace "bronze" with "raw" throughout.
- In Consequences, the note that target tables must carry the partition and
  lineage columns is now handled by ADR 0014 (source-derived schemas). Replace
  it with a cross-reference.

## 0005 — Chain DAGs with assets, not sensors

- Replace "bronze" with "raw".
- Add to Consequences: asset URIs are defined centrally and use a
  project-specific scheme — see ADR 0011.
- The DAG names referenced have changed; see ADR 0012.

Delete this file once both are edited.
