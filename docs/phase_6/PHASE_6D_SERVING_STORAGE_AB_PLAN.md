# Phase 6D - Serving Storage A/B Gate Plan

## Status

Approved for a bounded 100,000-row local comparison. No one-million-row work is authorized.

## Objective

Determine whether the full canonical JSON payload belongs in the PostgreSQL hot serving table after the Phase 6C gate measured substantial TOAST and WAL amplification.

## Compared Layouts

| Layout | Definition |
|---|---|
| A - full payload | Existing `serving.event_detail`, containing materialized serving fields plus the duplicated full `event_payload` JSONB |
| B - compact | `serving.event_detail_compact`, containing every materialized column, geometry, constraint, index, hash, Gold run ID, and raw lineage field except `event_payload` |

Authoritative Gold Parquet and its checksum manifest retain the complete governed event. Compact PostgreSQL remains rebuildable and conflict-safe through `event_id`, `governed_content_hash`, `gold_run_id`, source lineage, and Gold artifact lineage.

## Controlled Method

1. Preserve the passed 100,000-row full-payload table as control A.
2. Create compact table B from the governed schema using the same column types, defaults, checks, and indexes, then add the required reference foreign keys and read-only grants.
3. Materialize B from A in one transaction and run `ANALYZE`.
4. Prove an identical second materialization inserts zero rows.
5. Verify 100,000 row-by-row records are equal after excluding only `event_payload`.
6. Verify exact replay truth, spatial integrity, hashes, Gold lineage, source lineage, and constraints.
7. Run the same summary, lineage, spatial, and daily query workload against both layouts.
8. Record relation, heap, index, and TOAST sizes for both tables.
9. Execute a rolled-back differing-hash conflict probe against B.
10. Stop PostgreSQL and preserve the volume.

## Fairness Boundary

This phase measures projection materialization, storage, and read performance. It does not claim that `INSERT ... SELECT` from A is equivalent to parsing and bulk-loading the original 226.75-MB JSONL artifact. If B is selected, the production loader must be changed and independently gated before one-million-row execution.

## Selection Criteria

B may replace A only if:

- all 100,000 rows reconcile and differ only by absence of `event_payload`;
- all truth, spatial, lineage, constraint, role, idempotency, and conflict checks pass;
- Gold remains the authoritative complete payload store;
- total relation and TOAST storage decrease materially;
- representative query p95 does not regress by more than 20% without a defensible plan change;
- limitations and operational tradeoffs are recorded.

## Stop Conditions

Stop without selecting B if any row differs, a required constraint/index/grant is missing, geometry or lineage fails, idempotency fails, memory exceeds 2 GiB, or the database becomes unhealthy.
