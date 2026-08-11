# Version 1.0 Local Recovery Gate

## Result

**Status:** Passed

**Execution date:** 2026-08-11

The recovery gate used only preserved, previously admitted local state. It did not publish events, run Spark, rebuild Gold, modify serving data, or contact AWS.

## Kafka restart recovery

| Check | Result |
|---|---:|
| First startup to healthy | 21.183 s |
| Clean Compose stop command | exit 0 |
| Container stop state | exited 143 (expected SIGTERM) |
| Restart to healthy | 8.798 s |
| Partitions / replication | 6 / 1 |
| End offsets before restart | 197542, 193213, 203062, 206059, 195013, 205220 |
| End offsets after restart | identical |
| Total preserved end offset | 1,200,109 |
| Final state | stopped; exited 143 |

The single-node broker preserved topic identity, six partitions, configuration, and every end offset. Exit 143 is Docker's expected SIGTERM result and is not a crash. This proves local persisted-state restart, not broker high availability.

## PostgreSQL/PostGIS restart recovery

| Check | Before restart | After restart |
|---|---:|---:|
| Startup to healthy | 7.321 s | 6.760 s |
| Serving rows | 1,000,000 | 1,000,000 |
| Unique event IDs | 1,000,000 | 1,000,000 |
| Unique detections | 10,000 | 10,000 |
| Replay rows | 1,000,000 | 1,000,000 |
| Synthetic rows | 0 | 0 |
| Daily rows / total | 1 / 1,000,000 | 1 / 1,000,000 |
| Lineage rows / total | 10,000 / 1,000,000 | 10,000 / 1,000,000 |
| Clean stop command | exit 0 | exit 0 |
| Final container state | stopped | stopped, exit 0 |

The successful load-control row remained:

- idempotency key / admitted Gold manifest SHA-256: `43ada13e40f14ffcdbd93d76702ee0d5918be7a666235a902375a860b491ffb9`;
- status: `SUCCEEDED`;
- manifest, staged, and inserted rows: 1,000,000 each;
- already-present rows on admitted first load: 0.

The prior identical-manifest rerun evidence remains authoritative: it returned the original load identity, inserted zero rows, and reconciled one million already-present rows. Recovery preserved that immutable idempotency record and all serving truth.

## Governed rerun boundaries

- Airflow integration already proves a same-logical-date rerun maps to the same orchestration identity and reuses seven successful stage receipts without incrementing their attempts.
- Orchestration recovery tests prove a failed stage is recorded, downstream execution is blocked, a bounded retry can succeed, and a repeated successful stage is an idempotent no-op.
- Kafka checkpoint recovery previously consumed zero new messages and retained exact counts and offsets.
- PostgreSQL identical-manifest reload previously inserted zero and retained its original load identity; the recovery gate confirmed that idempotency key persisted.

## Limitations

- Local Kafka is single-node KRaft and does not prove replication, failover, or availability.
- The PostgreSQL volume is local Docker state, not a backup. Governed Gold remains the rebuild authority.
- This recovery run did not repeat the expensive one-million loader or Spark job; it verified preserved truth and existing governed idempotency evidence.
- AWS backup/restore and teardown evidence are deferred to Version 1.1.
