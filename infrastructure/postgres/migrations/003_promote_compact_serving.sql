-- Promote the measured compact serving projection selected by the Phase 6D A/B gate.
-- Gold remains the immutable recovery source; PostgreSQL stores query-serving fields only.

ALTER TABLE serving.event_detail
  DROP COLUMN IF EXISTS event_payload;

COMMENT ON TABLE serving.event_detail IS
  'Compact query-serving projection rebuilt from a checksum-validated governed Gold manifest.';
