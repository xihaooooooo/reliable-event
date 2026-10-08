-- Safe to repeat while event registration and published-event cleanup are paused.
-- Existing published Outbox rows provide the authoritative success timestamp.
UPDATE reliable_event_identity i
INNER JOIN reliable_event_outbox o
    ON i.id = o.id AND i.event_type = o.event_type AND i.event_key = o.event_key
SET i.published_at = o.published_at
WHERE i.published_at IS NULL
  AND o.status = 2
  AND o.published_at IS NOT NULL;

-- Legacy M7 cleanup removed the Outbox row but retained its identity. There is
-- no original success timestamp to recover, so start its full retention window
-- at this migration time. Run only after confirming orphan identities are all
-- expected results of the old PUBLISHED cleanup.
UPDATE reliable_event_identity i
SET i.published_at = UTC_TIMESTAMP(3)
WHERE i.published_at IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM reliable_event_outbox o WHERE o.id = i.id
  )
  AND NOT EXISTS (
      SELECT 1 FROM reliable_event_outbox o
      WHERE o.event_type = i.event_type AND o.event_key = i.event_key
  );
