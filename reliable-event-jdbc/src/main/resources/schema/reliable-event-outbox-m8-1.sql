-- Apply once before upgrading to M8. Status 4 remains terminal but is now named FAILED.
ALTER TABLE reliable_event_outbox
    ADD COLUMN fallback_stop_reason VARCHAR(32) NULL,
    ADD COLUMN fallback_failure_classification VARCHAR(32) NULL;

-- Existing status 4 rows retain their code and now mean FAILED. Existing replay audit tables
-- are application data and are intentionally not dropped by this migration.
