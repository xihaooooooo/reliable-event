-- Run once with event registration and published-event cleanup paused.
ALTER TABLE reliable_event_identity
    ADD COLUMN published_at DATETIME(3) NULL AFTER registered_at,
    ADD INDEX idx_identity_retention (published_at, id);
