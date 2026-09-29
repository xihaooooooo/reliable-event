-- Run once on existing M6 tables before enabling published-event cleanup.
ALTER TABLE reliable_event_outbox
    ADD INDEX idx_published_retention (status, published_at, id);
