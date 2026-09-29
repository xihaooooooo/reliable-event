-- Apply once to an existing Outbox table before using M6.1 dead-event paging.
ALTER TABLE reliable_event_outbox
    ADD INDEX idx_dead_list (status, id);
