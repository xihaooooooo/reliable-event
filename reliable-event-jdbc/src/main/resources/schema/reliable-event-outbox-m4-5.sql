-- Apply once to an existing M4.4 table before starting M4.5 code.
-- Historical first availability cannot be reconstructed after retries.
ALTER TABLE reliable_event_outbox
    ADD COLUMN first_available_at DATETIME(3) NULL AFTER next_attempt_at;
