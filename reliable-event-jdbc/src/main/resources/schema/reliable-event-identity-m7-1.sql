-- Run once with event registration paused, before deploying M7 writers.
-- Existing explicit IDs advance InnoDB's AUTO_INCREMENT beyond the largest old ID.
CREATE TABLE IF NOT EXISTS reliable_event_identity (
    id            BIGINT NOT NULL AUTO_INCREMENT,
    event_type    VARCHAR(128) NOT NULL,
    event_key     VARCHAR(192) NOT NULL,
    registered_at DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_event_identity (event_type, event_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO reliable_event_identity (id, event_type, event_key, registered_at)
SELECT id, event_type, event_key, created_at
FROM reliable_event_outbox;
