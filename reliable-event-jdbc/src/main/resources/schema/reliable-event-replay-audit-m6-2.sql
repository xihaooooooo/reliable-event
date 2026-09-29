-- Create once before using M6.2 replay. The library does not run migrations.
CREATE TABLE IF NOT EXISTS reliable_event_replay_audit (
    id                      BIGINT NOT NULL AUTO_INCREMENT,
    event_id                BIGINT NOT NULL,
    previous_version        BIGINT NOT NULL,
    new_version             BIGINT NOT NULL,
    previous_attempt_count  INT NOT NULL,
    previous_max_attempts   INT NOT NULL,
    previous_last_error     VARCHAR(1024) NULL,
    operator_id             VARCHAR(128) NOT NULL,
    reason                  VARCHAR(1024) NOT NULL,
    replayed_at             DATETIME(3) NOT NULL,
    result                  VARCHAR(16) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_replay_event_version (event_id, previous_version),
    KEY idx_replay_event (event_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
