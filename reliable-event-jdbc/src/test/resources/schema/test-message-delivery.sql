CREATE TABLE IF NOT EXISTS test_message_delivery (
    id            BIGINT NOT NULL AUTO_INCREMENT,
    event_id      BIGINT NOT NULL,
    event_key     VARCHAR(192) NOT NULL,
    worker_id     VARCHAR(128) NOT NULL,
    message_id    VARCHAR(192) NOT NULL,
    delivered_at  DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_test_message_id (message_id),
    KEY idx_test_delivery_event (event_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
