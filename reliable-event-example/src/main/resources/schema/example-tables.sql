CREATE TABLE IF NOT EXISTS example_order (
    id BIGINT NOT NULL AUTO_INCREMENT,
    item_code VARCHAR(64) NOT NULL,
    quantity INT NOT NULL,
    created_at DATETIME(3) NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS example_consumed_event (
    event_type VARCHAR(128) NOT NULL,
    event_key VARCHAR(192) NOT NULL,
    event_id VARCHAR(32) NOT NULL,
    consumed_at DATETIME(3) NOT NULL,
    PRIMARY KEY (event_type, event_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS example_order_effect (
    order_id BIGINT NOT NULL,
    handled_count INT NOT NULL,
    handled_at DATETIME(3) NOT NULL,
    PRIMARY KEY (order_id),
    CONSTRAINT fk_example_effect_order FOREIGN KEY (order_id) REFERENCES example_order(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
