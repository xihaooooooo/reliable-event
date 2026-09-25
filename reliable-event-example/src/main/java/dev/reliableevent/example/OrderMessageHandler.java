package dev.reliableevent.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

@Service
public class OrderMessageHandler {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public OrderMessageHandler(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = Objects.requireNonNull(jdbc);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Transactional
    public boolean handle(MessageView message) {
        Objects.requireNonNull(message, "message must not be null");
        Map<String, String> properties = message.getProperties();
        String type = properties.get("reliable_event_type");
        String key = properties.get("reliable_event_key");
        String id = properties.get("reliable_event_id");
        if (!OrderService.EVENT_TYPE.equals(type) || key == null || key.isBlank()
                || id == null || id.length() > 19 || !id.matches("[1-9][0-9]*")
                || !message.getKeys().contains(key)) {
            throw new IllegalArgumentException("Message has invalid reliable event identity");
        }
        try {
            Long.parseLong(id);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("Message has invalid reliable event id", failure);
        }

        OrderCreatedPayload payload;
        try {
            ByteBuffer body = message.getBody().asReadOnlyBuffer();
            byte[] bytes = new byte[body.remaining()];
            body.get(bytes);
            payload = mapper.readValue(bytes, OrderCreatedPayload.class);
        } catch (IOException failure) {
            throw new IllegalArgumentException("Message body is not an order-created payload", failure);
        }
        if (payload.orderId() <= 0 || !Long.toString(payload.orderId()).equals(key)
                || payload.itemCode() == null || payload.itemCode().isBlank()
                || payload.quantity() <= 0) {
            throw new IllegalArgumentException("Message payload does not match its event identity");
        }

        int inserted = jdbc.update("""
                INSERT IGNORE INTO example_consumed_event
                    (event_type, event_key, event_id, consumed_at)
                VALUES (?, ?, ?, UTC_TIMESTAMP(3))
                """, type, key, id);
        if (inserted == 0) {
            String originalId = jdbc.queryForObject("""
                    SELECT event_id FROM example_consumed_event
                    WHERE event_type = ? AND event_key = ?
                    """, String.class, type, key);
            if (!id.equals(originalId)) {
                throw new IllegalStateException("Duplicate business key has a different event id");
            }
            return false;
        }
        int effect = jdbc.update("""
                INSERT INTO example_order_effect (order_id, handled_count, handled_at)
                SELECT id, 1, UTC_TIMESTAMP(3)
                FROM example_order
                WHERE id = ? AND item_code = ? AND quantity = ?
                """, payload.orderId(), payload.itemCode(), payload.quantity());
        if (effect != 1) {
            throw new IllegalStateException("No matching order exists for the consumed event");
        }
        return true;
    }
}
