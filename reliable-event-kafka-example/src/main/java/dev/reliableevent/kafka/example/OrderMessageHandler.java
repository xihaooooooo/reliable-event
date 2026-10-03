package dev.reliableevent.kafka.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Example business handler; returning means Spring committed the business effect and dedupe row. */
@Service
public class OrderMessageHandler {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public OrderMessageHandler(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = Objects.requireNonNull(jdbc);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Transactional
    public boolean handle(ConsumerRecord<String, byte[]> record) {
        Objects.requireNonNull(record, "record must not be null");
        String type = header(record, "reliable_event_type");
        String key = header(record, "reliable_event_key");
        String id = header(record, "reliable_event_id");
        if (!OrderService.EVENT_TYPE.equals(type) || key == null || key.isBlank()
                || id == null || id.length() > 19 || !id.matches("[1-9][0-9]*")
                || !key.equals(record.key())) {
            throw new IllegalArgumentException("Message has invalid reliable event identity");
        }
        try { Long.parseLong(id); }
        catch (NumberFormatException failure) {
            throw new IllegalArgumentException("Message has invalid reliable event id", failure);
        }
        OrderCreatedPayload payload;
        try { payload = mapper.readValue(record.value(), OrderCreatedPayload.class); }
        catch (IOException failure) {
            throw new IllegalArgumentException("Message body is not an order-created payload", failure);
        }
        if (payload.orderId() <= 0 || !Long.toString(payload.orderId()).equals(key)
                || payload.itemCode() == null || !payload.itemCode().matches("[A-Za-z0-9_-]{1,64}")
                || payload.quantity() <= 0 || payload.quantity() > 10_000) {
            throw new IllegalArgumentException("Message payload does not match its event identity");
        }

        int inserted = jdbc.update("""
                INSERT IGNORE INTO example_consumed_event (event_type, event_key, event_id, consumed_at)
                VALUES (?, ?, ?, UTC_TIMESTAMP(3))
                """, type, key, id);
        if (inserted == 0) {
            String originalId = jdbc.queryForObject("""
                    SELECT event_id FROM example_consumed_event WHERE event_type = ? AND event_key = ?
                    """, String.class, type, key);
            if (!id.equals(originalId)) {
                throw new IllegalStateException("Duplicate business key has a different event id");
            }
            return false;
        }
        int effect = jdbc.update("""
                INSERT INTO example_order_effect (order_id, handled_count, handled_at)
                SELECT id, 1, UTC_TIMESTAMP(3) FROM example_order
                WHERE id = ? AND item_code = ? AND quantity = ?
                """, payload.orderId(), payload.itemCode(), payload.quantity());
        if (effect != 1) throw new IllegalStateException("No matching order exists for the consumed event");
        return true;
    }

    private static String header(ConsumerRecord<String, byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
