package dev.reliableevent.example;

import dev.reliableevent.EventId;
import dev.reliableevent.ReliableEvent;
import dev.reliableevent.ReliableEventPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Statement;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;

@Service
public class OrderService {

    public static final String EVENT_TYPE = "order-created";

    private final JdbcTemplate jdbc;
    private final ReliableEventPublisher publisher;
    private final Clock clock;

    public OrderService(JdbcTemplate jdbc, ReliableEventPublisher publisher, Clock clock) {
        this.jdbc = Objects.requireNonNull(jdbc);
        this.publisher = Objects.requireNonNull(publisher);
        this.clock = Objects.requireNonNull(clock);
    }

    @Transactional
    public CreatedOrder create(String itemCode, int quantity) {
        if (itemCode == null || !itemCode.matches("[A-Za-z0-9_-]{1,64}")) {
            throw new IllegalArgumentException("itemCode must use 1-64 letters, digits, _ or -");
        }
        if (quantity <= 0 || quantity > 10_000) {
            throw new IllegalArgumentException("quantity must be between 1 and 10000");
        }

        var key = new GeneratedKeyHolder();
        jdbc.update(connection -> {
            var statement = connection.prepareStatement(
                    "INSERT INTO example_order (item_code, quantity, created_at) VALUES (?, ?, UTC_TIMESTAMP(3))",
                    Statement.RETURN_GENERATED_KEYS
            );
            statement.setString(1, itemCode);
            statement.setInt(2, quantity);
            return statement;
        }, key);
        Number generated = key.getKey();
        if (generated == null) {
            throw new IllegalStateException("Order insert did not return an id");
        }
        long orderId = generated.longValue();
        EventId eventId = publisher.publish(new ReliableEvent<>(
                EVENT_TYPE,
                Long.toString(orderId),
                new OrderCreatedPayload(orderId, itemCode, quantity),
                clock.instant(),
                Map.of("source", "reliable-event-example")
        ));
        return new CreatedOrder(orderId, eventId.value());
    }

    public record CreatedOrder(long orderId, long eventId) { }
}
