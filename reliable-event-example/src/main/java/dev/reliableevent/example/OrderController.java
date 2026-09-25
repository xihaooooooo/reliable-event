package dev.reliableevent.example;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Objects;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService service;
    private final JdbcTemplate jdbc;

    public OrderController(OrderService service, JdbcTemplate jdbc) {
        this.service = Objects.requireNonNull(service);
        this.jdbc = Objects.requireNonNull(jdbc);
    }

    @PostMapping
    public ResponseEntity<OrderService.CreatedOrder> create(@RequestBody CreateOrder request) {
        if (request == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Request body is required");
        }
        try {
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(service.create(request.itemCode(), request.quantity()));
        } catch (IllegalArgumentException invalid) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, invalid.getMessage(), invalid);
        }
    }

    @GetMapping("/{id}")
    public OrderView get(@PathVariable long id) {
        return jdbc.query("""
                SELECT o.id, o.item_code, o.quantity, COALESCE(e.handled_count, 0) AS handled_count
                FROM example_order o
                LEFT JOIN example_order_effect e ON e.order_id = o.id
                WHERE o.id = ?
                """, result -> {
                    if (!result.next()) {
                        throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found");
                    }
                    return new OrderView(result.getLong("id"), result.getString("item_code"),
                            result.getInt("quantity"), result.getInt("handled_count"));
                }, id);
    }

    public record CreateOrder(String itemCode, int quantity) { }

    public record OrderView(long orderId, String itemCode, int quantity, int handledCount) { }
}
