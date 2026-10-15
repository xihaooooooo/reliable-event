package dev.reliableevent;

/**
 * Application supplied handling for terminal publication failures. Calls may be repeated when the
 * handler succeeded but the database result write failed or the process lost its lease; handlers
 * must be idempotent. The handler runs after the short claim transaction has committed, without a
 * database transaction or row lock. Returning normally marks fallback complete; throwing marks
 * the event failed.
 */
@FunctionalInterface
public interface EventFallbackHandler {
    void handle(EventFallbackContext context);
}
