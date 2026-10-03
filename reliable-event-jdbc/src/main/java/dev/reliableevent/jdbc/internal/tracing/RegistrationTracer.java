package dev.reliableevent.jdbc.internal.tracing;

import dev.reliableevent.EventId;

import java.util.Map;

/** Internal seam for best-effort tracing around outbox registration. */
public interface RegistrationTracer {
    RegistrationTracer NOOP = headers -> new Registration() {
        @Override public Map<String, String> headers() { return headers; }
        @Override public void succeeded(EventId eventId) { }
        @Override public void failed(Throwable failure) { }
        @Override public void close() { }
    };

    Registration begin(Map<String, String> headers);

    interface Registration extends AutoCloseable {
        Map<String, String> headers();
        void succeeded(EventId eventId);
        void failed(Throwable failure);
        @Override void close();
    }
}
