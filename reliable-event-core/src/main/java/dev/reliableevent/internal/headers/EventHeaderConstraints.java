package dev.reliableevent.internal.headers;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

/** Shared limits for event headers persisted by JDBC and sent by RocketMQ. */
public final class EventHeaderConstraints {
    public static final int MAX_HEADER_COUNT = 64;
    public static final int MAX_HEADER_KEY_LENGTH = 128;
    public static final int MAX_HEADER_VALUE_BYTES = 4 * 1024;
    public static final int MAX_TOTAL_HEADER_BYTES = 16 * 1024;
    public static final String RESERVED_PROPERTY_PREFIX = "reliable_event_";
    private static final Pattern HEADER_KEY_PATTERN = Pattern.compile("[A-Za-z0-9_.-]+");

    private EventHeaderConstraints() { }

    /** Returns the existing RocketMQ validation message, or {@code null} when valid. */
    public static String validationError(Map<String, String> headers) {
        if (headers.size() > MAX_HEADER_COUNT) {
            return "Reliable event has " + headers.size()
                    + " headers and exceeds the limit of " + MAX_HEADER_COUNT;
        }
        int totalBytes = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key == null || key.isBlank() || key.length() > MAX_HEADER_KEY_LENGTH
                    || !HEADER_KEY_PATTERN.matcher(key).matches()) {
                return "Reliable event contains an invalid header name " + safeHeaderName(key);
            }
            if (key.startsWith(RESERVED_PROPERTY_PREFIX)) {
                return "Reliable event header " + safeHeaderName(key) + " uses a reserved prefix";
            }
            if (value == null || value.isBlank()) {
                return "Reliable event header " + safeHeaderName(key) + " must have a non-blank value";
            }
            int valueBytes = value.getBytes(StandardCharsets.UTF_8).length;
            if (valueBytes > MAX_HEADER_VALUE_BYTES) {
                return "Reliable event header " + safeHeaderName(key)
                        + " exceeds the value limit of " + MAX_HEADER_VALUE_BYTES + " bytes";
            }
            totalBytes = Math.addExact(totalBytes,
                    key.getBytes(StandardCharsets.UTF_8).length + valueBytes);
            if (totalBytes > MAX_TOTAL_HEADER_BYTES) {
                return "Reliable event headers exceed the total limit of "
                        + MAX_TOTAL_HEADER_BYTES + " bytes";
            }
        }
        return null;
    }

    private static String safeHeaderName(String key) {
        if (key == null) {
            return "<null>";
        }
        String truncated = key.length() <= MAX_HEADER_KEY_LENGTH
                ? key : key.substring(0, MAX_HEADER_KEY_LENGTH);
        StringBuilder safe = new StringBuilder(truncated.length());
        for (int index = 0; index < truncated.length(); index++) {
            char character = truncated.charAt(index);
            boolean allowed = character >= 'A' && character <= 'Z'
                    || character >= 'a' && character <= 'z'
                    || character >= '0' && character <= '9'
                    || character == '_' || character == '.' || character == '-';
            safe.append(allowed ? character : '?');
        }
        return safe.toString();
    }
}
