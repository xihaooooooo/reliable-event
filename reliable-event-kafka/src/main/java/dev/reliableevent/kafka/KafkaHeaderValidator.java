package dev.reliableevent.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

/** Kafka-side validation of the frozen ReliableEvent header limits and reserved prefix. */
final class KafkaHeaderValidator {
    private static final int MAX_COUNT = 64;
    private static final int MAX_KEY_LENGTH = 128;
    private static final int MAX_VALUE_BYTES = 4 * 1024;
    private static final int MAX_TOTAL_BYTES = 16 * 1024;
    private static final String RESERVED_PREFIX = "reliable_event_";
    private static final Pattern KEY = Pattern.compile("[A-Za-z0-9_.-]+");

    private KafkaHeaderValidator() { }

    static String validationError(Map<String, String> headers) {
        if (headers.size() > MAX_COUNT) {
            return "Reliable event has " + headers.size() + " headers and exceeds the limit of " + MAX_COUNT;
        }
        int totalBytes = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            if (name == null || name.isBlank() || name.length() > MAX_KEY_LENGTH || !KEY.matcher(name).matches()) {
                return "Reliable event contains an invalid header name " + safeName(name);
            }
            if (name.startsWith(RESERVED_PREFIX)) {
                return "Reliable event header " + safeName(name) + " uses a reserved prefix";
            }
            if (value == null || value.isBlank()) {
                return "Reliable event header " + safeName(name) + " must have a non-blank value";
            }
            int valueBytes = value.getBytes(StandardCharsets.UTF_8).length;
            if (valueBytes > MAX_VALUE_BYTES) {
                return "Reliable event header " + safeName(name)
                        + " exceeds the value limit of " + MAX_VALUE_BYTES + " bytes";
            }
            totalBytes += name.getBytes(StandardCharsets.UTF_8).length + valueBytes;
            if (totalBytes > MAX_TOTAL_BYTES) {
                return "Reliable event headers exceed the total limit of " + MAX_TOTAL_BYTES + " bytes";
            }
        }
        return null;
    }

    private static String safeName(String name) {
        if (name == null) {
            return "<null>";
        }
        String truncated = name.length() <= MAX_KEY_LENGTH ? name : name.substring(0, MAX_KEY_LENGTH);
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
