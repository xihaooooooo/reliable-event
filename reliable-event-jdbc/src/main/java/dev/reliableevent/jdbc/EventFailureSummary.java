package dev.reliableevent.jdbc;

import java.util.Objects;

final class EventFailureSummary {

    private static final int MAX_CODE_POINTS = 1024;

    private EventFailureSummary() {
    }

    static String from(RuntimeException exception) {
        Objects.requireNonNull(exception, "exception must not be null");
        String type = exception.getClass().getName();
        String message = exception.getMessage();
        String summary = message == null || message.isBlank()
                ? type
                : type + ": " + message;
        int codePointCount = summary.codePointCount(0, summary.length());
        if (codePointCount <= MAX_CODE_POINTS) {
            return summary;
        }
        int endIndex = summary.offsetByCodePoints(0, MAX_CODE_POINTS);
        return summary.substring(0, endIndex);
    }
}
