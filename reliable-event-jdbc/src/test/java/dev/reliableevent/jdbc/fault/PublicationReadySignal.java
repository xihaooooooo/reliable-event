package dev.reliableevent.jdbc.fault;

import java.util.Objects;

record PublicationReadySignal(
        PublicationCrashMode mode,
        long eventId,
        long claimVersion,
        String workerId,
        String messageId
) {

    private static final String PREFIX = "READY";
    private static final String CLAIMED = "CLAIMED";
    private static final String DELIVERED = "DELIVERED";

    PublicationReadySignal {
        Objects.requireNonNull(mode, "mode must not be null");
        if (eventId <= 0) {
            throw new IllegalArgumentException("eventId must be positive");
        }
        if (claimVersion <= 0) {
            throw new IllegalArgumentException("claimVersion must be positive");
        }
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be blank");
        }
        if (mode == PublicationCrashMode.AFTER_CLAIM_BEFORE_SEND) {
            if (messageId != null) {
                throw new IllegalArgumentException("claimed signal must not contain messageId");
            }
        } else if (messageId == null || messageId.isBlank()) {
            throw new IllegalArgumentException("delivered signal requires messageId");
        }
    }

    static PublicationReadySignal claimed(long eventId, long claimVersion, String workerId) {
        return new PublicationReadySignal(
                PublicationCrashMode.AFTER_CLAIM_BEFORE_SEND,
                eventId,
                claimVersion,
                workerId,
                null
        );
    }

    static PublicationReadySignal delivered(
            long eventId,
            long claimVersion,
            String workerId,
            String messageId
    ) {
        return new PublicationReadySignal(
                PublicationCrashMode.AFTER_DELIVERY_BEFORE_STATE_UPDATE,
                eventId,
                claimVersion,
                workerId,
                messageId
        );
    }

    static PublicationReadySignal parse(String line) {
        if (line == null || line.isBlank()) {
            throw new IllegalArgumentException("ready signal must not be blank");
        }
        String[] fields = line.trim().split("\\s+");
        if (fields.length < 5 || !PREFIX.equals(fields[0])) {
            throw new IllegalArgumentException("invalid ready signal: " + line);
        }

        long eventId = positiveLong(fields[2], "eventId");
        long claimVersion = positiveLong(fields[3], "claimVersion");
        if (CLAIMED.equals(fields[1]) && fields.length == 5) {
            return claimed(eventId, claimVersion, fields[4]);
        }
        if (DELIVERED.equals(fields[1]) && fields.length == 6) {
            return delivered(eventId, claimVersion, fields[4], fields[5]);
        }
        throw new IllegalArgumentException("invalid ready signal: " + line);
    }

    String format() {
        if (mode == PublicationCrashMode.AFTER_CLAIM_BEFORE_SEND) {
            return String.join(
                    " ",
                    PREFIX,
                    CLAIMED,
                    Long.toString(eventId),
                    Long.toString(claimVersion),
                    workerId
            );
        }
        return String.join(
                " ",
                PREFIX,
                DELIVERED,
                Long.toString(eventId),
                Long.toString(claimVersion),
                workerId,
                messageId
        );
    }

    private static long positiveLong(String value, String name) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " must be a number", exception);
        }
    }
}
