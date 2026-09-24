package dev.reliableevent.rocketmq;

import java.util.Set;
import java.util.regex.Pattern;

public record RocketMqDestination(String topic, String tag) {

    private static final int MAX_TOPIC_LENGTH = 64;
    private static final int MAX_TAG_LENGTH = 128;
    private static final Pattern TOPIC_PATTERN = Pattern.compile("[A-Za-z0-9_%\\-]+");
    private static final Set<String> RESERVED_TOPICS = Set.of(
            "TBW102",
            "BenchmarkTest",
            "SELF_TEST_TOPIC",
            "OFFSET_MOVED_EVENT",
            "SCHEDULE_TOPIC_XXXX",
            "RMQ_SYS_TRANS_HALF_TOPIC",
            "RMQ_SYS_TRACE_TOPIC",
            "RMQ_SYS_TRANS_OP_HALF_TOPIC"
    );

    public RocketMqDestination {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (topic.length() > MAX_TOPIC_LENGTH || !TOPIC_PATTERN.matcher(topic).matches()) {
            throw new IllegalArgumentException("topic contains unsupported characters or exceeds 64 characters");
        }
        if (RESERVED_TOPICS.contains(topic)
                || topic.startsWith("rmq_sys")
                || topic.startsWith("%RETRY%")
                || topic.startsWith("%DLQ%")
                || topic.startsWith("rocketmq-broker-")) {
            throw new IllegalArgumentException("topic uses a RocketMQ reserved name or prefix");
        }
        if (tag != null) {
            if (tag.isBlank()) {
                throw new IllegalArgumentException("tag must be null or non-blank");
            }
            if (tag.length() > MAX_TAG_LENGTH || containsControlCharacter(tag)) {
                throw new IllegalArgumentException("tag contains control characters or exceeds 128 characters");
            }
        }
    }

    private static boolean containsControlCharacter(String value) {
        return value.codePoints().anyMatch(Character::isISOControl);
    }
}
