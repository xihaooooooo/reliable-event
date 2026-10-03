package dev.reliableevent.kafka;

import dev.reliableevent.spi.EventTransport;
import dev.reliableevent.spi.OutboundEvent;
import dev.reliableevent.spi.TransportException;
import dev.reliableevent.spi.TransportFailureType;
import dev.reliableevent.spi.TransportReceipt;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Synchronous Kafka transport. It acknowledges only a valid broker record position. */
public final class KafkaEventTransport implements EventTransport {
    public static final int DEFAULT_MAX_BODY_BYTES = 4 * 1024 * 1024;
    public static final String EVENT_ID_HEADER = "reliable_event_id";
    public static final String EVENT_TYPE_HEADER = "reliable_event_type";
    public static final String EVENT_KEY_HEADER = "reliable_event_key";

    private final Producer<String, byte[]> producer;
    private final KafkaDestinationResolver destinationResolver;
    private final Duration sendBudget;
    private final int maxBodyBytes;

    public KafkaEventTransport(Producer<String, byte[]> producer, KafkaDestinationResolver destinationResolver,
                               Duration sendBudget, int maxBodyBytes) {
        this.producer = Objects.requireNonNull(producer, "producer must not be null");
        this.destinationResolver = Objects.requireNonNull(destinationResolver, "destinationResolver must not be null");
        this.sendBudget = Objects.requireNonNull(sendBudget, "sendBudget must not be null");
        if (sendBudget.isZero() || sendBudget.isNegative()) {
            throw new IllegalArgumentException("sendBudget must be positive");
        }
        try {
            sendBudget.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("sendBudget is too large", overflow);
        }
        if (maxBodyBytes < 1) {
            throw new IllegalArgumentException("maxBodyBytes must be positive");
        }
        this.maxBodyBytes = maxBodyBytes;
    }

    @Override
    public TransportReceipt send(OutboundEvent event) {
        if (event == null) {
            throw permanent("Kafka outbound event is missing");
        }
        requireNonBlank(event.eventType(), "event type");
        requireNonBlank(event.eventKey(), "event key");
        requireNonBlank(event.payloadJson(), "payload");
        String headerError;
        try {
            headerError = KafkaHeaderValidator.validationError(event.headers());
        } catch (RuntimeException invalidHeaders) {
            throw new TransportException(TransportFailureType.NON_RETRYABLE,
                    "Kafka event headers must contain valid string values", invalidHeaders);
        }
        if (headerError != null) {
            throw permanent(headerError);
        }

        String topic = destinationResolver.resolve(event.eventType());
        if (!MapKafkaDestinationResolver.isValidTopic(topic)) {
            throw permanent("Kafka destination resolver returned an invalid topic");
        }
        byte[] payload = event.payloadJson().getBytes(StandardCharsets.UTF_8);
        if (payload.length > maxBodyBytes) {
            throw permanent("Kafka payload exceeds the configured body limit");
        }
        Headers headers = new RecordHeaders();
        event.headers().forEach((name, value) -> headers.add(name, value.getBytes(StandardCharsets.UTF_8)));
        headers.add(EVENT_ID_HEADER, Long.toString(event.id().value()).getBytes(StandardCharsets.UTF_8));
        headers.add(EVENT_TYPE_HEADER, event.eventType().getBytes(StandardCharsets.UTF_8));
        headers.add(EVENT_KEY_HEADER, event.eventKey().getBytes(StandardCharsets.UTF_8));
        ProducerRecord<String, byte[]> record;
        try {
            record = new ProducerRecord<>(topic, null, event.eventKey(), payload, headers);
        } catch (RuntimeException invalidRecord) {
            throw new TransportException(TransportFailureType.NON_RETRYABLE,
                    "Kafka record is invalid", invalidRecord);
        }

        long deadline = System.nanoTime() + sendBudget.toNanos();
        Future<RecordMetadata> future;
        try {
            future = producer.send(record);
        } catch (KafkaException sendFailure) {
            throw classify(sendFailure, true);
        } catch (RuntimeException sendFailure) {
            throw unknown(null);
        }
        if (future == null) {
            throw unknown(null);
        }

        try {
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                throw unknown(new TimeoutException("Kafka send budget elapsed before confirmation wait"));
            }
            RecordMetadata metadata = future.get(remainingNanos, TimeUnit.NANOSECONDS);
            if (deadline - System.nanoTime() <= 0) {
                throw unknown(null);
            }
            if (metadata == null || !metadata.hasOffset() || metadata.offset() < 0 || metadata.partition() < 0
                    || !topic.equals(metadata.topic())) {
                throw unknown(null);
            }
            Map<String, String> receiptMetadata = new LinkedHashMap<>();
            receiptMetadata.put("topic", metadata.topic());
            receiptMetadata.put("partition", Integer.toString(metadata.partition()));
            receiptMetadata.put("offset", Long.toString(metadata.offset()));
            return new TransportReceipt(Optional.empty(), receiptMetadata);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw unknown(interrupted);
        } catch (TimeoutException timeout) {
            throw unknown(timeout);
        } catch (ExecutionException failed) {
            Throwable cause = failed.getCause() == null ? failed : failed.getCause();
            throw classify(cause, false);
        } catch (KafkaException failure) {
            throw classify(failure, false);
        } catch (RuntimeException failure) {
            if (failure instanceof TransportException transportFailure) {
                throw transportFailure;
            }
            throw unknown(failure);
        }
    }

    private static TransportException classify(Throwable failure, boolean sendCallFailed) {
        if (failure instanceof AuthenticationException || failure instanceof AuthorizationException
                || failure instanceof InvalidTopicException
                || failure instanceof RecordTooLargeException || failure instanceof SerializationException
                || failure instanceof InvalidRecordException || failure instanceof UnsupportedVersionException) {
            return new TransportException(TransportFailureType.NON_RETRYABLE,
                    "Kafka permanently rejected the event");
        }
        if (sendCallFailed && failure instanceof RetriableException
                && !(failure instanceof InterruptException) && !Thread.currentThread().isInterrupted()) {
            return new TransportException(TransportFailureType.RETRYABLE,
                    "Kafka could not accept the event for delivery");
        }
        if (failure instanceof org.apache.kafka.common.errors.TimeoutException) {
            return unknown(null);
        }
        return unknown(null);
    }

    private static void requireNonBlank(String value, String label) {
        if (value == null || value.isBlank()) {
            throw permanent("Kafka event " + label + " must not be blank");
        }
    }

    private static TransportException permanent(String message) {
        return new TransportException(TransportFailureType.NON_RETRYABLE, message);
    }

    private static TransportException unknown(Throwable cause) {
        return new TransportException(TransportFailureType.RESULT_UNKNOWN, "Kafka send result is unknown", cause);
    }
}
