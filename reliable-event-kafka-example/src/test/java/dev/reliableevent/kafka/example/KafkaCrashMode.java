package dev.reliableevent.kafka.example;

enum KafkaCrashMode {
    AFTER_CLAIM_BEFORE_SEND,
    AFTER_BROKER_CONFIRMATION_BEFORE_STATE_COMMIT
}
