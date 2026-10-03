package dev.reliableevent.spi;

/** Classification used by a transport when a send does not receive confirmation. */
public enum TransportFailureType { RETRYABLE, NON_RETRYABLE, RESULT_UNKNOWN }
