package dev.reliableevent.spi;

/**
 * Public synchronous transport contract. Return only after the transport's documented
 * broker confirmation has arrived. A local enqueue, buffer acceptance, or locally
 * generated identifier is not confirmation. Implementations must keep the total send
 * duration within their configured budget and classify failures with {@link TransportException}.
 */
@FunctionalInterface
public interface EventTransport {
    TransportReceipt send(OutboundEvent event);
}
