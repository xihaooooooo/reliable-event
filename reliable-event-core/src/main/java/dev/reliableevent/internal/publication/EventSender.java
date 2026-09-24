package dev.reliableevent.internal.publication;

import dev.reliableevent.internal.model.StoredEvent;

public interface EventSender {

    SendReceipt send(StoredEvent event);
}
