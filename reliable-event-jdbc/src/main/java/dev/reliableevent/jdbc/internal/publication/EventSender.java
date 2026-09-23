package dev.reliableevent.jdbc.internal.publication;

import dev.reliableevent.jdbc.internal.model.StoredEvent;

public interface EventSender {

    SendReceipt send(StoredEvent event);
}
