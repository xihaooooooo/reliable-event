package dev.reliableevent.jdbc;

interface EventSender {

    SendReceipt send(StoredEvent event);
}
