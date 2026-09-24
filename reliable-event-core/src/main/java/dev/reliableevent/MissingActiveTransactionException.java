package dev.reliableevent;

public final class MissingActiveTransactionException extends IllegalStateException {

    public MissingActiveTransactionException() {
        super("Reliable events must be published inside an active database transaction");
    }
}
