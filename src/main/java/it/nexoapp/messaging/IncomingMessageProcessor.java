package it.nexoapp.messaging;

import java.util.function.Consumer;

/**
 * Entry point for consuming events in a service.
 * <p>
 * A consumer never handles an envelope directly: it passes it here together with its own name and a handler. The
 * processor runs the handler inside the tenant of the envelope, with the correlation id of the envelope in the logging
 * context and the application itself as audit actor. The handler runs in the same database transaction that records
 * the message as handled, so a redelivered message has its side effects applied exactly once.
 */
public interface IncomingMessageProcessor {

    /**
     * Handles an envelope at most once per consumer.
     * <p>
     * The tenant, the audit actor and the logging context values set for the handler are restored when the method
     * returns, also when the handler fails. A failing handler rolls the transaction back, so the message is not
     * recorded as handled and the exception reaches the caller, which lets the broker redeliver or dead-letter it.
     *
     * @param consumer name of the consumer, part of the deduplication key {@code (messageId, consumer)}
     * @param envelope the envelope received from the broker
     * @param handler  business handler, run inside the tenant and the transaction of the inbox row
     * @return true when the handler ran, false when the message had already been handled by this consumer
     * @throws IllegalArgumentException if the consumer is blank or the envelope has no tenant, in which case no scope
     *                                  and no transaction are opened
     */
    boolean process(String consumer, EventEnvelope envelope, Consumer<EventEnvelope> handler);

}
