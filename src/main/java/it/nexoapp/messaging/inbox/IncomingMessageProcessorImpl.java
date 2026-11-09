package it.nexoapp.messaging.inbox;

import io.quarkus.narayana.jta.QuarkusTransaction;
import it.nexoapp.common.configuration.AppInfo;
import it.nexoapp.common.constant.MDCConstant;
import it.nexoapp.common.context.ExecutionScope;
import it.nexoapp.common.tenant.TenantContext;
import it.nexoapp.common.utils.MDCUtils;
import it.nexoapp.messaging.EventEnvelope;
import it.nexoapp.messaging.IncomingMessageProcessor;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.function.Consumer;

/**
 * Default {@link IncomingMessageProcessor}: runs the handler inside the tenant of the envelope and records the message
 * in the inbox in the same transaction.
 */
@ApplicationScoped
@RequiredArgsConstructor
class IncomingMessageProcessorImpl implements IncomingMessageProcessor {

    /* - logger - */
    private static final Logger logger = LoggerFactory.getLogger(IncomingMessageProcessorImpl.class);

    /* - collaborators - */
    private final InboxRepository repository;
    private final AppInfo appInfo;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean process(final String consumer, final EventEnvelope envelope, final Consumer<EventEnvelope> handler) {
        if (consumer == null || consumer.isBlank()) {
            throw new IllegalArgumentException("Cannot process a message without consumer name");
        }
        if (envelope == null || envelope.tenantId() == null) {
            throw new IllegalArgumentException("Cannot process an envelope without tenant");
        }
        final String actor = ExecutionScope.systemActor(appInfo.code());
        final String previousCallId = MDCUtils.get(MDCConstant.CALL_ID);
        final String previousAssociationId = MDCUtils.get(MDCConstant.ASSOCIATION_ID);
        MDCUtils.put(MDCConstant.ASSOCIATION_ID, envelope.tenantId().toString());
        if (envelope.correlationId() != null && !envelope.correlationId().isBlank()) {
            MDCUtils.put(MDCConstant.CALL_ID, envelope.correlationId());
        }
        try {
            // the scopes are opened BEFORE the transaction: Hibernate resolves the tenant when the session opens
            return TenantContext.callAs(envelope.tenantId(), () -> ExecutionScope.callAs(actor,
                    () -> QuarkusTransaction.requiringNew().call(() -> handleOnce(consumer, envelope, handler, actor))));
        } finally {
            restore(MDCConstant.CALL_ID, previousCallId);
            restore(MDCConstant.ASSOCIATION_ID, previousAssociationId);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Records the message and, only when it is new, runs the handler, all in the current transaction.
     * The claim is the repository's {@code INSERT ... ON CONFLICT DO NOTHING}: zero inserted rows means a duplicate.
     *
     * @param consumer name of the consumer
     * @param envelope the envelope being processed
     * @param handler  business handler
     * @param actor    audit actor of the inbox row
     * @return true when the handler ran, false for a duplicate
     */
    private boolean handleOnce(final String consumer, final EventEnvelope envelope,
                               final Consumer<EventEnvelope> handler, final String actor) {
        final boolean first = repository.insertIfAbsent(
                envelope.messageId(), consumer, OffsetDateTime.now(ZoneOffset.UTC), actor);
        if (!first) {
            logger.info("Message {} already handled by {}, skipped", envelope.messageId(), consumer);
            return false;
        }
        handler.accept(envelope);
        logger.debug("Message {} of type {} handled by {}", envelope.messageId(), envelope.type(), consumer);
        return true;
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Puts a logging context value back to what it was before processing.
     *
     * @param key      the logging context key
     * @param previous the previous value, null when the key was not set
     */
    private static void restore(final MDCConstant key, final String previous) {
        if (previous == null) {
            MDCUtils.remove(key);
        } else {
            MDCUtils.put(key, previous);
        }
    }

}
