package it.nexoapp.messaging.outbox;

import io.quarkus.arc.Arc;
import it.nexoapp.common.constant.MDCConstant;
import it.nexoapp.common.context.ActorContext;
import it.nexoapp.common.context.RequestContext;
import it.nexoapp.common.tenant.TenantContext;
import it.nexoapp.common.tenant.TenantNotResolvedException;
import it.nexoapp.common.utils.MDCUtils;
import it.nexoapp.messaging.EnvelopeCodec;
import it.nexoapp.messaging.EventPublisher;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

/**
 * Default {@link EventPublisher}: writes the event to the outbox table in the transaction of the caller.
 */
@ApplicationScoped
@RequiredArgsConstructor
class EventPublisherImpl implements EventPublisher {

    /* - logger - */
    private static final Logger logger = LoggerFactory.getLogger(EventPublisherImpl.class);

    /* - collaborators - */
    private final OutboxRepository repository;
    private final EnvelopeCodec codec;
    private final ActorContext actorContext;

    /* - request - */
    /** Request-scoped context, read only while the request context is active. */
    private final RequestContext requestContext;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(Transactional.TxType.MANDATORY)
    public UUID publish(final String type, final int version, final Object payload) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Cannot publish an event without type");
        }
        if (version < 1) {
            throw new IllegalArgumentException("Event version must be at least 1, got " + version);
        }
        final UUID tenantId = resolveTenant();
        final String correlationId = correlationId();

        final OutboxEntity entity = OutboxEntity.builder()
                .tenantId(tenantId)
                .type(type)
                .version(version)
                .routingKey(type)
                .correlationId(correlationId)
                .actor(actorContext.currentActor())
                .payload(codec.toPayload(payload))
                .occurredAt(OffsetDateTime.now(ZoneOffset.UTC))
                .build();
        repository.persist(entity);

        logger.debug("Event {} v{} stored in the outbox with id {}", type, version, entity.getId());
        return entity.getId();
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Resolves the tenant of the event: the explicit tenant scope first, then the association of the request.
     *
     * @return the tenant identifier
     * @throws IllegalStateException   inside an unscoped block
     * @throws TenantNotResolvedException when neither a scope nor a linked request association exists
     */
    private UUID resolveTenant() {
        if (TenantContext.isUnscoped()) {
            throw new IllegalStateException("Events cannot be published outside a tenant scope");
        }
        return TenantContext.current()
                .map(UUID::fromString)
                .or(this::requestAssociation)
                .orElseThrow(TenantNotResolvedException::new);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Reads the association of the current request.
     *
     * @return the association of a linked caller, empty without an active request context or association
     */
    private Optional<UUID> requestAssociation() {
        if (!Arc.container().requestContext().isActive()) {
            return Optional.empty();
        }
        return Optional.ofNullable(requestContext.getAssociationId());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Correlation id of the event: the call id of the current request or job, a new one when there is none.
     *
     * @return the correlation id, never blank
     */
    private String correlationId() {
        final String callId = MDCUtils.get(MDCConstant.CALL_ID);
        return callId == null || callId.isBlank() ? UUID.randomUUID().toString() : callId;
    }

}
