package it.nexoapp.messaging;

import java.util.UUID;

/**
 * Entry point for publishing events from a service.
 * <p>
 * An event is never sent to the broker directly: it is written to the service's outbox table in the caller's
 * database transaction and a relay delivers it afterwards. If the transaction rolls back the event disappears with it,
 * so no event is emitted for work that did not happen.
 */
public interface EventPublisher {

    /**
     * Records an event in the outbox, inside the transaction of the caller.
     * <p>
     * The tenant is the one of the active {@code TenantContext} scope or, without a scope, the association of the
     * current request. The correlation id is the call id of the request (or a new one) and the actor is the audit actor
     * of the current thread.
     *
     * @param type    logical event type, also used as routing key, for example {@code identity.access.changed}
     * @param version version of the payload contract, 1 or greater
     * @param payload contract record carried by the event, serialized to JSON
     * @return the identifier of the event, which consumers use to deduplicate redeliveries
     * @throws IllegalArgumentException                           if the type is blank, the version is below 1 or the
     *                                                            payload is missing
     * @throws it.nexoapp.common.tenant.TenantNotResolvedException if no tenant can be resolved
     * @throws IllegalStateException                              if called inside an unscoped block, where events would
     *                                                            belong to no association
     * @throws jakarta.transaction.TransactionalException         if there is no active transaction
     */
    UUID publish(String type, int version, Object payload);

}
