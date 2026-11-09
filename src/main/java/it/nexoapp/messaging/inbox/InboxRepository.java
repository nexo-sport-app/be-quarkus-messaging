package it.nexoapp.messaging.inbox;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Data access of the inbox table.
 * <p>
 * It holds native SQL because the inbox is a global table and the claim of a message must be one atomic statement
 * that Hibernate cannot express: {@code INSERT ... ON CONFLICT DO NOTHING}.
 */
@ApplicationScoped
class InboxRepository implements PanacheRepositoryBase<InboxEntity, InboxEntity.Key> {

    /**
     * Records a message as handled by a consumer unless it is already recorded. Concurrent deliveries of the same
     * message block on the primary key until the first transaction ends, so only one of them inserts.
     *
     * @param messageId   identifier of the message
     * @param consumer    name of the consumer
     * @param processedAt instant at which the message is handled
     * @param actor       audit actor written as creator and last modifier
     * @return true when the row was inserted, false when the message was already recorded
     */
    boolean insertIfAbsent(final UUID messageId, final String consumer, final OffsetDateTime processedAt,
                           final String actor) {
        final int inserted = getEntityManager()
                .createNativeQuery("""
                        INSERT INTO {h-schema}inbox
                            (message_id, consumer, processed_at, created_by, created_at, updated_by, updated_at)
                        VALUES (:messageId, :consumer, :processedAt, :actor, :processedAt, :actor, :processedAt)
                        ON CONFLICT DO NOTHING
                        """)
                .setParameter("messageId", messageId)
                .setParameter("consumer", consumer)
                .setParameter("processedAt", processedAt)
                .setParameter("actor", actor)
                .executeUpdate();
        return inserted == 1;
    }

}
