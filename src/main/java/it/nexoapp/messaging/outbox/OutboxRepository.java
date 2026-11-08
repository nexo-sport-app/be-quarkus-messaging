package it.nexoapp.messaging.outbox;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;
import java.util.UUID;

/**
 * Data access of the outbox table.
 * <p>
 * It holds the only native SQL of the library: the outbox is a global table, so it must be read without the tenant
 * filter and with row locks that Hibernate queries cannot express.
 */
@ApplicationScoped
class OutboxRepository implements PanacheRepositoryBase<OutboxEntity, UUID> {

    /**
     * Claims the oldest pending events and locks them until the current transaction ends. Rows already locked by
     * another relay instance are skipped, so two instances never send the same event.
     *
     * @param batchSize maximum number of events to claim
     * @return the claimed events, oldest first, possibly empty
     */
    @SuppressWarnings("unchecked")
    List<OutboxEntity> claimPending(final int batchSize) {
        return getEntityManager()
                .createNativeQuery("""
                        SELECT * FROM {h-schema}outbox
                        WHERE published_at IS NULL
                        ORDER BY occurred_at
                        LIMIT :batch
                        FOR UPDATE SKIP LOCKED
                        """, OutboxEntity.class)
                .setParameter("batch", batchSize)
                .getResultList();
    }

}
