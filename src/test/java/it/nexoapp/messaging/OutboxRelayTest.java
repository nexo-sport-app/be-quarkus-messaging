package it.nexoapp.messaging;

import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.junit.QuarkusTest;
import it.nexoapp.common.constant.MDCConstant;
import it.nexoapp.common.tenant.TenantContext;
import it.nexoapp.common.utils.MDCUtils;
import it.nexoapp.messaging.contract.IdentityAccessChanged;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests the outbox: an event published in a transaction becomes an outbox row and the relay marks it published only
 * after the broker confirmed it.
 */
@QuarkusTest
class OutboxRelayTest {

    /* - tenants - */
    private static final UUID TENANT_A = UUID.fromString("11111111-1111-4111-8111-111111111111");

    /* - collaborators - */
    @Inject
    EventPublisher publisher;

    @Inject
    EntityManager em;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Snapshot of the columns of an outbox row that the tests assert on.
     *
     * @param tenantId      tenant column
     * @param correlationId correlation id column
     * @param actor         actor column
     * @param publishedAt   published instant, null while pending
     */
    private record Row(UUID tenantId, String correlationId, String actor, Object publishedAt) {
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Removes the rows created by a test and the MDC entry it set.
     */
    @AfterEach
    void cleanUp() {
        MDCUtils.remove(MDCConstant.CALL_ID);
        TenantContext.runUnscoped(() -> QuarkusTransaction.requiringNew()
                .run(() -> em.createNativeQuery("DELETE FROM {h-schema}outbox").executeUpdate()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Builds a small contract payload.
     *
     * @return a contract record
     */
    private static IdentityAccessChanged payload() {
        return new IdentityAccessChanged(TENANT_A, UUID.randomUUID(), "http://idp/realms/nexo", "sub-1", Set.of("PRESIDENT"), 1);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Reads one outbox row outside every tenant.
     *
     * @param id identifier of the row
     * @return the row snapshot, null when absent
     */
    private Row read(final UUID id) {
        return TenantContext.callUnscoped(() -> QuarkusTransaction.requiringNew().call(() -> {
            final var rows = em.createNativeQuery(
                            "SELECT tenant_id, correlation_id, actor, published_at FROM {h-schema}outbox WHERE id = :id")
                    .setParameter("id", id)
                    .getResultList();
            if (rows.isEmpty()) {
                return null;
            }
            final Object[] r = (Object[]) rows.getFirst();
            return new Row((UUID) r[0], (String) r[1], (String) r[2], r[3]);
        }));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tracer: an event published under tenant A is stored with tenant, correlation id and actor, and the relay marks it
     * published once the broker confirmed it.
     */
    @Test
    void publishedEventReachesTheBrokerAndIsMarkedPublished() {
        MDCUtils.put(MDCConstant.CALL_ID, "call-msg-1");

        final UUID id = TenantContext.callAs(TENANT_A, () -> QuarkusTransaction.requiringNew()
                .call(() -> publisher.publish(IdentityAccessChanged.TYPE, IdentityAccessChanged.VERSION, payload())));

        final Row stored = read(id);
        assertNotNull(stored);
        assertEquals(TENANT_A, stored.tenantId());
        assertEquals("call-msg-1", stored.correlationId());
        assertEquals("system:msg", stored.actor());

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertNotNull(read(id).publishedAt()));
    }

}
