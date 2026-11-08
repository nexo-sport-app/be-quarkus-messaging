package it.nexoapp.messaging;

import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.scheduler.Scheduler;
import io.quarkus.test.junit.QuarkusTest;
import it.nexoapp.common.constant.MDCConstant;
import it.nexoapp.common.tenant.TenantContext;
import it.nexoapp.common.tenant.TenantNotResolvedException;
import it.nexoapp.common.utils.MDCUtils;
import it.nexoapp.messaging.contract.IdentityAccessChanged;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.TransactionRequiredException;
import jakarta.transaction.TransactionalException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Inject
    Scheduler scheduler;

    /** Identity of the relay job, as declared by the library. */
    private static final String RELAY_JOB = "nexo-outbox-relay";

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
        scheduler.resume(RELAY_JOB);
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
     * Counts the outbox rows outside every tenant.
     *
     * @return the number of rows in the outbox
     */
    private long count() {
        return TenantContext.callUnscoped(() -> QuarkusTransaction.requiringNew().call(() ->
                ((Number) em.createNativeQuery("SELECT count(*) FROM {h-schema}outbox").getSingleResult()).longValue()));
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

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * A publish whose transaction rolls back leaves no outbox row, so nothing is ever sent for that work.
     */
    @Test
    void rolledBackTransactionLeavesNoOutboxRow() throws InterruptedException {
        final AtomicReference<UUID> id = new AtomicReference<>();

        assertThrows(IllegalStateException.class, () -> TenantContext.runAs(TENANT_A,
                () -> QuarkusTransaction.requiringNew().run(() -> {
                    id.set(publisher.publish(IdentityAccessChanged.TYPE, IdentityAccessChanged.VERSION, payload()));
                    throw new IllegalStateException("business failure after publishing");
                })));

        assertNotNull(id.get());
        assertNull(read(id.get()));
        // give the relay time to run: it must still find nothing
        Thread.sleep(1500);
        assertEquals(0, count());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Without a tenant scope and without a request association nothing is published.
     */
    @Test
    void publishWithoutTenantFailsAndInsertsNothing() {
        assertThrows(TenantNotResolvedException.class, () -> QuarkusTransaction.requiringNew()
                .run(() -> publisher.publish(IdentityAccessChanged.TYPE, IdentityAccessChanged.VERSION, payload())));

        assertEquals(0, count());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Inside an unscoped block an event would belong to no association, so publishing is refused.
     */
    @Test
    void publishInsideUnscopedBlockFailsAndInsertsNothing() {
        assertThrows(IllegalStateException.class, () -> TenantContext.runUnscoped(() -> QuarkusTransaction.requiringNew()
                .run(() -> publisher.publish(IdentityAccessChanged.TYPE, IdentityAccessChanged.VERSION, payload()))));

        assertEquals(0, count());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Publishing needs the transaction of the caller: without one the call is rejected.
     */
    @Test
    void publishOutsideTransactionIsRejected() {
        final TransactionalException e = assertThrows(TransactionalException.class, () -> TenantContext.runAs(TENANT_A,
                () -> publisher.publish(IdentityAccessChanged.TYPE, IdentityAccessChanged.VERSION, payload())));

        assertInstanceOf(TransactionRequiredException.class, e.getCause());
        assertEquals(0, count());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * While the relay is paused a committed row stays pending; once resumed the relay publishes it.
     */
    @Test
    void pendingRowStaysPendingWhileRelayIsPausedThenIsPublished() throws InterruptedException {
        scheduler.pause(RELAY_JOB);
        // let a run that started before the pause finish
        Thread.sleep(500);

        final UUID id = TenantContext.callAs(TENANT_A, () -> QuarkusTransaction.requiringNew()
                .call(() -> publisher.publish(IdentityAccessChanged.TYPE, IdentityAccessChanged.VERSION, payload())));

        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(6))
                .untilAsserted(() -> assertNull(read(id).publishedAt()));

        scheduler.resume(RELAY_JOB);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertNotNull(read(id).publishedAt()));
    }

}
