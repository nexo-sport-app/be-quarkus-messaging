package it.nexoapp.messaging;

import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.junit.QuarkusTest;
import it.nexoapp.common.constant.MDCConstant;
import it.nexoapp.common.context.ExecutionScope;
import it.nexoapp.common.tenant.TenantContext;
import it.nexoapp.common.utils.MDCUtils;
import it.nexoapp.messaging.job.TenantJobRunner;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves that scheduled jobs run with the tenant, a per-run correlation id and the system actor, leave no context
 * behind, and that work running for one tenant, as a job or as a consumer handler, never sees the rows of another.
 */
@QuarkusTest
class TenantJobAndConsumerIsolationTest {

    /* - tenants - */
    private static final UUID TENANT_A = UUID.fromString("11111111-1111-4111-8111-111111111111");
    private static final UUID TENANT_B = UUID.fromString("22222222-2222-4222-8222-222222222222");

    /* - collaborators - */
    @Inject
    TenantJobRunner runner;

    @Inject
    ProbeRecordRepository repository;

    @Inject
    ProbeSideEffects sideEffects;

    @Inject
    EntityManager em;

    @Inject
    EnvelopeCodec codec;

    @Inject
    ProbeSender sender;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Starts every test from a clean table and a disarmed job.
     */
    @BeforeEach
    @AfterEach
    void cleanUp() {
        ProbeTenantJob.reset();
        sideEffects.reset();
        MDCUtils.remove(MDCConstant.CALL_ID);
        MDCUtils.remove(MDCConstant.ASSOCIATION_ID);
        TenantContext.runUnscoped(() -> QuarkusTransaction.requiringNew().run(() ->
                em.createNativeQuery("DELETE FROM {h-schema}probe_record").executeUpdate()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Writes a probe row under a tenant, in its own transaction.
     *
     * @param tenant tenant that owns the row
     * @param label  label of the row
     */
    private void seed(final UUID tenant, final String label) {
        TenantContext.runAs(tenant, () -> QuarkusTransaction.requiringNew().run(() ->
                repository.persist(new ProbeRecordEntity(label))));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Counts the probe rows visible under a tenant.
     *
     * @param tenant tenant to read as
     * @return number of visible rows
     */
    private long countAs(final UUID tenant) {
        return TenantContext.callAs(tenant, () -> QuarkusTransaction.requiringNew().call(repository::count));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Reads, bypassing the tenant filter, the tenant and the creator of the row with a label.
     *
     * @param label label of the row
     * @return the row columns {@code tenant_id} and {@code created_by} as strings, null when absent
     */
    private String[] rawRow(final String label) {
        return TenantContext.callUnscoped(() -> QuarkusTransaction.requiringNew().call(() -> {
            final var rows = em.createNativeQuery(
                            "SELECT CAST(tenant_id AS TEXT), created_by FROM {h-schema}probe_record WHERE label = :label")
                    .setParameter("label", label).getResultList();
            if (rows.isEmpty()) {
                return null;
            }
            final Object[] row = (Object[]) rows.getFirst();
            return new String[]{(String) row[0], (String) row[1]};
        }));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Waits until the armed job has completed one run.
     *
     * @return what the job observed
     */
    private static ProbeTenantJob.Observation awaitJob() {
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> ProbeTenantJob.result() != null);
        return ProbeTenantJob.result();
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * The work of the job sees tenant A, the association id and a call id in the logging context and the system actor;
     * its row carries tenant A and the system actor; afterwards the scheduler thread is back to a clean state.
     */
    @Test
    void jobWorkRunsInTheTenantWithCallIdAndSystemActorAndLeavesNothingBehind() {
        ProbeTenantJob.arm(TENANT_A, "job-a");

        final ProbeTenantJob.Observation seen = awaitJob();

        assertNull(seen.error());
        assertEquals(TENANT_A.toString(), seen.tenantInside());
        assertEquals(TENANT_A.toString(), seen.associationInside());
        assertNotNull(seen.callIdInside());
        assertFalse(seen.callIdInside().isBlank());
        assertEquals("system:msg", seen.actorInside());
        // the row really in the database
        final String[] row = rawRow("job-a");
        assertNotNull(row);
        assertEquals(TENANT_A.toString(), row[0]);
        assertEquals("system:msg", row[1]);
        // scheduler thread restored: the values it had before the run (absent) are back
        assertNull(seen.callIdBefore());
        assertNull(seen.callIdAfter());
        assertNull(seen.associationAfter());
        assertEquals("", seen.tenantAfter());
        assertNull(seen.actorAfter());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Every run gets its own call id.
     */
    @Test
    void everyJobRunGetsItsOwnCallId() {
        final java.util.List<String> callIds = new java.util.ArrayList<>();
        runner.runForTenant("job-x", TENANT_A, () -> callIds.add(MDCUtils.get(MDCConstant.CALL_ID)));
        runner.runForTenant("job-x", TENANT_A, () -> callIds.add(MDCUtils.get(MDCConstant.CALL_ID)));

        assertEquals(2, callIds.size());
        assertTrue(callIds.stream().noneMatch(String::isBlank));
        assertTrue(!callIds.get(0).equals(callIds.get(1)));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Values that were set before the runner was called are put back afterwards, also when the work fails, and a
     * failing work rolls its transaction back.
     */
    @Test
    void previousLoggingValuesAreRestoredAlsoWhenTheWorkFails() {
        MDCUtils.put(MDCConstant.CALL_ID, "previous-call");
        MDCUtils.put(MDCConstant.ASSOCIATION_ID, "previous-association");

        runner.runForTenant("job-ok", TENANT_A, () -> assertNotNull(MDCUtils.get(MDCConstant.CALL_ID)));
        assertEquals("previous-call", MDCUtils.get(MDCConstant.CALL_ID));
        assertEquals("previous-association", MDCUtils.get(MDCConstant.ASSOCIATION_ID));

        assertThrows(IllegalStateException.class, () -> runner.runForTenant("job-fail", TENANT_A, () -> {
            repository.persist(new ProbeRecordEntity("rolled-back"));
            throw new IllegalStateException("work failure");
        }));
        assertEquals("previous-call", MDCUtils.get(MDCConstant.CALL_ID));
        assertEquals("previous-association", MDCUtils.get(MDCConstant.ASSOCIATION_ID));
        assertEquals("", TenantContext.current().orElse(""));
        assertTrue(ExecutionScope.currentActor().isEmpty());
        assertNull(rawRow("rolled-back"));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * A null tenant and a call inside an unscoped block are both rejected and the work never runs.
     */
    @Test
    void nullTenantAndUnscopedCallsAreRejectedWithoutRunningTheWork() {
        final AtomicBoolean ran = new AtomicBoolean(false);

        assertThrows(IllegalArgumentException.class, () -> runner.runForTenant("job-null", null, () -> ran.set(true)));
        assertThrows(IllegalStateException.class, () -> TenantContext.runUnscoped(
                () -> runner.runForTenant("job-unscoped", TENANT_A, () -> ran.set(true))));
        assertThrows(IllegalArgumentException.class, () -> runner.callForTenant("job-null", null, () -> {
            ran.set(true);
            return 1;
        }));

        assertFalse(ran.get());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * {@code callForTenant} returns the value of the work.
     */
    @Test
    void callForTenantReturnsTheValueOfTheWork() {
        final long visible = runner.callForTenant("job-call", TENANT_A, repository::count);

        assertEquals(0, visible);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Two-tenant job: a row seeded under tenant B is not seen by the job running for tenant A, and the row the job
     * wrote under A is invisible under B.
     */
    @Test
    void jobForTenantANeverSeesTenantBRowsAndItsRowIsInvisibleToB() {
        seed(TENANT_B, "b-seed");
        ProbeTenantJob.arm(TENANT_A, "job-a-isolation");

        final ProbeTenantJob.Observation seen = awaitJob();

        assertNull(seen.error());
        assertEquals(0, seen.visibleBefore());
        assertEquals(1, countAs(TENANT_A));
        assertEquals(1, countAs(TENANT_B));
        assertEquals(0, TenantContext.callAs(TENANT_B, () -> QuarkusTransaction.requiringNew().call(
                () -> repository.count("label", "job-a-isolation"))));
        assertEquals(0, TenantContext.callAs(TENANT_A, () -> QuarkusTransaction.requiringNew().call(
                () -> repository.count("label", "b-seed"))));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Builds an isolation envelope for the raw sender.
     *
     * @param tenant tenant of the envelope
     * @return a new envelope of type {@code probe.isolation}
     */
    private static EventEnvelope isolationEnvelope(final UUID tenant) {
        return new EventEnvelope(UUID.randomUUID(), "probe.isolation", 1, tenant, "call-isolation", "system:other",
                OffsetDateTime.now(), "{}");
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Two-tenant consumer: the handler for tenant A counts zero of B's rows and writes one; that row is visible under A
     * and invisible under B; a later envelope for B sees none of A's rows.
     */
    @Test
    void consumerHandlerForTenantANeverSeesTenantBRowsAndItsRowIsInvisibleToB() {
        seed(TENANT_B, "b-seed");
        final Map<UUID, Long> visible = new ConcurrentHashMap<>();
        sideEffects.register("probe.isolation", envelope -> {
            visible.put(envelope.messageId(), repository.count());
            repository.persist(new ProbeRecordEntity("consumer-" + (envelope.tenantId().equals(TENANT_A) ? "a" : "b")));
        });
        final EventEnvelope forA = isolationEnvelope(TENANT_A);

        sender.send(codec.encode(forA));
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> visible.containsKey(forA.messageId()));

        assertEquals(0L, visible.get(forA.messageId()));
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals(1, countAs(TENANT_A));
            assertEquals(0, TenantContext.callAs(TENANT_B, () -> QuarkusTransaction.requiringNew().call(
                    () -> repository.count("label", "consumer-a"))));
        });
        assertEquals("system:msg", rawRow("consumer-a")[1]);
        assertEquals(TENANT_A.toString(), rawRow("consumer-a")[0]);

        // an envelope for B, processed after the one for A, sees only B's own seed row and none of A's
        final EventEnvelope forB = isolationEnvelope(TENANT_B);
        sender.send(codec.encode(forB));
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> visible.containsKey(forB.messageId()));

        assertEquals(1L, visible.get(forB.messageId()));
        assertEquals(0, TenantContext.callAs(TENANT_B, () -> QuarkusTransaction.requiringNew().call(
                () -> repository.count("label", "consumer-a"))));
        assertEquals(1, countAs(TENANT_A));
    }

}
