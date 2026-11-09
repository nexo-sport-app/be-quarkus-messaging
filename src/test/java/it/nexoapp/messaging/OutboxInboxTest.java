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
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test of the messaging library: an event travels from the outbox through RabbitMQ to the consumer, which
 * handles it inside the tenant of the event, with its correlation id and the system actor, leaving one inbox row.
 */
@QuarkusTest
class OutboxInboxTest {

    /* - tenants - */
    private static final UUID TENANT_A = UUID.fromString("11111111-1111-4111-8111-111111111111");

    /* - collaborators - */
    @Inject
    EventPublisher publisher;

    @Inject
    IncomingMessageProcessor processor;

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
     * Removes the rows, recordings and MDC entries left by a test.
     */
    @AfterEach
    void cleanUp() {
        MDCUtils.remove(MDCConstant.CALL_ID);
        MDCUtils.remove(MDCConstant.ASSOCIATION_ID);
        sideEffects.reset();
        TenantContext.runUnscoped(() -> QuarkusTransaction.requiringNew().run(() -> {
            em.createNativeQuery("DELETE FROM {h-schema}outbox").executeUpdate();
            em.createNativeQuery("DELETE FROM {h-schema}inbox").executeUpdate();
        }));
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
     * Builds an envelope for the probe consumer.
     *
     * @param tenant tenant of the envelope, may be null
     * @return a new envelope
     */
    private static EventEnvelope envelope(final UUID tenant) {
        return new EventEnvelope(UUID.randomUUID(), "probe.created", 1, tenant, "call-direct-1", "system:other",
                OffsetDateTime.now(), "{}");
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Counts the inbox rows of a message, outside every tenant.
     *
     * @param messageId message id
     * @return number of rows
     */
    private long inboxCount(final UUID messageId) {
        return TenantContext.callUnscoped(() -> QuarkusTransaction.requiringNew().call(() ->
                ((Number) em.createNativeQuery("SELECT count(*) FROM {h-schema}inbox WHERE message_id = :id")
                        .setParameter("id", messageId).getSingleResult()).longValue()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Reads a single string column of a row identified by a UUID column.
     *
     * @param table  table name
     * @param column column to read
     * @param idCol  identifying column
     * @param id     identifier value
     * @return the value, null when the row is absent or the value is null
     */
    private Object scalar(final String table, final String column, final String idCol, final UUID id) {
        return TenantContext.callUnscoped(() -> QuarkusTransaction.requiringNew().call(() -> {
            final var rows = em.createNativeQuery(
                            "SELECT " + column + " FROM {h-schema}" + table + " WHERE " + idCol + " = :id")
                    .setParameter("id", id).getResultList();
            return rows.isEmpty() ? null : rows.getFirst();
        }));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * An event published under tenant A reaches the consumer with tenant, call id and system actor; it leaves one inbox
     * row, the outbox row is published and the consumer thread is left clean. The request context is active inside the
     * consumer before the processor opens any scope.
     */
    @Test
    void eventTravelsFromOutboxThroughBrokerToConsumerWithTenantAndCallId() {
        MDCUtils.put(MDCConstant.CALL_ID, "call-msg-1");

        final UUID id = TenantContext.callAs(TENANT_A, () -> QuarkusTransaction.requiringNew()
                .call(() -> publisher.publish("probe.created", 1, payload())));
        MDCUtils.remove(MDCConstant.CALL_ID);

        Awaitility.await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertNotNull(sideEffects.surroundingsFor(id)));

        // exactly one handling, inside the tenant, with correlation id and system actor
        assertEquals(1, sideEffects.handledFor(id).size());
        final ProbeSideEffects.Handled handled = sideEffects.handledFor(id).getFirst();
        assertEquals(TENANT_A.toString(), handled.tenant());
        assertEquals("call-msg-1", handled.callId());
        assertEquals(TENANT_A.toString(), handled.associationId());
        assertEquals("system:msg", handled.actor());

        // one inbox row written by the system actor, outbox row marked published
        assertEquals(1, inboxCount(id));
        assertEquals("system:msg", scalar("inbox", "created_by", "message_id", id));
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertNotNull(scalar("outbox", "published_at", "id", id)));

        // the consumer thread is clean afterwards and the request context was active before the processor ran
        final ProbeSideEffects.Surroundings after = sideEffects.surroundingsFor(id);
        assertTrue(after.requestContextActiveOnEntry(), "request context must be active inside the consumer");
        assertEquals("", after.tenantAfter());
        assertNull(after.callIdAfter());
        assertNull(after.associationIdAfter());
        assertNull(after.actorAfter());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * The same message handled twice by the same consumer runs the handler once.
     */
    @Test
    void secondProcessingOfTheSameMessageSkipsTheHandler() {
        final EventEnvelope envelope = envelope(TENANT_A);
        final AtomicInteger calls = new AtomicInteger();

        assertTrue(processor.process("direct", envelope, e -> calls.incrementAndGet()));
        assertFalse(processor.process("direct", envelope, e -> calls.incrementAndGet()));

        assertEquals(1, calls.get());
        assertEquals(1, inboxCount(envelope.messageId()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Values set on the calling thread are put back, also when the handler fails; a failing handler leaves no inbox
     * row, so the message can be handled again.
     */
    @Test
    void previousMdcValuesAreRestoredAndFailingHandlerLeavesNoInboxRow() {
        MDCUtils.put(MDCConstant.CALL_ID, "outer-call");
        MDCUtils.put(MDCConstant.ASSOCIATION_ID, "outer-assoc");
        final EventEnvelope envelope = envelope(TENANT_A);

        assertThrows(IllegalStateException.class, () -> processor.process("direct", envelope, e -> {
            assertEquals("call-direct-1", MDCUtils.get(MDCConstant.CALL_ID));
            throw new IllegalStateException("handler failure");
        }));

        assertEquals("outer-call", MDCUtils.get(MDCConstant.CALL_ID));
        assertEquals("outer-assoc", MDCUtils.get(MDCConstant.ASSOCIATION_ID));
        assertTrue(TenantContext.current().isEmpty());
        assertEquals(0, inboxCount(envelope.messageId()));
        assertTrue(processor.process("direct", envelope, e -> {
        }));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * An envelope without tenant is rejected before any scope or transaction opens.
     */
    @Test
    void envelopeWithoutTenantIsRejectedBeforeAnyScope() {
        final EventEnvelope envelope = envelope(null);
        final AtomicInteger calls = new AtomicInteger();

        assertThrows(IllegalArgumentException.class,
                () -> processor.process("direct", envelope, e -> calls.incrementAndGet()));

        assertEquals(0, calls.get());
        assertEquals(0, inboxCount(envelope.messageId()));
        assertNull(MDCUtils.get(MDCConstant.ASSOCIATION_ID));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Builds an envelope of the given type for the raw sender.
     *
     * @param type   envelope type, the routing key is configured on the sender channel
     * @param tenant tenant of the envelope, may be null
     * @return a new envelope
     */
    private static EventEnvelope rawEnvelope(final String type, final UUID tenant) {
        return new EventEnvelope(UUID.randomUUID(), type, 1, tenant, "call-raw-1", "system:other",
                OffsetDateTime.now(), "{}");
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * The same envelope bytes delivered twice by the broker are handled once: one handler call and one inbox row. A
     * later sentinel message proves the consumer already went through the second delivery (the consumer is serial).
     */
    @Test
    void redeliveredEnvelopeIsHandledExactlyOnce() {
        final EventEnvelope duplicate = rawEnvelope("probe.dup", TENANT_A);
        final byte[] bytes = codec.encode(duplicate);

        sender.send(bytes);
        sender.send(bytes);
        final EventEnvelope sentinel = rawEnvelope("probe.dup", TENANT_A);
        sender.send(codec.encode(sentinel));

        Awaitility.await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertNotNull(sideEffects.surroundingsFor(sentinel.messageId())));
        // one more consumer cycle after the sentinel, then the counts must still be 1
        Awaitility.await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals(1, sideEffects.handledFor(duplicate.messageId()).size()));

        assertEquals(1, sideEffects.handledFor(duplicate.messageId()).size());
        assertEquals(1, inboxCount(duplicate.messageId()));
        assertEquals(1, sideEffects.handledFor(sentinel.messageId()).size());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * A handler failure rolls the inbox row back and the message ends on the dead-letter queue.
     */
    @Test
    void failingHandlerLeavesNoInboxRowAndTheMessageIsDeadLettered() {
        sideEffects.register("probe.fail", e -> {
            throw new IllegalStateException("handler failure");
        });
        final EventEnvelope failing = rawEnvelope("probe.fail", TENANT_A);

        sender.send(codec.encode(failing));

        Awaitility.await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertTrue(sideEffects.isDeadLettered(failing.messageId())));
        assertEquals(1, sideEffects.handledFor(failing.messageId()).size());
        assertEquals(0, inboxCount(failing.messageId()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * An envelope without tenant is never handled, leaves no inbox row and is dead-lettered.
     */
    @Test
    void envelopeWithoutTenantIsNeverHandledAndIsDeadLettered() {
        final EventEnvelope tenantless = rawEnvelope("probe.dup", null);

        sender.send(codec.encode(tenantless));

        Awaitility.await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertTrue(sideEffects.isDeadLettered(tenantless.messageId())));
        assertTrue(sideEffects.handledFor(tenantless.messageId()).isEmpty());
        assertEquals(0, inboxCount(tenantless.messageId()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * An event published in a transaction that rolls back is never delivered: nothing reaches the consumer and no
     * outbox row remains. A later committed event proves the relay and the consumer kept running.
     */
    @Test
    void rolledBackPublishIsNeverDelivered() {
        final UUID[] rolledBack = new UUID[1];
        assertThrows(IllegalStateException.class, () -> TenantContext.runAs(TENANT_A, () ->
                QuarkusTransaction.requiringNew().run(() -> {
                    rolledBack[0] = publisher.publish("probe.created", 1, payload());
                    throw new IllegalStateException("rollback");
                })));

        final UUID committed = TenantContext.callAs(TENANT_A, () -> QuarkusTransaction.requiringNew()
                .call(() -> publisher.publish("probe.created", 1, payload())));
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertNotNull(sideEffects.surroundingsFor(committed)));

        assertNotNull(rolledBack[0]);
        assertTrue(sideEffects.handledFor(rolledBack[0]).isEmpty());
        assertEquals(0, inboxCount(rolledBack[0]));
        assertNull(scalar("outbox", "id", "id", rolledBack[0]));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * The application headers of a delivered message are exactly the four ids; only broker-added headers may come in
     * addition. No header carries personal data.
     */
    @Test
    void deliveredMessageCarriesOnlyTheIdHeaders() {
        final UUID id = TenantContext.callAs(TENANT_A, () -> QuarkusTransaction.requiringNew()
                .call(() -> publisher.publish("headers.probe", 1, payload())));

        Awaitility.await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertNotNull(sideEffects.headersFor(id)));

        final Set<String> applicationHeaders = new HashSet<>(sideEffects.headersFor(id));
        applicationHeaders.removeIf(name -> name.startsWith("x-death") || name.startsWith("x-first-death")
                || name.startsWith("x-last-death") || name.startsWith("x-opt-") || name.equals("x-delivery-count")
                || name.equals("x-received-from"));
        assertEquals(Set.of("x-message-id", "x-message-type", "x-tenant-id", "x-call-id"), applicationHeaders);
    }

}
