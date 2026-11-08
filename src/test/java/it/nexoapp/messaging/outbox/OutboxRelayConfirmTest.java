package it.nexoapp.messaging.outbox;

import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.scheduler.Scheduler;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;
import it.nexoapp.common.tenant.TenantContext;
import it.nexoapp.messaging.EnvelopeCodec;
import it.nexoapp.messaging.EventPublisher;
import it.nexoapp.messaging.config.MessagingConfig;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Proves that the relay marks a row as published only when the broker confirmed it: a scripted emitter acks, nacks
 * or never answers depending on the routing key, and only the acked row gets {@code published_at}.
 */
@QuarkusTest
class OutboxRelayConfirmTest {

    /* - tenants - */
    private static final UUID TENANT_A = UUID.fromString("11111111-1111-4111-8111-111111111111");

    /* - collaborators - */
    @Inject
    EventPublisher publisher;

    @Inject
    OutboxRepository repository;

    @Inject
    EnvelopeCodec codec;

    @Inject
    EntityManager em;

    @Inject
    Scheduler scheduler;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Resumes the real relay job and removes the rows of the test.
     */
    @AfterEach
    void cleanUp() {
        scheduler.resume(OutboxRelay.JOB_IDENTITY);
        TenantContext.runUnscoped(() -> QuarkusTransaction.requiringNew()
                .run(() -> em.createNativeQuery("DELETE FROM {h-schema}outbox").executeUpdate()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Emitter that answers according to the routing key: {@code test.acked} is confirmed, {@code test.nacked} is
     * rejected, anything else is never answered.
     */
    private static final class ScriptedEmitter implements Emitter<byte[]> {

        /**
         * {@inheritDoc}
         */
        @Override
        public CompletionStage<Void> send(final byte[] msg) {
            throw new UnsupportedOperationException();
        }

        // -------------------------------------------------------------------------------------------------------------

        /**
         * {@inheritDoc}
         */
        @Override
        public <M extends Message<? extends byte[]>> void send(final M msg) {
            final String routingKey = msg.getMetadata(OutgoingRabbitMQMetadata.class).orElseThrow().getRoutingKey();
            if ("test.acked".equals(routingKey)) {
                msg.ack();
            } else if ("test.nacked".equals(routingKey)) {
                msg.nack(new IllegalStateException("broker nack"));
            }
        }

        // -------------------------------------------------------------------------------------------------------------

        /**
         * {@inheritDoc}
         */
        @Override
        public void complete() {
        }

        // -------------------------------------------------------------------------------------------------------------

        /**
         * {@inheritDoc}
         */
        @Override
        public void error(final Exception e) {
        }

        // -------------------------------------------------------------------------------------------------------------

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCancelled() {
            return false;
        }

        // -------------------------------------------------------------------------------------------------------------

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasRequests() {
            return true;
        }

    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Configuration with a short confirm timeout so the unanswered row does not slow the test down.
     */
    private static final class FastConfig implements MessagingConfig {

        /**
         * {@inheritDoc}
         */
        @Override
        public Relay relay() {
            return new Relay() {
                @Override
                public Duration every() {
                    return Duration.ofSeconds(1);
                }

                @Override
                public int batchSize() {
                    return 50;
                }

                @Override
                public Duration confirmTimeout() {
                    return Duration.ofMillis(300);
                }
            };
        }

    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Publishes one event with the given type in its own committed transaction.
     *
     * @param type event type, also the routing key
     * @return the event id
     */
    private UUID publish(final String type) {
        return TenantContext.callAs(TENANT_A, () -> QuarkusTransaction.requiringNew()
                .call(() -> publisher.publish(type, 1, new Ping("x"))));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Reads the published instant of a row.
     *
     * @param id row identifier
     * @return the published instant, null while the row is pending
     */
    private Object publishedAt(final UUID id) {
        return TenantContext.callUnscoped(() -> QuarkusTransaction.requiringNew().call(() -> em
                .createNativeQuery("SELECT published_at FROM {h-schema}outbox WHERE id = :id")
                .setParameter("id", id)
                .getSingleResult()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Minimal payload contract for the scripted events.
     *
     * @param note free text
     */
    public record Ping(String note) {
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Only the confirmed row is marked; the nacked and the unanswered rows stay pending for the next run.
     */
    @Test
    void onlyConfirmedRowsAreMarkedPublished() throws InterruptedException {
        scheduler.pause(OutboxRelay.JOB_IDENTITY);
        // let a run of the real relay that started before the pause finish
        Thread.sleep(500);

        final UUID acked = publish("test.acked");
        final UUID nacked = publish("test.nacked");
        final UUID silent = publish("test.silent");

        final OutboxRelay relay = new OutboxRelay(repository, codec, new FastConfig(), new ScriptedEmitter());
        relay.relay();

        assertNotNull(publishedAt(acked));
        assertNull(publishedAt(nacked));
        assertNull(publishedAt(silent));
        assertEquals(1, TenantContext.callUnscoped(() -> QuarkusTransaction.requiringNew().call(() -> ((Number) em
                .createNativeQuery("SELECT count(*) FROM {h-schema}outbox WHERE published_at IS NOT NULL")
                .getSingleResult()).intValue())));
    }

}
