package it.nexoapp.messaging.outbox;

import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;
import it.nexoapp.common.tenant.TenantContext;
import it.nexoapp.messaging.EnvelopeCodec;
import it.nexoapp.messaging.EventEnvelope;
import it.nexoapp.messaging.config.MessagingConfig;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Delivers pending outbox rows to the RabbitMQ topic exchange of the service.
 * <p>
 * Every run claims the oldest pending rows with {@code FOR UPDATE SKIP LOCKED}, sends them on the confirmed channel
 * {@code nexo-outbox} and sets {@code published_at} only for the rows the broker confirmed, all in one transaction.
 * Rows that were nacked or not confirmed in time stay pending and are retried by the next run. A crash between the
 * confirm and the commit can deliver a row twice, which is why consumers deduplicate on the message id.
 * <p>
 * The job is scheduled by elapsed interval and the instants are {@code TIMESTAMPTZ}: daylight saving time changes
 * cannot skip, double or reorder runs.
 */
@ApplicationScoped
class OutboxRelay {

    /** Identity of the scheduled job, used to pause and resume it. */
    static final String JOB_IDENTITY = "nexo-outbox-relay";

    /* - logger - */
    private static final Logger logger = LoggerFactory.getLogger(OutboxRelay.class);

    /* - collaborators - */
    private final OutboxRepository repository;
    private final EnvelopeCodec codec;
    private final MessagingConfig config;
    private final Emitter<byte[]> emitter;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates the relay.
     *
     * @param repository outbox data access
     * @param codec      envelope codec
     * @param config     messaging configuration
     * @param emitter    emitter of the broker-confirmed outgoing channel
     */
    OutboxRelay(final OutboxRepository repository,
                final EnvelopeCodec codec,
                final MessagingConfig config,
                @Channel("nexo-outbox") final Emitter<byte[]> emitter) {
        this.repository = repository;
        this.codec = codec;
        this.config = config;
        this.emitter = emitter;
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Runs one relay pass outside every tenant, in its own transaction. A failure is logged and leaves the rows
     * pending, so the next pass retries them.
     */
    @Scheduled(
            identity = JOB_IDENTITY,
            every = "${nexo.messaging.relay.every:1s}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP
    )
    void relay() {
        try {
            // the scope is opened BEFORE the transaction: Hibernate resolves the tenant when the session opens
            TenantContext.runUnscoped(() -> QuarkusTransaction.requiringNew().call(this::publishPending));
        } catch (final RuntimeException e) {
            logger.warn("Outbox relay run failed, pending rows are retried: {}", e.toString());
            logger.debug("Outbox relay failure detail", e);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Claims, sends and marks one batch inside the current transaction.
     *
     * @return the number of rows confirmed by the broker and marked as published
     */
    private int publishPending() {
        final List<OutboxEntity> claimed = repository.claimPending(config.relay().batchSize());
        if (claimed.isEmpty()) {
            return 0;
        }

        final List<CompletableFuture<Void>> confirms = new ArrayList<>(claimed.size());
        for (final OutboxEntity row : claimed) {
            confirms.add(send(row));
        }

        final long deadline = System.nanoTime() + config.relay().confirmTimeout().toNanos();
        final OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        int published = 0;
        for (int i = 0; i < claimed.size(); i++) {
            if (isConfirmed(confirms.get(i), deadline)) {
                claimed.get(i).setPublishedAt(now);
                published++;
            }
        }
        logger.debug("Outbox relay: {} of {} claimed rows confirmed by the broker", published, claimed.size());
        return published;
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Sends one row on the confirmed channel.
     *
     * @param row the outbox row to send
     * @return a future completed when the broker confirms the message and failed when it nacks it or cannot take it
     */
    private CompletableFuture<Void> send(final OutboxEntity row) {
        final CompletableFuture<Void> confirm = new CompletableFuture<>();
        try {
            final byte[] body = codec.encode(new EventEnvelope(
                    row.getId(), row.getType(), row.getVersion(), row.getTenantId(),
                    row.getCorrelationId(), row.getActor(), row.getOccurredAt(), row.getPayload()));
            final Metadata metadata = Metadata.of(OutgoingRabbitMQMetadata.builder()
                    .withRoutingKey(row.getRoutingKey())
                    .withMessageId(row.getId().toString())
                    .withContentType("application/json")
                    .withDeliveryMode(2)
                    .withHeader("x-message-id", row.getId().toString())
                    .withHeader("x-message-type", row.getType())
                    .withHeader("x-tenant-id", row.getTenantId().toString())
                    .withHeader("x-call-id", row.getCorrelationId())
                    .build());
            emitter.send(Message.of(body, metadata,
                    () -> {
                        confirm.complete(null);
                        return CompletableFuture.completedFuture(null);
                    },
                    failure -> {
                        confirm.completeExceptionally(failure);
                        return CompletableFuture.<Void>completedFuture(null);
                    }));
        } catch (final RuntimeException e) {
            logger.warn("Outbox row {} could not be sent, it stays pending: {}", row.getId(), e.toString());
            confirm.completeExceptionally(e);
        }
        return confirm;
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Waits for one confirmation until the shared deadline.
     *
     * @param confirm  the confirmation future of a row
     * @param deadline {@link System#nanoTime()} value after which an unconfirmed row counts as not confirmed
     * @return true only when the broker confirmed the row in time
     */
    private boolean isConfirmed(final CompletableFuture<Void> confirm, final long deadline) {
        try {
            confirm.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            return true;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (final Exception e) {
            logger.debug("Outbox row not confirmed: {}", e.toString());
            return false;
        }
    }

}
