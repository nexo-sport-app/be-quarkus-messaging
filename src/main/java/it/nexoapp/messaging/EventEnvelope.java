package it.nexoapp.messaging;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Versioned envelope wrapping every event that travels over the broker.
 * <p>
 * The envelope metadata carries identifiers only (message id, type, version, tenant id, correlation id and actor).
 * It never carries personal data such as names, emails, tax codes or birth data: whatever business content has to be
 * transported lives in the {@code payload}, which is owned by a versioned contract record.
 *
 * @param messageId     unique identifier of this message, used by consumers to deduplicate redeliveries
 * @param type          logical event type, for example {@code identity.access.changed}
 * @param version       version of the payload contract identified by {@code type}, always 1 or greater
 * @param tenantId      identifier of the association (tenant) the event belongs to
 * @param correlationId identifier tying this message to the request or job that produced it
 * @param actor         identifier of the principal that caused the event (an id, never a name or email)
 * @param occurredAt    instant at which the event happened, with its original offset preserved
 * @param payload       JSON serialization of the contract record carried by this envelope
 */
public record EventEnvelope(
        UUID messageId,
        String type,
        int version,
        UUID tenantId,
        String correlationId,
        String actor,
        OffsetDateTime occurredAt,
        String payload) {

    /**
     * Validates the mandatory components so that no partial envelope can ever exist.
     *
     * @throws IllegalArgumentException if the message id or the type are missing, or the version is below 1
     */
    public EventEnvelope {
        if (messageId == null) {
            throw new IllegalArgumentException("Event envelope without messageId");
        }
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Event envelope without type");
        }
        if (version < 1) {
            throw new IllegalArgumentException("Event envelope version must be at least 1, got " + version);
        }
    }
}
