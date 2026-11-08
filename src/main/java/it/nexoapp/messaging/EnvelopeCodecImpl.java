package it.nexoapp.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;

/**
 * Default {@link EnvelopeCodec}: explicit Jackson serialization of the whole envelope to UTF-8 JSON, with the payload
 * stored as a JSON string.
 */
@ApplicationScoped
class EnvelopeCodecImpl implements EnvelopeCodec {

    /* - mapper - */
    private final JsonMapper mapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            // keep the offset the producer wrote, so a round trip yields an equal envelope
            .configure(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE, false)
            // forward compatibility: a newer producer may add fields to a contract
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .build();

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] encode(final EventEnvelope envelope) {
        if (envelope == null) {
            throw new IllegalArgumentException("Cannot encode a null envelope");
        }
        try {
            return mapper.writeValueAsString(envelope).getBytes(StandardCharsets.UTF_8);
        } catch (final JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot encode envelope " + envelope.messageId(), e);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public EventEnvelope decode(final byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("Cannot decode an empty message");
        }
        try {
            return mapper.readValue(new String(bytes, StandardCharsets.UTF_8), EventEnvelope.class);
        } catch (final JsonProcessingException e) {
            // an invalid envelope (record constructor rejection) surfaces as the cause of the Jackson failure
            if (e.getCause() instanceof final IllegalArgumentException invalid) {
                throw invalid;
            }
            throw new IllegalArgumentException("Malformed event envelope", e);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T payload(final EventEnvelope envelope, final Class<T> type) {
        if (envelope == null || envelope.payload() == null) {
            throw new IllegalArgumentException("Envelope without payload");
        }
        try {
            return mapper.readValue(envelope.payload(), type);
        } catch (final JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "Payload of " + envelope.messageId() + " does not match " + type.getSimpleName(), e);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public String toPayload(final Object contract) {
        if (contract == null) {
            throw new IllegalArgumentException("Cannot serialize a null payload");
        }
        try {
            return mapper.writeValueAsString(contract);
        } catch (final JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize payload " + contract.getClass().getSimpleName(), e);
        }
    }
}
