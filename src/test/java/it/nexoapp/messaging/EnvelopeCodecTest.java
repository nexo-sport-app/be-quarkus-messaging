package it.nexoapp.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import it.nexoapp.messaging.contract.IdentityAccessChanged;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Plain unit tests of the envelope codec: round trip, payload contract and rejection of malformed envelopes.
 */
class EnvelopeCodecTest {

    /* - fixtures - */
    private final EnvelopeCodecImpl codec = new EnvelopeCodecImpl();
    private final JsonMapper json = JsonMapper.builder().build();

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Builds a fully populated envelope around an identity contract.
     *
     * @param payload the serialized payload
     * @return the envelope
     */
    private static EventEnvelope envelope(final String payload) {
        return new EventEnvelope(
                UUID.randomUUID(),
                IdentityAccessChanged.TYPE,
                IdentityAccessChanged.VERSION,
                UUID.randomUUID(),
                "corr-123",
                "actor-" + UUID.randomUUID(),
                OffsetDateTime.parse("2026-11-08T10:15:30.123456+02:00"),
                payload);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Encoding then decoding yields an equal envelope in all 8 components.
     */
    @Test
    void roundTripKeepsEveryComponent() {
        final EventEnvelope original = envelope("{\"a\":1}");

        final EventEnvelope decoded = codec.decode(codec.encode(original));

        assertEquals(original, decoded);
        assertEquals(original.occurredAt().toInstant(), decoded.occurredAt().toInstant());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * The payload deserializes back into the contract record that was serialized into it.
     */
    @Test
    void payloadDecodesIntoContractRecord() {
        final IdentityAccessChanged contract = new IdentityAccessChanged(
                UUID.randomUUID(), UUID.randomUUID(), "https://idp.example/realms/nexo", "sub-1",
                Set.of("PRESIDENT", "COACH"), 7L);
        final EventEnvelope decoded = codec.decode(codec.encode(envelope(codec.toPayload(contract))));

        assertEquals(contract, codec.payload(decoded, IdentityAccessChanged.class));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * The encoded JSON has exactly the eight envelope keys, nothing else.
     *
     * @throws Exception if the encoded bytes cannot be parsed
     */
    @Test
    void encodedJsonHasExactlyTheEnvelopeKeys() throws Exception {
        final JsonNode tree = json.readTree(new String(codec.encode(envelope("{}")), StandardCharsets.UTF_8));
        final List<String> keys = new ArrayList<>();
        tree.fieldNames().forEachRemaining(keys::add);

        assertEquals(
                Set.of("messageId", "type", "version", "tenantId", "correlationId", "actor", "occurredAt", "payload"),
                Set.copyOf(keys));
        assertEquals(8, keys.size());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Bytes without a messageId are rejected.
     */
    @Test
    void decodeRejectsMissingMessageId() {
        assertThrows(IllegalArgumentException.class,
                () -> codec.decode("{\"type\":\"t\",\"version\":1}".getBytes(StandardCharsets.UTF_8)));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Bytes without a type are rejected.
     */
    @Test
    void decodeRejectsMissingType() {
        final String body = "{\"messageId\":\"" + UUID.randomUUID() + "\",\"version\":1}";

        assertThrows(IllegalArgumentException.class, () -> codec.decode(body.getBytes(StandardCharsets.UTF_8)));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Version 0 (or absent, which defaults to 0) is rejected.
     */
    @Test
    void decodeRejectsVersionBelowOne() {
        final String zero = "{\"messageId\":\"" + UUID.randomUUID() + "\",\"type\":\"t\",\"version\":0}";
        final String absent = "{\"messageId\":\"" + UUID.randomUUID() + "\",\"type\":\"t\"}";

        assertThrows(IllegalArgumentException.class, () -> codec.decode(zero.getBytes(StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class, () -> codec.decode(absent.getBytes(StandardCharsets.UTF_8)));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Garbage and empty bytes are rejected with the same exception type.
     */
    @Test
    void decodeRejectsMalformedBytes() {
        assertThrows(IllegalArgumentException.class, () -> codec.decode("not json".getBytes(StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class, () -> codec.decode(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> codec.decode(null));
    }
}
