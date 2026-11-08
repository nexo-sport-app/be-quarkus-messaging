package it.nexoapp.messaging;

/**
 * Converts {@link EventEnvelope}s to and from the bytes that travel on the broker, and payload contract records to and
 * from the envelope's JSON payload string.
 */
public interface EnvelopeCodec {

    /**
     * Serializes the whole envelope to UTF-8 JSON bytes.
     *
     * @param envelope the envelope to serialize
     * @return the JSON bytes of the envelope
     * @throws IllegalArgumentException if the envelope is {@code null} or cannot be serialized
     */
    byte[] encode(EventEnvelope envelope);

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Parses broker bytes into an envelope.
     *
     * @param bytes the UTF-8 JSON bytes received from the broker
     * @return the decoded envelope
     * @throws IllegalArgumentException if the bytes are not a valid envelope: malformed JSON, missing messageId or
     *                                  type, or a version below 1
     */
    EventEnvelope decode(byte[] bytes);

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Deserializes the payload of an envelope into its contract record.
     *
     * @param envelope the envelope carrying the payload
     * @param type     the contract record type
     * @param <T>      the contract record type
     * @return the payload as a contract record
     * @throws IllegalArgumentException if the payload is missing or does not match the contract
     */
    <T> T payload(EventEnvelope envelope, Class<T> type);

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Serializes a contract record into the JSON string stored in {@link EventEnvelope#payload()}.
     *
     * @param contract the contract record to serialize
     * @return the JSON payload string
     * @throws IllegalArgumentException if the contract is {@code null} or cannot be serialized
     */
    String toPayload(Object contract);
}
