package it.nexoapp.messaging;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

/**
 * Test sender that puts raw envelope bytes on the exchange of the library, bypassing the outbox, to simulate
 * redeliveries and malformed traffic.
 */
@ApplicationScoped
public class ProbeSender {

    /* - channel - */
    private final Emitter<byte[]> emitter;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates the sender.
     *
     * @param emitter emitter of the {@code probe-dup} channel (routing key {@code probe.dup})
     */
    ProbeSender(@Channel("probe-dup") final Emitter<byte[]> emitter) {
        this.emitter = emitter;
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Sends raw bytes to the exchange.
     *
     * @param body the bytes to send
     */
    public void send(final byte[] body) {
        emitter.send(body);
    }

}
