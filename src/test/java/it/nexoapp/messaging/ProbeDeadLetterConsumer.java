package it.nexoapp.messaging;

import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import org.eclipse.microprofile.reactive.messaging.Incoming;

/**
 * Test consumer of the dead-letter queue of the probe consumer: records the id of every dead-lettered message.
 */
@ApplicationScoped
@RequiredArgsConstructor
public class ProbeDeadLetterConsumer {

    /* - collaborators - */
    private final EnvelopeCodec codec;
    private final ProbeSideEffects sideEffects;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Records one dead-lettered message.
     *
     * @param body the envelope bytes
     */
    @Incoming("probe-dlq")
    public void onMessage(final byte[] body) {
        sideEffects.deadLettered(codec.decode(body).messageId());
    }

}
