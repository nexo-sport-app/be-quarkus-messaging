package it.nexoapp.messaging;

import io.quarkus.arc.Arc;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import org.eclipse.microprofile.reactive.messaging.Incoming;

/**
 * Test consumer of the {@code probe-in} channel: hands every envelope to the processor and records the state of the
 * consumer thread around the call.
 */
@ApplicationScoped
@RequiredArgsConstructor
public class ProbeConsumer {

    /** Consumer name used as inbox key. */
    public static final String CONSUMER = "probe";

    /* - collaborators - */
    private final IncomingMessageProcessor processor;
    private final EnvelopeCodec codec;
    private final ProbeSideEffects sideEffects;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Decodes and processes one message.
     *
     * @param body the envelope bytes
     */
    @Incoming("probe-in")
    @Blocking
    public void onMessage(final byte[] body) {
        final EventEnvelope envelope = codec.decode(body);
        final boolean requestActive = Arc.container().requestContext().isActive();
        processor.process(CONSUMER, envelope, sideEffects::handle);
        sideEffects.afterProcessing(envelope.messageId(), requestActive);
    }

}
