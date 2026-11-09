package it.nexoapp.messaging;

import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.concurrent.CompletionStage;

/**
 * Test consumer that records the AMQP header names of the messages delivered to it.
 */
@ApplicationScoped
@RequiredArgsConstructor
public class ProbeHeaderConsumer {

    /* - collaborators - */
    private final EnvelopeCodec codec;
    private final ProbeSideEffects sideEffects;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Records the header names of one delivered message and acknowledges it.
     *
     * @param message the delivered message
     * @return the acknowledgement
     */
    @Incoming("probe-headers")
    public CompletionStage<Void> onMessage(final Message<byte[]> message) {
        final var metadata = message.getMetadata(IncomingRabbitMQMetadata.class).orElseThrow();
        sideEffects.headers(codec.decode(message.getPayload()).messageId(), metadata.getHeaders().keySet());
        return message.ack();
    }

}
