package it.nexoapp.messaging;

import it.nexoapp.common.constant.MDCConstant;
import it.nexoapp.common.context.ExecutionScope;
import it.nexoapp.common.tenant.TenantContext;
import it.nexoapp.common.utils.MDCUtils;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Test recorder of what the probe consumer's handler observed. Tests may register handlers per envelope type; without
 * one the handler only records.
 */
@ApplicationScoped
public class ProbeSideEffects {

    /**
     * What the handler saw while handling one message.
     *
     * @param tenant        tenant of the thread, empty string when none
     * @param callId        MDC call id, null when unset
     * @param associationId MDC association id, null when unset
     * @param actor         audit actor of the thread, null when none
     */
    public record Handled(String tenant, String callId, String associationId, String actor) {
    }

    /**
     * What the consumer thread looked like around the processor call.
     *
     * @param requestContextActiveOnEntry true when the CDI request context was active before the processor was called
     * @param tenantAfter                 tenant of the thread after processing, empty string when none
     * @param callIdAfter                 MDC call id after processing, null when unset
     * @param associationIdAfter          MDC association id after processing, null when unset
     * @param actorAfter                  audit actor after processing, null when none
     */
    public record Surroundings(boolean requestContextActiveOnEntry, String tenantAfter, String callIdAfter,
                               String associationIdAfter, String actorAfter) {
    }

    /* - state - */
    private final Map<UUID, List<Handled>> handled = new ConcurrentHashMap<>();
    private final Map<UUID, Surroundings> surroundings = new ConcurrentHashMap<>();
    private final Map<String, Consumer<EventEnvelope>> handlers = new ConcurrentHashMap<>();
    private final Set<UUID> deadLettered = ConcurrentHashMap.newKeySet();
    private final Map<UUID, Set<String>> headerNames = new ConcurrentHashMap<>();

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Records what the current thread sees and runs the handler registered for the envelope type, if any.
     *
     * @param envelope the envelope being handled
     */
    public void handle(final EventEnvelope envelope) {
        handled.computeIfAbsent(envelope.messageId(), k -> new CopyOnWriteArrayList<>()).add(new Handled(
                TenantContext.current().orElse(""),
                MDCUtils.get(MDCConstant.CALL_ID),
                MDCUtils.get(MDCConstant.ASSOCIATION_ID),
                ExecutionScope.currentActor().orElse(null)));
        final Consumer<EventEnvelope> custom = handlers.get(envelope.type());
        if (custom != null) {
            custom.accept(envelope);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Records the state of the consumer thread right after the processor returned.
     *
     * @param messageId       message just processed
     * @param requestActiveOn true when the request context was active on entry
     */
    public void afterProcessing(final UUID messageId, final boolean requestActiveOn) {
        surroundings.put(messageId, new Surroundings(requestActiveOn,
                TenantContext.current().orElse(""),
                MDCUtils.get(MDCConstant.CALL_ID),
                MDCUtils.get(MDCConstant.ASSOCIATION_ID),
                ExecutionScope.currentActor().orElse(null)));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Registers a handler for an envelope type, replacing a previous one.
     *
     * @param type    envelope type
     * @param handler handler run after the recording
     */
    public void register(final String type, final Consumer<EventEnvelope> handler) {
        handlers.put(type, handler);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Handlings recorded for a message.
     *
     * @param messageId message id
     * @return the recorded handlings, empty when none
     */
    public List<Handled> handledFor(final UUID messageId) {
        return handled.getOrDefault(messageId, List.of());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Consumer thread state recorded after a message.
     *
     * @param messageId message id
     * @return the recorded state, null when not yet processed
     */
    public Surroundings surroundingsFor(final UUID messageId) {
        return surroundings.get(messageId);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Records that a message arrived on the dead-letter queue.
     *
     * @param messageId id of the dead-lettered message
     */
    public void deadLettered(final UUID messageId) {
        deadLettered.add(messageId);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tells whether a message arrived on the dead-letter queue.
     *
     * @param messageId message id
     * @return true when the message was dead-lettered
     */
    public boolean isDeadLettered(final UUID messageId) {
        return deadLettered.contains(messageId);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Records the AMQP header names a delivered message carried.
     *
     * @param messageId message id
     * @param names     header names
     */
    public void headers(final UUID messageId, final Set<String> names) {
        headerNames.put(messageId, Set.copyOf(names));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * AMQP header names recorded for a message.
     *
     * @param messageId message id
     * @return the header names, null when the message was not delivered to the header consumer
     */
    public Set<String> headersFor(final UUID messageId) {
        return headerNames.get(messageId);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Forgets everything recorded and every registered handler.
     */
    public void reset() {
        handled.clear();
        surroundings.clear();
        handlers.clear();
        deadLettered.clear();
        headerNames.clear();
    }

}
