package it.nexoapp.messaging.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;

/**
 * Configuration of the messaging library, mapped from the properties with the {@code nexo.messaging} prefix.
 * <p>
 * Every value has a default, so a service configures only what it wants to change.
 */
@ConfigMapping(prefix = "nexo.messaging")
public interface MessagingConfig {

    /**
     * Settings of the outbox relay (properties {@code nexo.messaging.relay.*}).
     */
    Relay relay();

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Settings of the outbox relay that delivers pending events to the broker.
     */
    interface Relay {

        /**
         * Interval between two relay runs (property {@code nexo.messaging.relay.every}), measured as elapsed time.
         * Optional, defaults to one second. The scheduled job reads the same property with the same default.
         */
        @WithDefault("1s")
        Duration every();

        // -------------------------------------------------------------------------------------------------------------

        /**
         * Maximum number of pending events claimed by one relay run (property {@code nexo.messaging.relay.batch-size}).
         * Optional, defaults to 50.
         */
        @WithDefault("50")
        int batchSize();

        // -------------------------------------------------------------------------------------------------------------

        /**
         * Maximum time a relay run waits for the broker to confirm the events it sent (property
         * {@code nexo.messaging.relay.confirm-timeout}). Events not confirmed in time stay pending and are retried.
         * Optional, defaults to ten seconds.
         */
        @WithDefault("10s")
        Duration confirmTimeout();

    }

}
