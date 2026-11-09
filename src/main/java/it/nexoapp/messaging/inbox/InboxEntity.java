package it.nexoapp.messaging.inbox;

import it.nexoapp.common.hibernate.entity.AuditEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * A message already handled by a consumer, mapped to the table {@code inbox}.
 * <p>
 * The row is written in the same transaction as the side effects of the handler, so its existence proves that the
 * effects were applied. The table is global: it has no tenant column, because the key {@code (message_id, consumer)}
 * already identifies the message uniquely across associations.
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "inbox")
@IdClass(InboxEntity.Key.class)
public class InboxEntity extends AuditEntity implements Serializable {

    @Serial
    private static final long serialVersionUID = 6120937454810276331L;

    /* identifiers */
    /**
     * Identifier of the handled message, the message id of the envelope.
     */
    @Id
    @Column(
            name = "message_id",
            columnDefinition = "UUID",
            nullable = false,
            updatable = false
    )
    private UUID messageId;

    /**
     * Name of the consumer that handled the message.
     */
    @Id
    @Column(
            name = "consumer",
            columnDefinition = "TEXT",
            nullable = false,
            updatable = false
    )
    private String consumer;

    /* lifecycle */
    /**
     * Instant at which the message was handled.
     */
    @Column(
            name = "processed_at",
            columnDefinition = "TIMESTAMPTZ",
            nullable = false,
            updatable = false
    )
    private OffsetDateTime processedAt;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Composite primary key of the inbox.
     *
     * @param messageId identifier of the handled message
     * @param consumer  name of the consumer that handled it
     */
    public record Key(UUID messageId, String consumer) implements Serializable {
    }

}
