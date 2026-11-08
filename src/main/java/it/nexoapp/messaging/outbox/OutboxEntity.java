package it.nexoapp.messaging.outbox;

import it.nexoapp.common.hibernate.entity.AuditEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.UuidGenerator;

import java.io.Serial;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * An event waiting in the outbox of a service, mapped to the table {@code outbox}.
 * <p>
 * The row is written in the transaction that produced the event and is delivered to the broker by the relay, which
 * then fills {@code published_at}. The table is global: {@code tenant_id} is a plain data column, not a Hibernate
 * tenant discriminator, because the relay reads the rows of every association.
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "outbox")
public class OutboxEntity extends AuditEntity implements Serializable {

    @Serial
    private static final long serialVersionUID = 3318045920467159214L;

    /* identifiers */
    /**
     * Identifies the event uniquely; it travels as the message id that consumers use to deduplicate.
     */
    @Id
    @UuidGenerator(style = UuidGenerator.Style.VERSION_7)
    @Column(
            name = "id",
            columnDefinition = "UUID",
            nullable = false,
            unique = true
    )
    private UUID id;

    /**
     * Association (tenant) the event belongs to.
     */
    @Column(
            name = "tenant_id",
            columnDefinition = "UUID",
            nullable = false,
            updatable = false
    )
    private UUID tenantId;

    /* event data */
    /**
     * Logical event type, for example {@code identity.access.changed}.
     */
    @Column(
            name = "type",
            columnDefinition = "TEXT",
            nullable = false,
            updatable = false
    )
    private String type;

    /**
     * Version of the payload contract identified by the type, 1 or greater.
     */
    @Column(
            name = "version",
            columnDefinition = "INTEGER",
            nullable = false,
            updatable = false
    )
    private int version;

    /**
     * Routing key used on the topic exchange of the service.
     */
    @Column(
            name = "routing_key",
            columnDefinition = "TEXT",
            nullable = false,
            updatable = false
    )
    private String routingKey;

    /**
     * Identifier tying the event to the request or job that produced it.
     */
    @Column(
            name = "correlation_id",
            columnDefinition = "TEXT",
            nullable = false,
            updatable = false
    )
    private String correlationId;

    /**
     * Audit actor that caused the event: an id, never a name or an email.
     */
    @Column(
            name = "actor",
            columnDefinition = "TEXT",
            nullable = false,
            updatable = false
    )
    private String actor;

    /**
     * JSON serialization of the contract record carried by the event.
     */
    @Column(
            name = "payload",
            columnDefinition = "TEXT",
            nullable = false,
            updatable = false
    )
    private String payload;

    /* lifecycle */
    /**
     * Instant at which the event happened.
     */
    @Column(
            name = "occurred_at",
            columnDefinition = "TIMESTAMPTZ",
            nullable = false,
            updatable = false
    )
    private OffsetDateTime occurredAt;

    /**
     * Instant at which the broker confirmed the event, empty while the event is still pending.
     */
    @Column(
            name = "published_at",
            columnDefinition = "TIMESTAMPTZ"
    )
    private OffsetDateTime publishedAt;

}
