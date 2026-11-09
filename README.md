# be-quarkus-messaging

Messaging library of the Nexo platform: a transactional outbox, a broker-confirmed relay, an idempotent inbox processor
and a versioned event envelope on top of RabbitMQ. Services depend on it to publish and consume events without ever
publishing straight from a REST transaction (no dual write) and without handling a redelivered message twice.

## What it gives you

| Piece | Purpose |
|-------|---------|
| `EventPublisher.publish(type, version, payload)` | Writes the event to the `outbox` table in the caller's transaction. The transaction is mandatory and so is a tenant (explicit tenant scope or a linked request). |
| `OutboxRelay` (internal, scheduled) | Claims pending rows with `FOR UPDATE SKIP LOCKED`, sends them on the confirmed channel `nexo-outbox` and marks a row published only after the broker confirmed it. |
| `IncomingMessageProcessor.process(consumer, envelope, handler)` | Opens the tenant of the envelope, the system actor and the correlation id, then inserts `(message_id, consumer)` into the `inbox` and runs the handler in the same transaction. A duplicate skips the handler. |
| `EventEnvelope`, `EnvelopeCodec` | Versioned envelope (`messageId`, `type`, `version`, `tenantId`, `correlationId`, `actor`, `occurredAt`, `payload`) and its JSON codec. |
| `contract.*` | Versioned payload records shared by services (for example `IdentityAccessChanged`). |

## Adoption in a service

### 1. Dependency

```xml
<dependency>
    <groupId>it.nexoapp</groupId>
    <artifactId>messaging</artifactId>
</dependency>
```

The library already brings `quarkus-messaging-rabbitmq` and `quarkus-scheduler`. The service needs the shared
`common` library (tenant context, actors) and a tenant-aware Hibernate configuration:

```properties
app.code=usr
quarkus.hibernate-orm.multitenant=DISCRIMINATOR
quarkus.hibernate-orm.database.default-schema=<service schema>
quarkus.datasource.jdbc.additional-jdbc-properties.stringtype=unspecified
```

### 2. Own Flyway copy of the outbox and inbox tables

The library ships no migrations: each service owns its schema history. Copy the two blocks below verbatim into the
service's own migrations, using the next free version numbers (for example `V1.0.N__create_table_outbox.sql` and
`V1.0.M__create_table_inbox.sql`). They create the tables in the service's default schema.

`outbox`:

```sql
CREATE TABLE outbox (
    id              UUID DEFAULT gen_random_uuid() NOT NULL,
    tenant_id       UUID NOT NULL,
    type            TEXT NOT NULL,
    version         INTEGER NOT NULL,
    routing_key     TEXT NOT NULL,
    correlation_id  TEXT NOT NULL,
    actor           TEXT NOT NULL,
    payload         TEXT NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    published_at    TIMESTAMPTZ,
    created_by      TEXT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_by      TEXT NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT now() NOT NULL,
    CONSTRAINT outbox_pk PRIMARY KEY (id)
);
CREATE INDEX idx_outbox_published_at_occurred_at ON outbox(published_at, occurred_at);
```

`inbox`:

```sql
CREATE TABLE inbox (
    message_id      UUID NOT NULL,
    consumer        TEXT NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL,
    created_by      TEXT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_by      TEXT NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT now() NOT NULL,
    CONSTRAINT inbox_pk PRIMARY KEY (message_id, consumer)
);
```

`outbox` carries a plain `tenant_id` column that the relay reads outside every tenant (`TenantContext.runUnscoped`);
both tables are global, not tenant-filtered by Hibernate. Native SQL is allowed only inside the `outbox` and `inbox`
packages of this library (enforced by the shared architecture rules).

### 3. Publishing

```java
@Transactional
public void changeRoles(...) {
    // ... change the data ...
    eventPublisher.publish("identity.access.changed", 1, new IdentityAccessChanged(...));
}
```

`publish` runs in the caller's transaction, so the event and the data commit or roll back together. The relay delivers
the row afterwards, at least once. Without a tenant (no scope, no linked request, or inside an unscoped block) or
without a transaction the call fails and inserts nothing.

### 4. Channel configuration

The library configures the outgoing channel `nexo-outbox` (topic exchange `nexo.<app.code>`, durable, publisher
confirms). A service only declares its incoming channels, one per consumer queue:

```properties
mp.messaging.incoming.access-in.connector=smallrye-rabbitmq
mp.messaging.incoming.access-in.exchange.name=nexo.<producer app.code>
mp.messaging.incoming.access-in.exchange.type=topic
mp.messaging.incoming.access-in.exchange.durable=true
mp.messaging.incoming.access-in.queue.name=<app.code>.access
mp.messaging.incoming.access-in.queue.durable=true
mp.messaging.incoming.access-in.queue.x-queue-type=quorum
mp.messaging.incoming.access-in.routing-keys=identity.access.#
mp.messaging.incoming.access-in.auto-bind-dlq=true
mp.messaging.incoming.access-in.dlx.declare=true
mp.messaging.incoming.access-in.failure-strategy=reject
mp.messaging.incoming.access-in.max-outstanding-messages=10
```

`auto-bind-dlq` needs `dlx.declare=true` (or a pre-declared dead-letter exchange). Relay tuning (all optional):
`nexo.messaging.relay.every` (default `1s`), `nexo.messaging.relay.batch-size` (default `50`),
`nexo.messaging.relay.confirm-timeout` (default `10s`).

### 5. Consumer pattern

```java
@ApplicationScoped
@RequiredArgsConstructor
class AccessConsumer {

    private final IncomingMessageProcessor processor;
    private final EnvelopeCodec codec;

    @Incoming("access-in")
    @Blocking
    void onMessage(final byte[] body) {
        processor.process("access", codec.decode(body), this::apply);
    }

    private void apply(final EventEnvelope envelope) {
        // runs inside the tenant of the envelope, the system actor and one transaction with the inbox row
    }
}
```

Rules for consumers:

- The consumer name (`"access"`) is part of the deduplication key: one name per logical consumer.
- Never catch the handler's exception: it must reach the broker. The processor rolls the inbox row back, the exception
  propagates and `failure-strategy=reject` dead-letters the message.
- The CDI request context is already active in `@Incoming @Blocking` methods; no extra annotation is needed.
- An envelope without tenant is rejected before any scope or transaction opens and is dead-lettered.

## Topology conventions

- Exchange: one durable topic exchange per producing service, named `nexo.<app.code>`.
- Routing key: the event `type`, for example `identity.access.changed`.
- Queues: durable quorum queues (`x-queue-type=quorum`), one per consumer.
- Dead-lettering: failed or malformed messages go to `<queue>.dlq` (for example `msg.probe.dlq`), declared
  automatically through `auto-bind-dlq`. Re-driving dead letters is manual in Phase 1 and gets tooling in Phase 3.
- Delivery is at least once; exactly-once effect comes from the inbox primary key `(message_id, consumer)`.

## Ids-only rule

Event envelope metadata and AMQP headers carry identifiers only: `x-message-id`, `x-message-type`, `x-tenant-id` and
`x-call-id` (plus broker-added headers). Never put names, emails, tax codes or birth data in the envelope metadata or in
a header; whatever business content must travel lives in the payload of a versioned contract record, and even there
prefer ids over personal data. `OutboxInboxTest` asserts the exact header set.

## Daylight saving time

The relay is scheduled by elapsed interval (`every`), not by wall-clock cron, and every instant is stored as
`TIMESTAMPTZ`. A daylight saving change cannot skip, double or reorder relay runs.
