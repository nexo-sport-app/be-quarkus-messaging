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
