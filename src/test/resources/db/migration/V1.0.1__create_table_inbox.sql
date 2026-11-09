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
