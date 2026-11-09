CREATE TABLE probe_record (
    id              UUID DEFAULT gen_random_uuid() NOT NULL,
    tenant_id       UUID NOT NULL,
    label           VARCHAR(40) NOT NULL,
    created_by      TEXT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_by      TEXT NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT now() NOT NULL,
    CONSTRAINT probe_record_pk PRIMARY KEY (id)
);
CREATE INDEX idx_probe_record_tenant_id ON probe_record(tenant_id);
