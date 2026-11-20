--liquibase formatted sql

--changeset mineberries:012-outbox-module
CREATE TABLE outbox (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id      UUID        NOT NULL,
    kind            VARCHAR(10) NOT NULL,
    type            VARCHAR(100) NOT NULL,
    payload         JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at         TIMESTAMPTZ,
    attempts        INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT,
    CONSTRAINT ck_outbox_kind CHECK (kind IN ('task', 'cancel')),
    CONSTRAINT uq_outbox_message_kind UNIQUE (message_id, kind)
);

CREATE INDEX idx_outbox_unsent ON outbox (id)
    WHERE sent_at IS NULL;

--rollback DROP TABLE outbox CASCADE;
