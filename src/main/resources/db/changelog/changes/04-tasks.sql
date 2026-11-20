--liquibase formatted sql

--changeset mineberries:011-tasks-module
CREATE TABLE task (
    task_id             UUID PRIMARY KEY,
    type                VARCHAR(100) NOT NULL,
    payload             JSONB        NOT NULL,
    state               VARCHAR(20)  NOT NULL,
    last_event_version  BIGINT       NOT NULL DEFAULT -1,
    progress_stage      VARCHAR(100),
    assigned_bot        VARCHAR(16),
    result              JSONB,
    error               JSONB,
    expires_at          TIMESTAMPTZ,
    accept_by           TIMESTAMPTZ,
    finish_by           TIMESTAMPTZ,
    cancel_requested_at TIMESTAMPTZ,
    processed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT ck_task_state CHECK (state IN ('NEW', 'ACCEPTED', 'COMPLETED', 'FAILED', 'CANCELLED')),
    CONSTRAINT ck_task_error CHECK ((state = 'FAILED') = (error IS NOT NULL))
);

CREATE INDEX idx_task_non_terminal ON task (state, created_at)
    WHERE state IN ('NEW', 'ACCEPTED');

CREATE INDEX idx_task_terminal_unprocessed ON task (state, updated_at)
    WHERE state IN ('COMPLETED', 'FAILED', 'CANCELLED') AND processed_at IS NULL;

--rollback DROP TABLE task CASCADE;
