--liquibase formatted sql

--changeset mineberries:014-task-deadlines
ALTER TABLE task ADD COLUMN completed_late BOOLEAN NOT NULL DEFAULT FALSE,
                 ADD COLUMN last_event_at TIMESTAMPTZ;
ALTER TABLE task ADD CONSTRAINT ck_task_completed_late CHECK (NOT completed_late OR state = 'COMPLETED');

--rollback ALTER TABLE task DROP CONSTRAINT ck_task_completed_late, DROP COLUMN completed_late, DROP COLUMN last_event_at;
