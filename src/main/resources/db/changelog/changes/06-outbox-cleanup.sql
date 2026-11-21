--liquibase formatted sql

--changeset mineberries:013-outbox-cleanup-index runInTransaction:false
CREATE INDEX CONCURRENTLY idx_outbox_sent_retention ON outbox (sent_at, id)
    WHERE sent_at IS NOT NULL;

--rollback DROP INDEX CONCURRENTLY idx_outbox_sent_retention;
