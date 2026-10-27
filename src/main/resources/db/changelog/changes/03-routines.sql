--liquibase formatted sql

-- changeset mineberries:010-order-status-history-trigger

CREATE OR REPLACE FUNCTION fn_order_status_history()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO order_status_history (
            order_id,
            old_status,
            new_status,
            reason,
            changed_at
        )
        VALUES (
            NEW.order_id,
            OLD.status,
            NEW.status,
            NULL,
            CURRENT_TIMESTAMP
        );
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_order_status_history
AFTER UPDATE OF status ON delivery_order
FOR EACH ROW
WHEN (OLD.status IS DISTINCT FROM NEW.status)
EXECUTE FUNCTION fn_order_status_history();

-- rollback DROP TRIGGER trg_order_status_history ON delivery_order;
-- rollback DROP FUNCTION fn_order_status_history();