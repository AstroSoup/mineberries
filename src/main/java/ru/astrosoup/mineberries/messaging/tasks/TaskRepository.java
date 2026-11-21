package ru.astrosoup.mineberries.messaging.tasks;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import ru.astrosoup.mineberries.messaging.common.TaskState;

/**
 * Persists task state for service A.
 *
 * <p>JSON columns are passed and returned as raw JSON text; SQL casts handle the
 * {@code jsonb} conversion. The repository contains no broker or TaskManager code.
 *
 * <p>{@link #findByIdForUpdate(UUID)} relies on the caller's transaction. The row
 * lock is held until that transaction commits or rolls back.
 */
@Repository
public class TaskRepository {

    private static final String COLUMNS =
            "task_id, type, payload, state, last_event_version, progress_stage, "
            + "assigned_bot, result, error, expires_at, accept_by, finish_by, "
            + "cancel_requested_at, processed_at, created_at, updated_at";

    private static final String SELECT_BY_ID_FOR_UPDATE =
            "SELECT " + COLUMNS + " FROM task WHERE task_id = ? FOR UPDATE";

    private static final String SELECT_ACTIVE =
            "SELECT " + COLUMNS + " FROM task "
            + "WHERE state IN ('NEW', 'ACCEPTED') ORDER BY created_at, task_id";

    private static final String SELECT_TERMINAL_UNPROCESSED =
            "SELECT " + COLUMNS + " FROM task "
            + "WHERE state IN ('COMPLETED', 'FAILED', 'CANCELLED') AND processed_at IS NULL "
            + "ORDER BY updated_at, task_id";

    private static final RowMapper<TaskRecord> ROW_MAPPER = (row, rowNumber) -> new TaskRecord(
            row.getObject("task_id", UUID.class),
            row.getString("type"),
            row.getString("payload"),
            TaskState.valueOf(row.getString("state")),
            row.getLong("last_event_version"),
            row.getString("progress_stage"),
            row.getString("assigned_bot"),
            row.getString("result"),
            row.getString("error"),
            instant(row, "expires_at"),
            instant(row, "accept_by"),
            instant(row, "finish_by"),
            instant(row, "cancel_requested_at"),
            instant(row, "processed_at"),
            instant(row, "created_at"),
            instant(row, "updated_at"));

    private final JdbcClient jdbc;

    public TaskRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts a task in {@code NEW} state.
     *
     * @param task task data
     */
    public void insert(NewTask task) {
        jdbc.sql("""
                INSERT INTO task (
                    task_id, type, payload, state, expires_at, accept_by, finish_by
                )
                VALUES (?, ?, CAST(? AS jsonb), ?, ?, ?, ?)
                """)
                .param(1, task.taskId())
                .param(2, task.type())
                .param(3, opaqueText(task.payload()))
                .param(4, TaskState.NEW.name())
                .param(5, timestamp(task.expiresAt()))
                .param(6, timestamp(task.acceptBy()))
                .param(7, timestamp(task.finishBy()))
                .update();
    }

    /**
     * Reads a task and locks its row until the current transaction ends.
     *
     * @param taskId task id
     * @return the task, or empty when it does not exist
     */
    public Optional<TaskRecord> findByIdForUpdate(UUID taskId) {
        return jdbc.sql(SELECT_BY_ID_FOR_UPDATE)
                .param(1, taskId)
                .query(ROW_MAPPER)
                .optional();
    }

    /**
     * Records the first cancel request for a non-terminal task. The guarded write
     * locks the row until the caller commits or rolls back.
     *
     * @return {@code true} if the caller must enqueue a cancel message
     */
    public boolean requestCancellation(UUID taskId) {
        return jdbc.sql("""
                UPDATE task
                SET cancel_requested_at = statement_timestamp(), updated_at = statement_timestamp()
                WHERE task_id = ? AND cancel_requested_at IS NULL AND state IN ('NEW', 'ACCEPTED')
                """)
                .param(taskId)
                .update() > 0;
    }

    /**
     * Applies the state produced by an event.
     *
     * @param taskId task id
     * @param update new field values
     * @return {@code true} when a row was updated
     */
    public boolean updateAfterEvent(UUID taskId, TaskUpdate update) {
        return jdbc.sql("""
                UPDATE task
                SET state = ?,
                    last_event_version = ?,
                    progress_stage = ?,
                    assigned_bot = ?,
                    result = CAST(? AS jsonb),
                    error = CAST(? AS jsonb),
                    updated_at = now()
                WHERE task_id = ?
                """)
                .param(1, update.state().name())
                .param(2, update.lastEventVersion())
                .param(3, opaqueText(update.progressStage()))
                .param(4, opaqueText(update.assignedBot()))
                .param(5, opaqueText(update.result()))
                .param(6, opaqueText(update.error()))
                .param(7, taskId)
                .update() > 0;
    }

    /**
     * Lists tasks that are not in a terminal state.
     *
     * @return active tasks ordered by creation time
     */
    public List<TaskRecord> listActive() {
        return jdbc.sql(SELECT_ACTIVE)
                .query(ROW_MAPPER)
                .list();
    }

    /**
     * Lists terminal tasks whose continuation has not finished yet.
     *
     * @return unprocessed terminal tasks ordered by update time
     */
    public List<TaskRecord> listTerminalUnprocessed() {
        return jdbc.sql(SELECT_TERMINAL_UNPROCESSED)
                .query(ROW_MAPPER)
                .list();
    }

    /**
     * Marks a task's continuation as finished.
     *
     * @param taskId task id
     * @return {@code true} when the task was marked by this call
     */
    public boolean markProcessed(UUID taskId) {
        return jdbc.sql("""
                UPDATE task
                SET processed_at = now(),
                    updated_at = now()
                WHERE task_id = ? AND processed_at IS NULL
                    AND state IN ('COMPLETED', 'FAILED', 'CANCELLED')
                """)
                .param(1, taskId)
                .update() > 0;
    }

    private static SqlTypeValue opaqueText(String value) {
        // Bind without exposing task/event data to Spring's parameter TRACE logging.
        return (statement, index, sqlType, typeName) -> statement.setString(index, value);
    }

    private static OffsetDateTime timestamp(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet row, String column) throws SQLException {
        OffsetDateTime value = row.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
