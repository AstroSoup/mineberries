package ru.astrosoup.mineberries.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.astrosoup.mineberries.messaging.common.TaskState;

/**
 * Verifies the task table changeset against a real PostgreSQL 18 container.
 *
 * <p>The shared container has the complete changelog applied by
 * {@link AbstractIntegrationTest}. The rollback test uses a scratch database so it
 * cannot disturb the schema shared by the other integration tests.
 */
class TaskSchemaIntegrationTest extends AbstractIntegrationTest {

    private static final String TASK_CHANGELOG = "db/changelog/changes/04-tasks.sql";

    private Connection connection;

    @BeforeEach
    void openTransaction() throws SQLException {
        connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        connection.setAutoCommit(false);
    }

    @AfterEach
    void rollbackAndClose() throws SQLException {
        if (connection != null) {
            connection.rollback();
            connection.close();
        }
    }

    @Test
    void createsTaskTableWithDefaultsAndNullableColumns() throws SQLException {
        UUID taskId = UUID.randomUUID();
        insertTask(taskId, "NEW", null);

        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT type, payload, state, last_event_version, progress_stage,
                       assigned_bot, result, error, expires_at, accept_by, finish_by,
                       cancel_requested_at, processed_at, created_at, updated_at
                FROM task
                WHERE task_id = ?
                """)) {
            statement.setObject(1, taskId);
            try (ResultSet row = statement.executeQuery()) {
                assertTrue(row.next());
                assertEquals("courier.deliver_order", row.getString("type"));
                assertEquals("{\"orderId\":42}", row.getString("payload").replace(" ", ""));
                assertEquals("NEW", row.getString("state"));
                assertEquals(-1L, row.getLong("last_event_version"));
                assertNull(row.getObject("progress_stage"));
                assertNull(row.getObject("assigned_bot"));
                assertNull(row.getObject("result"));
                assertNull(row.getObject("error"));
                assertNull(row.getObject("expires_at"));
                assertNull(row.getObject("accept_by"));
                assertNull(row.getObject("finish_by"));
                assertNull(row.getObject("cancel_requested_at"));
                assertNull(row.getObject("processed_at"));
                assertNotNull(row.getObject("created_at"));
                assertNotNull(row.getObject("updated_at"));
            }
        }
    }

    @Test
    void acceptsEveryTaskState() throws SQLException {
        for (TaskState state : TaskState.values()) {
            UUID taskId = UUID.randomUUID();
            String errorJson = state == TaskState.FAILED
                    ? "{\"message\":\"boom\",\"retryable\":false,\"outcomeUnknown\":false}"
                    : null;
            insertTask(taskId, state.name(), errorJson);

            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT state, error FROM task WHERE task_id = ?")) {
                statement.setObject(1, taskId);
                try (ResultSet row = statement.executeQuery()) {
                    assertTrue(row.next());
                    assertEquals(state.name(), row.getString("state"));
                    if (state == TaskState.FAILED) {
                        assertNotNull(row.getObject("error"));
                    } else {
                        assertNull(row.getObject("error"));
                    }
                }
            }
        }
    }

    @Test
    void rejectsUnknownState() {
        assertThrows(SQLException.class,
                () -> insertTask(UUID.randomUUID(), "UNKNOWN", null));
    }

    @Test
    void rejectsFailedTaskWithoutError() {
        assertThrows(SQLException.class,
                () -> insertTask(UUID.randomUUID(), "FAILED", null));
    }

    @Test
    void rejectsErrorOutsideFailedState() throws SQLException {
        for (TaskState state : TaskState.values()) {
            if (state == TaskState.FAILED) {
                continue;
            }
            Savepoint savepoint = connection.setSavepoint();
            assertThrows(SQLException.class,
                    () -> insertTask(UUID.randomUUID(), state.name(), "{\"message\":\"boom\"}"));
            connection.rollback(savepoint);
        }
    }

    @Test
    void createsPartialIndexesForActiveAndUnprocessedTerminalTasks() throws SQLException {
        Map<String, String> definitions = new HashMap<>();
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND tablename = 'task'
                  AND indexname IN ('idx_task_non_terminal', 'idx_task_terminal_unprocessed')
                """)) {
            try (ResultSet row = statement.executeQuery()) {
                while (row.next()) {
                    definitions.put(row.getString("indexname"), row.getString("indexdef"));
                }
            }
        }

        assertEquals(2, definitions.size());
        assertTrue(definitions.get("idx_task_non_terminal").contains("WHERE"));
        assertTrue(definitions.get("idx_task_terminal_unprocessed").contains("WHERE"));
        assertTrue(definitions.get("idx_task_terminal_unprocessed").contains("processed_at IS NULL"));
    }

    @Test
    void changesetAppliesAndRollsBackOnPostgres18() throws Exception {
        String databaseName = "task_rollback_" + UUID.randomUUID().toString().replace("-", "");
        String adminUrl = POSTGRES.getJdbcUrl().replace(
                "/" + POSTGRES.getDatabaseName(), "/postgres");
        String scratchUrl = POSTGRES.getJdbcUrl().replace(
                "/" + POSTGRES.getDatabaseName(), "/" + databaseName);

        createDatabase(adminUrl, databaseName);
        try {
            try (Connection scratch = DriverManager.getConnection(
                    scratchUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
                 Liquibase liquibase = new Liquibase(
                         TASK_CHANGELOG,
                         new ClassLoaderResourceAccessor(),
                         new JdbcConnection(scratch))) {
                liquibase.update(new Contexts("test"));
                assertTrue(tableExists(scratch));

                liquibase.rollback(1, "");
                assertFalse(tableExists(scratch));
            }
        } finally {
            dropDatabase(adminUrl, databaseName);
        }
    }

    private void insertTask(UUID taskId, String state, String errorJson) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO task (task_id, type, payload, state, error)
                VALUES (?, 'courier.deliver_order', '{"orderId":42}'::jsonb, ?, ?::jsonb)
                """)) {
            statement.setObject(1, taskId);
            statement.setString(2, state);
            if (errorJson == null) {
                statement.setNull(3, Types.OTHER);
            } else {
                statement.setString(3, errorJson);
            }
            statement.executeUpdate();
        }
    }

    private static boolean tableExists(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT to_regclass('public.task')");
             ResultSet row = statement.executeQuery()) {
            assertTrue(row.next());
            return row.getString(1) != null;
        }
    }

    private static void createDatabase(String adminUrl, String databaseName) throws SQLException {
        try (Connection admin = DriverManager.getConnection(
                adminUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = admin.createStatement()) {
            statement.execute("CREATE DATABASE " + databaseName);
        }
    }

    private static void dropDatabase(String adminUrl, String databaseName) throws SQLException {
        try (Connection admin = DriverManager.getConnection(
                adminUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement statement = admin.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS " + databaseName + " WITH (FORCE)");
        }
    }
}
