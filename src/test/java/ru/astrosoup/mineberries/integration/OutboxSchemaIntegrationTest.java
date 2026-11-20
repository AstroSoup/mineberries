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
import java.sql.Statement;
import java.util.UUID;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the outbox table changeset against a real PostgreSQL 18 container.
 *
 * <p>The shared container has the complete changelog applied by
 * {@link AbstractIntegrationTest}. The rollback test uses a scratch database so it
 * cannot disturb the schema shared by the other integration tests.
 */
class OutboxSchemaIntegrationTest extends AbstractIntegrationTest {

    private static final String OUTBOX_CHANGELOG = "db/changelog/changes/05-outbox.sql";

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
    void createsOutboxTableWithDefaults() throws SQLException {
        UUID messageId = UUID.randomUUID();
        long id = insertOutbox(messageId, "task", "courier.deliver_order", "{\"orderId\":42}");

        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT id, message_id, kind, type, payload, created_at, sent_at,
                       attempts, next_attempt_at, last_error
                FROM outbox
                WHERE id = ?
                """)) {
            statement.setLong(1, id);
            try (ResultSet row = statement.executeQuery()) {
                assertTrue(row.next());
                assertTrue(id > 0);
                assertEquals(messageId, row.getObject("message_id", UUID.class));
                assertEquals("task", row.getString("kind"));
                assertEquals("courier.deliver_order", row.getString("type"));
                assertEquals("{\"orderId\":42}", row.getString("payload").replace(" ", ""));
                assertNotNull(row.getObject("created_at"));
                assertNull(row.getObject("sent_at"));
                assertEquals(0, row.getInt("attempts"));
                assertNotNull(row.getObject("next_attempt_at"));
                assertNull(row.getObject("last_error"));
            }
        }
    }

    @Test
    void allowsOneTaskAndOneCancelForSameMessage() throws SQLException {
        UUID messageId = UUID.randomUUID();
        insertOutbox(messageId, "task", "courier.deliver_order", "{\"orderId\":42}");
        insertOutbox(messageId, "cancel", "courier.deliver_order", "{}");

        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT count(*) FROM outbox WHERE message_id = ?")) {
            statement.setObject(1, messageId);
            try (ResultSet row = statement.executeQuery()) {
                assertTrue(row.next());
                assertEquals(2, row.getInt(1));
            }
        }
    }

    @Test
    void rejectsDuplicateMessageIdAndKind() throws SQLException {
        UUID messageId = UUID.randomUUID();
        insertOutbox(messageId, "task", "courier.deliver_order", "{\"orderId\":42}");

        assertThrows(SQLException.class,
                () -> insertOutbox(messageId, "task", "courier.deliver_order", "{\"orderId\":43}"));
    }

    @Test
    void rejectsUnknownKind() {
        assertThrows(SQLException.class,
                () -> insertOutbox(UUID.randomUUID(), "event", "courier.deliver_order", "{}"));
    }

    @Test
    void createsPartialIndexForUnsentRowsOrderedById() throws SQLException {
        String definition = null;
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT indexdef
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND tablename = 'outbox'
                  AND indexname = 'idx_outbox_unsent'
                """)) {
            try (ResultSet row = statement.executeQuery()) {
                if (row.next()) {
                    definition = row.getString("indexdef");
                }
            }
        }

        assertNotNull(definition);
        assertTrue(definition.contains("btree (id)"));
        assertTrue(definition.contains("WHERE"));
        assertTrue(definition.contains("sent_at IS NULL"));
    }

    @Test
    void changesetAppliesAndRollsBackOnPostgres18() throws Exception {
        String databaseName = "outbox_rollback_" + UUID.randomUUID().toString().replace("-", "");
        String adminUrl = POSTGRES.getJdbcUrl().replace(
                "/" + POSTGRES.getDatabaseName(), "/postgres");
        String scratchUrl = POSTGRES.getJdbcUrl().replace(
                "/" + POSTGRES.getDatabaseName(), "/" + databaseName);

        createDatabase(adminUrl, databaseName);
        try {
            try (Connection scratch = DriverManager.getConnection(
                    scratchUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
                 Liquibase liquibase = new Liquibase(
                         OUTBOX_CHANGELOG,
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

    private long insertOutbox(UUID messageId, String kind, String type, String payload)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO outbox (message_id, kind, type, payload)
                VALUES (?, ?, ?, ?::jsonb)
                RETURNING id
                """)) {
            statement.setObject(1, messageId);
            statement.setString(2, kind);
            statement.setString(3, type);
            statement.setString(4, payload);
            try (ResultSet row = statement.executeQuery()) {
                assertTrue(row.next());
                return row.getLong(1);
            }
        }
    }

    private static boolean tableExists(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT to_regclass('public.outbox')");
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
