package ru.astrosoup.mineberries.messaging.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import ru.astrosoup.mineberries.integration.AbstractIntegrationTest;
import ru.astrosoup.mineberries.messaging.common.broker.MessageSender;
import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;
import tools.jackson.databind.ObjectMapper;

class OutboxMessageSenderIntegrationTest extends AbstractIntegrationTest {
    private final UUID id = UUID.randomUUID();
    private final ObjectMapper mapper = TaskCodec.defaultMapper();
    private JdbcClient jdbc;
    private MessageSender sender;
    private TransactionTemplate transaction;

    @BeforeEach
    void setUp() {
        var dataSource = dataSource();
        jdbc = JdbcClient.create(dataSource);
        sender = new OutboxMessageSender(dataSource, mapper);
        transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @AfterEach
    void deleteMessageRows() {
        jdbc.sql("DELETE FROM outbox WHERE message_id = ?").param(id).update();
    }

    @Test
    void committedSendLeavesOneUnsentRowWithTheCompleteWireMessage() {
        var expiry = Instant.parse("2026-11-21T12:15:00Z");
        var dto = new MessageDto(id, "task", "banker.collect_payment", 1, expiry,
                Map.of("payerNickname", "buyer", "amount", new BigDecimal("12.50"), "timeoutSeconds", 900));

        transaction.executeWithoutResult(status -> {
            var future = sender.send(dto);
            assertTrue(future.isDone());
            assertNull(future.join());
            assertEquals(1, countRows());
        });

        assertEquals(1, countRows());
        var row = jdbc.sql("""
                SELECT kind, type, payload, sent_at, attempts FROM outbox WHERE message_id = ?
                """).param(id).query().singleRow();
        assertEquals("task", row.get("kind"));
        assertEquals(dto.type(), row.get("type"));
        assertNull(row.get("sent_at"));
        assertEquals(0, row.get("attempts"));
        assertPayload("""
                {
                  "messageId": "%s", "kind": "task", "type": "banker.collect_payment",
                  "schemaVersion": 1, "expiresAt": "2026-11-21T12:15:00Z",
                  "payload": {"payerNickname": "buyer", "amount": 12.50, "timeoutSeconds": 900}
                }
                """.formatted(id), row.get("payload").toString());
    }

    @Test
    void rolledBackSendLeavesNoRow() {
        transaction.executeWithoutResult(status -> {
            sender.send(taskMessage()).join();
            assertEquals(1, countRows());
            status.setRollbackOnly();
        });

        assertEquals(0, countRows());
    }

    @Test
    void sendWithoutATransactionThrowsImmediately() {
        assertThrows(IllegalStateException.class, () -> sender.send(taskMessage()));
        assertEquals(0, countRows());
    }

    @Test
    void aTransactionForAnotherDataSourceCannotEnqueue() {
        var otherTransaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource()));
        otherTransaction.executeWithoutResult(status ->
                assertThrows(IllegalStateException.class, () -> sender.send(taskMessage())));
        assertEquals(0, countRows());
    }

    @Test
    void cancelUsesTheTaskIdAndStoresOnlyCancelFieldsInTheWireMessage() {
        transaction.executeWithoutResult(status -> {
            sender.send(taskMessage()).join();
            sender.sendCancel(id, 1).join();
        });

        assertEquals(2, countRows());
        var cancel = jdbc.sql("""
                SELECT type, payload FROM outbox WHERE message_id = ? AND kind = 'cancel'
                """).param(id).query().singleRow();
        assertEquals("cancel", cancel.get("type"));
        assertPayload("""
                {"messageId": "%s", "kind": "cancel", "schemaVersion": 1}
                """.formatted(id), cancel.get("payload").toString());
    }

    private static DriverManagerDataSource dataSource() {
        return new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private MessageDto taskMessage() {
        return new MessageDto(id, "task", "banker.send_payment", 1, null,
                Map.of("recipientNickname", "seller", "amount", 10));
    }

    private long countRows() {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE message_id = ?").param(id).query(Long.class).single();
    }

    private void assertPayload(String expected, String actual) {
        assertEquals(mapper.readTree(expected), mapper.readTree(actual));
    }
}
