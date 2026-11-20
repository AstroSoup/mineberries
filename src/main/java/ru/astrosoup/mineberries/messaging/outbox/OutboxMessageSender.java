package ru.astrosoup.mineberries.messaging.outbox;

import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import ru.astrosoup.mineberries.messaging.common.broker.MessageSender;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Writes the complete wire message to the outbox in the caller's Spring transaction.
 *
 * <p>The future completes when the insert succeeds, before transaction commit and
 * independently of any broker confirmation. A rollback removes the row.
 */
@Component
public class OutboxMessageSender implements MessageSender {
    private static final String INSERT = """
            INSERT INTO outbox (message_id, kind, type, payload)
            VALUES (?, ?, ?, CAST(? AS jsonb))
            """;
    private static final String TRANSACTION_REQUIRED = "Outbox send requires an active database transaction";

    private final DataSource dataSource;
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public OutboxMessageSender(DataSource dataSource, ObjectMapper mapper) {
        this.dataSource = dataSource;
        this.jdbc = new JdbcTemplate(dataSource);
        this.mapper = mapper;
    }

    /**
     * Writes a single outbox row, throwing synchronously if the write fails.
     *
     * @throws IllegalStateException if there is no active transaction for the outbox database
     *                               or the row cannot be written
     */
    @Override
    public CompletableFuture<Void> send(MessageDto dto) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager.hasResource(dataSource)) {
            throw new IllegalStateException(TRANSACTION_REQUIRED);
        }

        String wireMessage = encode(dto);
        String storedType = "cancel".equals(dto.kind()) ? "cancel" : dto.type();
        try {
            jdbc.execute((ConnectionCallback<Void>) connection -> {
                if (connection.getAutoCommit()) {
                    throw new IllegalStateException(TRANSACTION_REQUIRED);
                }
                // Bind directly: Spring's parameter binding can print values at TRACE level.
                try (PreparedStatement statement = connection.prepareStatement(INSERT)) {
                    statement.setObject(1, dto.messageId());
                    statement.setString(2, dto.kind());
                    statement.setString(3, storedType);
                    statement.setString(4, wireMessage);
                    statement.executeUpdate();
                }
                return null;
            });
        } catch (DataAccessException e) {
            // PostgreSQL errors can include the full row, including secret payload fields.
            throw new IllegalStateException("Could not write message to the outbox");
        }
        return CompletableFuture.completedFuture(null);
    }

    private String encode(MessageDto dto) {
        if (dto.messageId() == null || dto.schemaVersion() < 1) {
            throw new IllegalArgumentException("Outbox messages need a task ID and a positive schema version");
        }

        var envelope = new LinkedHashMap<String, Object>();
        envelope.put("messageId", dto.messageId().toString());
        envelope.put("kind", dto.kind());
        envelope.put("schemaVersion", dto.schemaVersion());

        if ("task".equals(dto.kind())) {
            if (dto.type() == null || dto.type().isBlank() || dto.payload() == null) {
                throw new IllegalArgumentException("Outbox task messages need a type and payload");
            }
            envelope.put("type", dto.type());
            if (dto.expiresAt() != null) {
                envelope.put("expiresAt", dto.expiresAt().toString());
            }
            envelope.put("payload", dto.payload());
        } else if (!"cancel".equals(dto.kind())) {
            throw new IllegalArgumentException("Outbox kind must be task or cancel");
        }

        try {
            return mapper.writeValueAsString(envelope);
        } catch (JacksonException e) {
            throw new IllegalArgumentException("Could not serialize the outbox message");
        }
    }
}
