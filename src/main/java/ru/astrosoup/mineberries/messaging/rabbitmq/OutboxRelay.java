package ru.astrosoup.mineberries.messaging.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ShutdownSignalException;
import lombok.extern.slf4j.Slf4j;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Publishes due outbox rows, recording the outcome of each publish independently. */
@Slf4j
public class OutboxRelay {
    private final JdbcClient jdbc;
    private final TransactionTemplate transaction;
    private final ConnectionFactory rabbit;
    private final String tasksQueue;
    private final int batchSize;
    private final long confirmTimeoutMillis;
    private final OutboxRetryBackoff retryBackoff;

    OutboxRelay(JdbcClient jdbc, PlatformTransactionManager transactionManager, ConnectionFactory rabbit,
                String tasksQueue, int batchSize, Duration confirmTimeout, OutboxRetryBackoff retryBackoff) {
        if (batchSize < 1 || confirmTimeout.toMillis() < 1) {
            throw new IllegalArgumentException("Outbox relay requires a positive batch size and confirm timeout");
        }
        this.jdbc = jdbc;
        this.transaction = new TransactionTemplate(transactionManager);
        this.transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        ConnectionFactory publisher = rabbit.getPublisherConnectionFactory();
        this.rabbit = publisher != null ? publisher : rabbit;
        this.tasksQueue = tasksQueue;
        this.batchSize = batchSize;
        this.confirmTimeoutMillis = confirmTimeout.toMillis();
        this.retryBackoff = retryBackoff;
    }

    @Scheduled(fixedDelayString = "${mineberries.outbox.relay.interval:1s}")
    public void relayBatch() {
        transaction.executeWithoutResult(status -> {
            var rows = jdbc.sql("""
                    SELECT id, message_id, kind, type, payload, attempts
                    FROM outbox
                    WHERE sent_at IS NULL AND next_attempt_at <= statement_timestamp()
                    ORDER BY id
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                    """)
                    .param("limit", batchSize)
                    .query((row, index) -> new OutboxRow(row.getLong("id"), row.getObject("message_id", UUID.class),
                            row.getString("kind"), row.getString("type"), row.getString("payload"),
                            row.getInt("attempts")))
                    .list();

            for (OutboxRow row : rows) {
                PublishOutcome outcome = publishAndConfirm(row);
                if (outcome == PublishOutcome.CONFIRMED) {
                    jdbc.sql("UPDATE outbox SET sent_at = statement_timestamp(), last_error = NULL WHERE id = ?")
                            .param(row.id())
                            .update();
                } else {
                    recordFailure(row, outcome);
                }
            }
        });
    }

    private void recordFailure(OutboxRow row, PublishOutcome outcome) {
        int attempt = row.attempts() == Integer.MAX_VALUE ? Integer.MAX_VALUE : row.attempts() + 1;
        jdbc.sql("""
                UPDATE outbox
                SET attempts = :attempt, last_error = :error,
                    next_attempt_at = statement_timestamp() + :delay * INTERVAL '1 millisecond'
                WHERE id = :id
                """)
                .param("attempt", attempt)
                .param("error", outcome.error)
                .param("delay", retryBackoff.delayMillis(attempt))
                .param("id", row.id())
                .update();
        log.warn("Outbox publish failed: message_id={}, attempt={}, reason={}", row.messageId(), attempt, outcome.error);
    }

    private PublishOutcome publishAndConfirm(OutboxRow row) {
        // Isolate channel errors and overdue confirms to one row, without RabbitTemplate's body logging.
        try (var connection = rabbit.createConnection()) {
            var opened = connection.getDelegate().openChannel();
            if (opened.isEmpty()) return PublishOutcome.PUBLISH_ERROR;

            try (var channel = opened.get()) {
                channel.confirmSelect();
                var returned = new AtomicBoolean();
                channel.addReturnListener(message -> returned.set(true));
                var properties = new AMQP.BasicProperties.Builder()
                        .messageId(row.messageId().toString())
                        .headers(Map.of("kind", row.kind(), "type", row.type()))
                        .contentType("application/json")
                        .contentEncoding(StandardCharsets.UTF_8.name())
                        .deliveryMode(2)
                        .build();
                channel.basicPublish("", tasksQueue, true, properties, row.payload().getBytes(StandardCharsets.UTF_8));

                boolean confirmed = channel.waitForConfirms(confirmTimeoutMillis);
                // RabbitMQ sends basic.return before confirming an unroutable mandatory publish.
                if (returned.get()) return PublishOutcome.UNROUTABLE;
                return confirmed ? PublishOutcome.CONFIRMED : PublishOutcome.NACK;
            }
        } catch (TimeoutException e) {
            return PublishOutcome.TIMEOUT;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing the outbox row");
        } catch (IOException | AmqpException | ShutdownSignalException | IllegalArgumentException e) {
            // Exception details can include broker credentials or message contents.
            return PublishOutcome.PUBLISH_ERROR;
        }
    }

    private enum PublishOutcome {
        CONFIRMED(null),
        UNROUTABLE("RabbitMQ returned an unroutable message"),
        NACK("RabbitMQ did not acknowledge the message"),
        TIMEOUT("Timed out waiting for RabbitMQ publisher confirm"),
        PUBLISH_ERROR("RabbitMQ connection or publish failed");

        private final String error;

        PublishOutcome(String error) {
            this.error = error;
        }
    }

    private record OutboxRow(long id, UUID messageId, String kind, String type, String payload, int attempts) {}
}
