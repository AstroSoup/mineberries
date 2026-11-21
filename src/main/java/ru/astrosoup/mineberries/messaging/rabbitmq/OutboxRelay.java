package ru.astrosoup.mineberries.messaging.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rabbitmq.client.AMQP;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Publishes one locked outbox batch at a time, marking it sent only after broker confirmation. */
public class OutboxRelay {
    private final JdbcClient jdbc;
    private final TransactionTemplate transaction;
    private final ConnectionFactory rabbit;
    private final String tasksQueue;
    private final int batchSize;
    private final long confirmTimeoutMillis;

    OutboxRelay(JdbcClient jdbc, PlatformTransactionManager transactionManager, ConnectionFactory rabbit,
                String tasksQueue, int batchSize, Duration confirmTimeout) {
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
    }

    @Scheduled(fixedDelayString = "${mineberries.outbox.relay.interval:1s}")
    public void relayBatch() {
        transaction.executeWithoutResult(status -> {
            var rows = jdbc.sql("""
                    SELECT id, message_id, kind, type, payload
                    FROM outbox
                    WHERE sent_at IS NULL
                    ORDER BY id
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                    """)
                    .param("limit", batchSize)
                    .query((row, index) -> new OutboxRow(row.getLong("id"), row.getObject("message_id", UUID.class),
                            row.getString("kind"), row.getString("type"), row.getString("payload")))
                    .list();
            if (rows.isEmpty()) return;

            publishAndConfirm(rows);
            jdbc.sql("UPDATE outbox SET sent_at = statement_timestamp() WHERE id IN (:ids)")
                    .param("ids", rows.stream().map(OutboxRow::id).toList())
                    .update();
        });
    }

    private void publishAndConfirm(List<OutboxRow> rows) {
        // Isolate each batch on a dedicated channel, without RabbitTemplate's body logging.
        try (var connection = rabbit.createConnection();
             var channel = connection.getDelegate().openChannel()
                     .orElseThrow(() -> new IllegalStateException("No RabbitMQ channel available for the outbox relay"))) {
            channel.confirmSelect();
            var returned = new AtomicBoolean();
            channel.addReturnListener(message -> returned.set(true));

            for (OutboxRow row : rows) {
                var properties = new AMQP.BasicProperties.Builder()
                        .messageId(row.messageId().toString())
                        .headers(Map.of("kind", row.kind(), "type", row.type()))
                        .contentType("application/json")
                        .contentEncoding(StandardCharsets.UTF_8.name())
                        .deliveryMode(2)
                        .build();
                channel.basicPublish("", tasksQueue, true, properties, row.payload().getBytes(StandardCharsets.UTF_8));
            }

            if (!channel.waitForConfirms(confirmTimeoutMillis)) {
                throw new IllegalStateException("RabbitMQ did not acknowledge the outbox batch");
            }
            // RabbitMQ sends basic.return before confirming an unroutable mandatory publish on this channel.
            if (returned.get()) {
                throw new IllegalStateException("RabbitMQ returned an unroutable outbox message");
            }
        } catch (TimeoutException e) {
            throw new IllegalStateException("Timed out waiting for RabbitMQ to confirm the outbox batch");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing the outbox batch");
        } catch (IOException | AmqpException e) {
            throw new IllegalStateException("Could not publish the outbox batch to RabbitMQ");
        }
    }

    private record OutboxRow(long id, UUID messageId, String kind, String type, String payload) {}
}
