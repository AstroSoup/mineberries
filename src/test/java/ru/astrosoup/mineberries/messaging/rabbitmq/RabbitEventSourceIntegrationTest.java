package ru.astrosoup.mineberries.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import ru.astrosoup.mineberries.integration.AbstractIntegrationTest;
import ru.astrosoup.mineberries.messaging.common.broker.EventSource;
import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.tasks.NewTask;
import ru.astrosoup.mineberries.messaging.tasks.TaskRepository;
import tools.jackson.databind.ObjectMapper;

class RabbitEventSourceIntegrationTest extends AbstractIntegrationTest {
    private final UUID id = UUID.randomUUID();
    private final String queue = "events.source-" + id;
    private final String deadLetters = queue + ".dlq";
    private JdbcClient jdbc;
    private DataSourceTransactionManager transactions;
    private TaskRepository repository;

    @BeforeEach
    void setUp() {
        var source = new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        jdbc = JdbcClient.create(source);
        transactions = new DataSourceTransactionManager(source);
        repository = new TaskRepository(jdbc);
    }

    @AfterEach
    void deleteTestTask() {
        jdbc.sql("DELETE FROM task WHERE task_id = ?").param(id).update();
    }

    @Test
    void deliveryStaysUnacknowledgedUntilTheDatabaseCommit() {
        contextRunner().run(context -> {
            assertThat(context).hasNotFailed();
            var beforeCommit = new CountDownLatch(1);
            var allowCommit = new CountDownLatch(1);
            context.getBean(EventSource.class).subscribe(event -> {
                insertTask(event.messageId());
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void beforeCommit(boolean readOnly) {
                        beforeCommit.countDown();
                        try {
                            if (!allowCommit.await(20, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("Test did not release the commit");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("Test interrupted before commit");
                        }
                    }
                });
            });

            publish(context.getBean(ConnectionFactory.class), validBody());
            try {
                assertTrue(beforeCommit.await(5, TimeUnit.SECONDS), "Handler did not enter commit");
                assertEquals(0, committedTasks(), "The write must not yet be visible");
                await().atMost(Duration.ofSeconds(10)).until(() -> unacknowledged() == 1);
            } finally {
                allowCommit.countDown();
            }

            await().atMost(Duration.ofSeconds(10)).until(() -> committedTasks() == 1 && unacknowledged() == 0);
        });
    }

    @Test
    @ExtendWith(OutputCaptureExtension.class)
    void malformedAndSchemaInvalidEventsGoToTheDeadLetterQueueWithoutHandling(CapturedOutput output) {
        var logger = (Logger) LoggerFactory.getLogger("org.springframework.amqp.rabbit.listener.BlockingQueueConsumer");
        var previousLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        var marker = "test-sensitive-value";
        try {
            contextRunner().run(context -> {
                assertThat(context).hasNotFailed();
                var handled = new AtomicInteger();
                context.getBean(EventSource.class).subscribe(event -> handled.incrementAndGet());
                var rabbit = context.getBean(ConnectionFactory.class);
                var bodies = List.of("{not json: " + marker, validBody().replace("\"version\":0,", ""));
                for (String body : bodies) {
                    publish(rabbit, body, Map.of("untrusted-header", marker));
                    var message = takeDeadLetter(rabbit);
                    assertEquals(body, new String(message.getBody(), StandardCharsets.UTF_8));
                    assertEquals("rejected", deadLetterReason(message));
                }
                assertEquals(0, handled.get());
            });
            assertTrue(output.getAll().contains("Received message:"));
            assertFalse(output.getAll().contains(marker), "Delivery bodies and headers must not be logged");
        } finally {
            logger.setLevel(previousLevel);
        }
    }

    @Test
    void transientHandlerFailureRollsBackAndRedelivers() {
        contextRunner().run(context -> {
            assertThat(context).hasNotFailed();
            var attempts = new AtomicInteger();
            context.getBean(EventSource.class).subscribe(event -> {
                insertTask(event.messageId());
                if (attempts.incrementAndGet() == 1) {
                    throw new TransientDataAccessResourceException("Simulated transient handler failure");
                }
            });
            publish(context.getBean(ConnectionFactory.class), validBody());

            await().atMost(Duration.ofSeconds(10)).until(() -> committedTasks() == 1 && unacknowledged() == 0);
            assertEquals(2, attempts.get());
        });
    }

    @Test
    void repeatedFailuresCountTowardTheQuorumDeliveryLimit() {
        contextRunner().withPropertyValues("mineberries.rabbitmq.delivery-limit=2").run(context -> {
            assertThat(context).hasNotFailed();
            var attempts = new AtomicInteger();
            context.getBean(EventSource.class).subscribe(event -> {
                attempts.incrementAndGet();
                throw new TransientDataAccessResourceException("Simulated recurring handler failure");
            });
            var rabbit = context.getBean(ConnectionFactory.class);
            publish(rabbit, validBody());

            assertEquals("delivery_limit", deadLetterReason(takeDeadLetter(rabbit)));
            assertTrue(attempts.get() > 1);
        });
    }

    private ApplicationContextRunner contextRunner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RabbitAutoConfiguration.class))
                .withUserConfiguration(RabbitTopologyConfiguration.class, RabbitEventSourceConfiguration.class)
                .withBean(ObjectMapper.class, TaskCodec::defaultMapper)
                .withBean(PlatformTransactionManager.class, () -> transactions)
                .withPropertyValues(
                        "spring.rabbitmq.host=" + RABBITMQ.getHost(),
                        "spring.rabbitmq.port=" + RABBITMQ.getAmqpPort(),
                        "spring.rabbitmq.username=" + RABBITMQ.getAdminUsername(),
                        "spring.rabbitmq.password=" + RABBITMQ.getAdminPassword(),
                        "mineberries.rabbitmq.tasks-queue=tasks.source-" + id,
                        "mineberries.rabbitmq.events-queue=" + queue,
                        "mineberries.rabbitmq.events-dead-letter-queue=" + deadLetters);
    }

    private void insertTask(UUID id) {
        repository.insert(new NewTask(id, "event-test", "{}", null, null, null));
    }

    private long committedTasks() {
        return jdbc.sql("SELECT count(*) FROM task WHERE task_id = ?").param(id).query(Long.class).single();
    }

    private int unacknowledged() throws Exception {
        var result = RABBITMQ.execInContainer("rabbitmqctl", "-q", "list_queues", "name", "messages_unacknowledged");
        assertEquals(0, result.getExitCode());
        return result.getStdout().lines()
                .map(line -> line.strip().split("\\s+"))
                .filter(columns -> columns.length == 2 && queue.equals(columns[0]))
                .mapToInt(columns -> Integer.parseInt(columns[1]))
                .findFirst().orElseThrow();
    }

    private String validBody() {
        return """
                {"messageId":"%s","version":0,"type":"ACCEPTED","occurredAt":"2026-11-21T12:00:00Z",
                 "payload":{"bot":"banker_bot"}}
                """.formatted(id);
    }

    private void publish(ConnectionFactory rabbit, String body) throws Exception {
        publish(rabbit, body, Map.of());
    }

    private void publish(ConnectionFactory rabbit, String body, Map<String, Object> headers) throws Exception {
        try (var connection = rabbit.createConnection();
             var channel = connection.getDelegate().openChannel().orElseThrow()) {
            channel.confirmSelect();
            var properties = new AMQP.BasicProperties.Builder().contentType("application/json")
                    .deliveryMode(2).headers(headers).build();
            channel.basicPublish("", queue, true, properties,
                    body.getBytes(StandardCharsets.UTF_8));
            channel.waitForConfirmsOrDie(5000);
        }
    }

    private GetResponse takeDeadLetter(ConnectionFactory rabbit) {
        return await().atMost(Duration.ofSeconds(10)).until(() -> {
            try (var connection = rabbit.createConnection();
                 var channel = connection.getDelegate().openChannel().orElseThrow()) {
                return channel.basicGet(deadLetters, true);
            }
        }, Objects::nonNull);
    }

    private static String deadLetterReason(GetResponse message) {
        var history = assertInstanceOf(List.class, message.getProps().getHeaders().get("x-death"));
        var death = assertInstanceOf(Map.class, history.getFirst());
        return death.get("reason").toString();
    }
}
