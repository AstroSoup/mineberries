package ru.astrosoup.mineberries.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.SCAN_PACKAGES;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import ru.astrosoup.mineberries.integration.AbstractIntegrationTest;
import ru.astrosoup.mineberries.messaging.banker.tasks.SendPayment;
import ru.astrosoup.mineberries.messaging.common.TaskManager;
import ru.astrosoup.mineberries.messaging.common.codec.ResultMetaRegistry;
import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.common.codec.TaskMetaRegistry;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;
import ru.astrosoup.mineberries.messaging.outbox.OutboxMessageSender;
import ru.astrosoup.mineberries.messaging.support.InMemoryBroker;
import ru.astrosoup.mineberries.messaging.tasks.TaskRepository;
import tools.jackson.databind.ObjectMapper;

class OutboxRelayIntegrationTest extends AbstractIntegrationTest {
    private static final Duration CONFIRM_TIMEOUT = Duration.ofSeconds(5);
    private static final OutboxRetryBackoff BACKOFF =
            new OutboxRetryBackoff(Duration.ofSeconds(30), Duration.ofMinutes(2));

    private final String tasksQueue = "tasks.relay-" + UUID.randomUUID();
    private final List<UUID> taskIds = new ArrayList<>();
    private final List<MessageDto> expected = new ArrayList<>();
    private final TaskCodec codec;
    private final ObjectMapper mapper = TaskCodec.defaultMapper();
    private JdbcClient jdbc;
    private DataSourceTransactionManager transactions;
    private OutboxMessageSender sender;
    private TaskManager manager;

    OutboxRelayIntegrationTest() {
        var results = new ResultMetaRegistry(SCAN_PACKAGES);
        codec = new TaskCodec(new TaskMetaRegistry(results, SCAN_PACKAGES), results, mapper);
    }

    @BeforeEach
    void setUp() {
        var dataSource = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        jdbc = JdbcClient.create(dataSource);
        transactions = new DataSourceTransactionManager(dataSource);
        sender = new OutboxMessageSender(dataSource, mapper);
        manager = new TaskManager(sender, new InMemoryBroker(), codec, new TaskRepository(jdbc), mapper, transactions);
    }

    @AfterEach
    void deleteRows() {
        if (taskIds.isEmpty()) return;
        jdbc.sql("DELETE FROM outbox WHERE message_id IN (:ids)").param("ids", taskIds).update();
        jdbc.sql("DELETE FROM task WHERE task_id IN (:ids)").param("ids", taskIds).update();
    }

    @Test
    void scheduledRelayPublishesInIdOrderAndMarksRowsSent() {
        contextRunner()
                .withUserConfiguration(OutboxRelayConfiguration.class)
                .withBean(JdbcClient.class, () -> jdbc)
                .withBean(PlatformTransactionManager.class, () -> transactions)
                .withPropertyValues("mineberries.outbox.relay.interval=25ms",
                        "mineberries.outbox.relay.batch-size=2", "mineberries.outbox.relay.confirm-timeout=5s")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    submitTasks(4, true);
                    await().atMost(Duration.ofSeconds(10)).until(() -> sentTaskIds().size() == expected.size());
                    assertEquals(expected.stream().map(MessageDto::messageId).toList(), sentTaskIds());

                    assertMessages(expected, drain(context.getBean(ConnectionFactory.class)));
                });
    }

    @Test
    void concurrentRelaysSkipEachOthersLocksAndDoNotPublishDuplicates() {
        contextRunner().run(context -> {
            assertThat(context).hasNotFailed();
            submitTasks(4, false);
            var rabbit = context.getBean(ConnectionFactory.class);
            var firstHasLockedRows = new CountDownLatch(1);
            var releaseFirst = new CountDownLatch(1);
            var pausedRabbit = mock(ConnectionFactory.class);
            when(pausedRabbit.createConnection()).thenAnswer(call -> {
                firstHasLockedRows.countDown();
                assertTrue(releaseFirst.await(10, TimeUnit.SECONDS), "First relay was not released");
                return rabbit.createConnection();
            });

            var first = relay(pausedRabbit);
            var second = relay(rabbit);
            try (var executor = Executors.newFixedThreadPool(2)) {
                var firstRun = executor.submit(first::relayBatch);
                try {
                    assertTrue(firstHasLockedRows.await(5, TimeUnit.SECONDS), "First relay did not lock its batch");
                    assertTrue(sentTaskIds().isEmpty());
                    executor.submit(second::relayBatch).get(5, TimeUnit.SECONDS);
                    assertEquals(taskIds.subList(2, 4), sentTaskIds());
                } finally {
                    releaseFirst.countDown();
                }
                firstRun.get(5, TimeUnit.SECONDS);
            }

            assertEquals(taskIds, sentTaskIds());
            var expectedOrder = List.of(expected.get(2), expected.get(3), expected.get(0), expected.get(1));
            assertMessages(expectedOrder, drain(rabbit));
        });
    }

    @Test
    void retriesAfterRabbitMqStopsAndRecovers() {
        contextRunner().run(context -> {
            assertThat(context).hasNotFailed();
            submitTasks(2, false);
            var rabbit = context.getBean(ConnectionFactory.class);
            var relay = relay(rabbit);

            try {
                assertEquals(0, RABBITMQ.execInContainer("rabbitmqctl", "stop_app").getExitCode());
                relay.relayBatch();
                assertRetries(1, "RabbitMQ connection or publish failed");

                var waiting = retryStates();
                relay.relayBatch();
                assertEquals(waiting, retryStates(), "A failed row must not be retried before it is due");

                makeRetriesDue();
                relay.relayBatch();
                assertRetries(2, "RabbitMQ connection or publish failed");
            } finally {
                assertEquals(0, RABBITMQ.execInContainer("rabbitmqctl", "start_app").getExitCode());
            }

            makeRetriesDue();
            relay.relayBatch();
            assertEquals(taskIds, sentTaskIds());
            assertEquals(List.of(new RetryState(2, null, false), new RetryState(2, null, false)), retryStates());
            assertMessages(expected, drain(rabbit));
        });
    }

    @Test
    void mandatoryReturnFromDeletedQueueIsRecordedAndRetried() {
        contextRunner().run(context -> {
            assertThat(context).hasNotFailed();
            submitTasks(2, false);
            var rabbit = context.getBean(ConnectionFactory.class);
            // Open the publisher connection before deleting; opening it can trigger auto-declaration.
            try (var connection = rabbit.getPublisherConnectionFactory().createConnection();
                 var channel = connection.getDelegate().openChannel().orElseThrow()) {
                channel.queueDelete(tasksQueue);
            }

            var relay = relay(rabbit);
            relay.relayBatch();
            assertRetries(1, "RabbitMQ returned an unroutable message");

            context.getBean(AmqpAdmin.class).initialize();
            makeRetriesDue();
            relay.relayBatch();
            assertEquals(taskIds, sentTaskIds());
            assertMessages(expected, drain(rabbit));
        });
    }

    @Test
    void brokerRejectedPoisonRowDoesNotBlockTheNextRow() {
        try (var broker = new RabbitMQContainer(DockerImageName.parse("rabbitmq:4-management"))
                .withAdminUser(RABBITMQ.getAdminUsername())
                .withAdminPassword(RABBITMQ.getAdminPassword())
                .withCopyToContainer(Transferable.of("max_message_size = 4096\n"),
                        "/etc/rabbitmq/conf.d/91-test-size.conf")) {
            broker.start();
            contextRunner(broker).run(context -> {
                assertThat(context).hasNotFailed();
                submitTasks(2, false);
                jdbc.sql("""
                        UPDATE outbox
                        SET payload = payload || jsonb_build_object('extra', repeat('x', 8192))
                        WHERE message_id = ?
                        """).param(taskIds.getFirst()).update();
                var rabbit = context.getBean(ConnectionFactory.class);

                relay(rabbit).relayBatch();

                assertEquals(List.of(taskIds.get(1)), sentTaskIds());
                var states = retryStates();
                assertEquals(new RetryState(1, "RabbitMQ connection or publish failed", true), states.getFirst());
                assertEquals(0, states.get(1).attempts());
                assertMessages(expected.subList(1, 2), drain(rabbit));
            });
        }
    }

    @Test
    void nackAndConfirmTimeoutAreRecordedSeparately() throws Exception {
        submitTasks(2, false);
        var nack = mock(Channel.class);
        var timeout = mock(Channel.class);
        when(nack.waitForConfirms(CONFIRM_TIMEOUT.toMillis())).thenReturn(false);
        when(timeout.waitForConfirms(CONFIRM_TIMEOUT.toMillis())).thenThrow(new TimeoutException("test details"));
        var delegate = mock(com.rabbitmq.client.Connection.class);
        when(delegate.openChannel()).thenReturn(Optional.of(nack)).thenReturn(Optional.of(timeout));
        var connection = mock(Connection.class);
        when(connection.getDelegate()).thenReturn(delegate);
        var rabbit = mock(ConnectionFactory.class);
        when(rabbit.createConnection()).thenReturn(connection);

        relay(rabbit).relayBatch();

        assertTrue(sentTaskIds().isEmpty());
        assertEquals(List.of(
                new RetryState(1, "RabbitMQ did not acknowledge the message", true),
                new RetryState(1, "Timed out waiting for RabbitMQ publisher confirm", true)), retryStates());
    }

    private ApplicationContextRunner contextRunner() {
        return contextRunner(RABBITMQ);
    }

    private ApplicationContextRunner contextRunner(RabbitMQContainer broker) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RabbitAutoConfiguration.class))
                .withUserConfiguration(RabbitTopologyConfiguration.class)
                .withPropertyValues(
                        "spring.rabbitmq.host=" + broker.getHost(),
                        "spring.rabbitmq.port=" + broker.getAmqpPort(),
                        "spring.rabbitmq.username=" + broker.getAdminUsername(),
                        "spring.rabbitmq.password=" + broker.getAdminPassword(),
                        "mineberries.rabbitmq.tasks-queue=" + tasksQueue);
    }

    private OutboxRelay relay(ConnectionFactory rabbit) {
        return new OutboxRelay(jdbc, transactions, rabbit, tasksQueue, 2, CONFIRM_TIMEOUT, BACKOFF);
    }

    private void submitTasks(int count, boolean includeCancel) {
        new TransactionTemplate(transactions).executeWithoutResult(status -> {
            for (int i = 1; i <= count; i++) {
                var task = new SendPayment("seller_" + i, BigDecimal.valueOf(i));
                taskIds.add(task.id());
                manager.submit(task);
                expected.add(codec.encode(task));
            }
            if (includeCancel) {
                var id = taskIds.getFirst();
                sender.sendCancel(id, 1).join();
                expected.add(new MessageDto(id, "cancel", null, 1, null, null));
            }
        });
    }

    private List<UUID> sentTaskIds() {
        return jdbc.sql("""
                SELECT message_id FROM outbox
                WHERE message_id IN (:ids) AND sent_at IS NOT NULL
                ORDER BY id
                """).param("ids", taskIds).query(UUID.class).list();
    }

    private void assertRetries(int attempts, String error) {
        assertTrue(sentTaskIds().isEmpty());
        assertEquals(taskIds.stream().map(id -> new RetryState(attempts, error, true)).toList(), retryStates());
    }

    private List<RetryState> retryStates() {
        return jdbc.sql("""
                SELECT attempts, last_error, next_attempt_at > statement_timestamp() AS waiting
                FROM outbox WHERE message_id IN (:ids) ORDER BY id
                """).param("ids", taskIds)
                .query((row, i) -> new RetryState(row.getInt("attempts"), row.getString("last_error"),
                        row.getBoolean("waiting")))
                .list();
    }

    private void makeRetriesDue() {
        jdbc.sql("""
                UPDATE outbox SET next_attempt_at = statement_timestamp()
                WHERE message_id IN (:ids) AND sent_at IS NULL
                """).param("ids", taskIds).update();
    }

    private record RetryState(int attempts, String error, boolean waiting) {}

    private List<GetResponse> drain(ConnectionFactory rabbit) throws Exception {
        try (var connection = rabbit.createConnection(); var channel = connection.getDelegate().openChannel().orElseThrow()) {
            var messages = new ArrayList<GetResponse>();
            GetResponse message;
            while ((message = channel.basicGet(tasksQueue, true)) != null) {
                messages.add(message);
            }
            return messages;
        }
    }

    private void assertMessages(List<MessageDto> expected, List<GetResponse> actual) {
        assertEquals(expected.size(), actual.size(), "Number of messages on the tasks queue");
        for (int i = 0; i < expected.size(); i++) {
            var dto = expected.get(i);
            var message = actual.get(i);
            var properties = message.getProps();
            var body = mapper.readTree(message.getBody());
            boolean cancel = "cancel".equals(dto.kind());

            assertEquals(dto.messageId().toString(), properties.getMessageId());
            assertEquals(dto.kind(), properties.getHeaders().get("kind").toString());
            assertEquals(cancel ? "cancel" : dto.type(), properties.getHeaders().get("type").toString());
            assertEquals(2, properties.getDeliveryMode());
            assertEquals("application/json", properties.getContentType());
            assertEquals(dto.messageId().toString(), body.path("messageId").asString());
            assertEquals(dto.kind(), body.path("kind").asString());
            assertEquals(dto.schemaVersion(), body.path("schemaVersion").asInt());
            if (cancel) {
                assertEquals(3, body.size());
            } else {
                assertEquals(dto.type(), body.path("type").asString());
                assertEquals(mapper.readTree(mapper.writeValueAsString(dto.payload())), body.path("payload"));
            }
        }
    }
}
