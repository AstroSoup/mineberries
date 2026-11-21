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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

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

            var first = relay(pausedRabbit, tasksQueue);
            var second = relay(rabbit, tasksQueue);
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
    void mandatoryReturnLeavesEveryRowUnsentEvenWhenRabbitConfirms() {
        contextRunner().run(context -> {
            assertThat(context).hasNotFailed();
            submitTasks(2, false);
            var rabbit = context.getBean(ConnectionFactory.class);

            var failure = assertThrows(IllegalStateException.class, relay(rabbit, tasksQueue + ".missing")::relayBatch);
            assertEquals("RabbitMQ returned an unroutable outbox message", failure.getMessage());
            assertTrue(sentTaskIds().isEmpty());

            relay(rabbit, tasksQueue).relayBatch();
            assertEquals(taskIds, sentTaskIds());
            assertMessages(expected, drain(rabbit));
        });
    }

    private ApplicationContextRunner contextRunner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RabbitAutoConfiguration.class))
                .withUserConfiguration(RabbitTopologyConfiguration.class)
                .withPropertyValues(
                        "spring.rabbitmq.host=" + RABBITMQ.getHost(),
                        "spring.rabbitmq.port=" + RABBITMQ.getAmqpPort(),
                        "spring.rabbitmq.username=" + RABBITMQ.getAdminUsername(),
                        "spring.rabbitmq.password=" + RABBITMQ.getAdminPassword(),
                        "mineberries.rabbitmq.tasks-queue=" + tasksQueue);
    }

    private OutboxRelay relay(ConnectionFactory rabbit, String queue) {
        return new OutboxRelay(jdbc, transactions, rabbit, queue, 2, CONFIRM_TIMEOUT);
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
