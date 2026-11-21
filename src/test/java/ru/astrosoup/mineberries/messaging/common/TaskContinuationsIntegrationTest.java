package ru.astrosoup.mineberries.messaging.common;

import static org.junit.jupiter.api.Assertions.*;
import static ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.SCAN_PACKAGES;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import ru.astrosoup.mineberries.integration.AbstractIntegrationTest;
import ru.astrosoup.mineberries.messaging.banker.tasks.SendPayment;
import ru.astrosoup.mineberries.messaging.common.codec.ResultMetaRegistry;
import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.common.codec.TaskMetaRegistry;
import ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.PingTask;
import ru.astrosoup.mineberries.messaging.outbox.OutboxMessageSender;
import ru.astrosoup.mineberries.messaging.support.Events;
import ru.astrosoup.mineberries.messaging.support.InMemoryBroker;
import ru.astrosoup.mineberries.messaging.tasks.NewTask;
import ru.astrosoup.mineberries.messaging.tasks.TaskRepository;
import tools.jackson.databind.ObjectMapper;

class TaskContinuationsIntegrationTest extends AbstractIntegrationTest {
    private final HashSet<UUID> ids = new HashSet<>();
    private final ObjectMapper mapper = TaskCodec.defaultMapper();
    private JdbcClient jdbc;
    private TaskRepository repository;
    private DataSourceTransactionManager transactions;
    private TransactionTemplate transaction;
    private TaskCodec codec;
    private OutboxMessageSender outbox;

    @BeforeEach
    void setUp() {
        var dataSource = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        jdbc = JdbcClient.create(dataSource);
        repository = new TaskRepository(jdbc);
        transactions = new DataSourceTransactionManager(dataSource);
        transaction = new TransactionTemplate(transactions);
        var results = new ResultMetaRegistry(SCAN_PACKAGES);
        codec = new TaskCodec(new TaskMetaRegistry(results, SCAN_PACKAGES), results, mapper);
        outbox = new OutboxMessageSender(dataSource, mapper);
    }

    @AfterEach
    void cleanUp() {
        if (ids.isEmpty()) return;
        jdbc.sql("DELETE FROM outbox WHERE message_id IN (:ids)").param("ids", ids).update();
        jdbc.sql("DELETE FROM task WHERE task_id IN (:ids)").param("ids", ids).update();
    }

    @Test
    void retriesOnRestartWhenTheProcessStopsBeforeTheContinuationFinishes() throws Exception {
        var events = new InMemoryBroker();
        var manager = manager(events, Duration.ofMinutes(5));
        var entered = new CountDownLatch(1);
        var stop = new CountDownLatch(1);
        manager.reattach(SendPayment.class, task -> {
            if (!task.state().isTerminal()) return;
            entered.countDown();
            try {
                assertTrue(stop.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("Simulated termination during terminal work");
        });
        var task = submit(manager, payment());
        try (var executor = Executors.newSingleThreadExecutor()) {
            var handling = executor.submit(() -> events.publish(Events.completed(task.id(), 0, result())));
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertEquals(new Processing(TaskState.COMPLETED, null), processing(task.id()));
                assertTrue(task.result().isDone());
            } finally {
                stop.countDown();
            }
            handling.get(5, TimeUnit.SECONDS);
        }
        assertNull(processing(task.id()).processedAt());

        var restarted = manager(new InMemoryBroker(), Duration.ofMinutes(5));
        var replays = new ArrayList<UUID>();
        restarted.reattach(SendPayment.class, recovered -> {
            replays.add(recovered.id());
            assertEquals("seller", recovered.result().join().recipientNickname());
        });
        assertEquals(List.of(task.id()), replays);
        assertNotNull(processing(task.id()).processedAt());
        assertTrue(restarted.terminalUnprocessedTasks().isEmpty());

        manager(new InMemoryBroker(), Duration.ofMinutes(5)).reattach(SendPayment.class,
                recovered -> fail("Processed task must not be replayed"));
    }

    @Test
    @ExtendWith(OutputCaptureExtension.class)
    void failedContinuationLeavesProcessedAtNullAndRollsBackItsDatabaseWrites(CapturedOutput output) {
        var events = new InMemoryBroker();
        var manager = manager(events, Duration.ZERO);
        var task = submit(manager, payment());
        events.publish(Events.completed(task.id(), 0, result()));
        var writeId = newId();
        var marker = "test-sensitive-value";

        manager.reattach(SendPayment.class, recovered -> {
            repository.insert(new NewTask(writeId, "continuation-test", "{}", null, null, null));
            throw new IllegalArgumentException(marker);
        });
        manager.processUnownedTerminalTasks();

        assertEquals(new Processing(TaskState.COMPLETED, null), processing(task.id()));
        assertTrue(jdbc.sql("SELECT task_id FROM task WHERE task_id = ?").param(writeId).query(UUID.class).optional().isEmpty());
        assertTrue(output.getAll().contains("Continuation failed for task " + task.id()));
        assertFalse(output.getAll().contains(marker));
    }

    @Test
    void reattachingToARunningTaskReceivesLaterProgressAndRunsTerminalWork() {
        var original = new InMemoryBroker();
        var task = submit(manager(original, Duration.ofMinutes(5)), payment());
        original.publish(Events.accepted(task.id(), 0, "banker_bot"));
        var events = new InMemoryBroker();
        var restarted = manager(events, Duration.ofMinutes(5));
        var stages = new ArrayList<String>();
        var calls = new ArrayList<TaskState>();
        var observedResult = new AtomicReference<CompletableFuture<String>>();

        restarted.reattach(SendPayment.class, recovered -> {
            calls.add(recovered.state());
            if (!recovered.state().isTerminal()) {
                recovered.onProgress(progress -> stages.add(progress.stage()));
                observedResult.set(recovered.result().thenApply(result -> result.recipientNickname()));
            }
        });
        assertEquals(List.of(TaskState.ACCEPTED), calls);
        events.publish(Events.progress(task.id(), 1, "waiting"));
        assertEquals(List.of("waiting"), stages);
        assertFalse(observedResult.get().isDone());
        assertNull(processing(task.id()).processedAt());
        events.publish(Events.completed(task.id(), 2, result()));
        assertEquals("seller", observedResult.get().join());
        assertEquals(List.of(TaskState.ACCEPTED, TaskState.COMPLETED), calls);
        assertNotNull(processing(task.id()).processedAt());
    }

    @Test
    void registeredOwnerAttachesOnSubmitCommitAndHandlesTerminalStateOnce() {
        var events = new InMemoryBroker();
        var manager = manager(events, Duration.ZERO);
        var calls = new ArrayList<TaskState>();
        manager.reattach(SendPayment.class, task -> calls.add(task.state()));
        assertThrows(IllegalStateException.class, () -> manager.reattach(SendPayment.class, task -> fail()));
        var task = payment();
        transaction.executeWithoutResult(status -> {
            manager.submit(task);
            assertTrue(calls.isEmpty());
        });
        assertEquals(List.of(TaskState.NEW), calls);

        var completed = Events.completed(task.id(), 0, result());
        events.publish(completed);
        events.publish(completed);
        assertEquals(List.of(TaskState.NEW, TaskState.COMPLETED), calls);
        assertNotNull(processing(task.id()).processedAt());
    }

    @Test
    @ExtendWith(OutputCaptureExtension.class)
    void marksOnlyUnownedTerminalTasksAfterTheGracePeriod(CapturedOutput output) {
        var events = new InMemoryBroker();
        var manager = manager(events, Duration.ofMinutes(5));
        var orphan = submit(manager, new PingTask(newId()));
        var owned = submit(manager, payment());
        var active = submit(manager, new PingTask(newId()));
        events.publish(Events.canceled(orphan.id(), 0));
        events.publish(Events.completed(owned.id(), 0, result()));

        manager.processUnownedTerminalTasks();
        assertNull(processing(orphan.id()).processedAt());

        var afterGrace = manager(new InMemoryBroker(), Duration.ZERO);
        var ownerCalls = new AtomicInteger();
        afterGrace.reattach(SendPayment.class, task -> {
            ownerCalls.incrementAndGet();
            throw new IllegalStateException("Owner failure");
        });
        afterGrace.processUnownedTerminalTasks();

        assertEquals(1, ownerCalls.get());
        assertNotNull(processing(orphan.id()).processedAt());
        assertNull(processing(owned.id()).processedAt());
        assertNull(processing(active.id()).processedAt());
        assertTrue(output.getAll().contains("Marked task " + orphan.id() + " processed without a registered continuation"));
    }

    private TaskManager manager(InMemoryBroker events, Duration grace) {
        return new TaskManager(outbox, events, codec, repository, mapper, transactions, grace);
    }

    private <T extends AbstractTask<?>> T submit(TaskManager manager, T task) {
        return transaction.execute(status -> manager.submit(task));
    }

    private UUID newId() {
        var id = UUID.randomUUID();
        ids.add(id);
        return id;
    }

    private SendPayment payment() {
        return new SendPayment(newId(), "seller", new BigDecimal("12.00"));
    }

    private Processing processing(UUID id) {
        return jdbc.sql("SELECT state, processed_at FROM task WHERE task_id = ?").param(id)
                .query((row, index) -> new Processing(TaskState.valueOf(row.getString("state")),
                        row.getObject("processed_at", OffsetDateTime.class))).single();
    }

    private static Map<String, Object> result() {
        return Map.of("recipientNickname", "seller", "amount", new BigDecimal("12.00"),
                "paidAt", "2026-11-21T12:00:00Z");
    }

    private record Processing(TaskState state, OffsetDateTime processedAt) {}
}
