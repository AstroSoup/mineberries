package ru.astrosoup.mineberries.messaging.tasks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import ru.astrosoup.mineberries.integration.AbstractIntegrationTest;
import ru.astrosoup.mineberries.messaging.common.TaskState;

class TaskRepositoryIntegrationTest extends AbstractIntegrationTest {

    private static final Instant EXPIRES_AT = Instant.parse("2026-11-20T10:00:00Z");
    private static final Instant ACCEPT_BY = Instant.parse("2026-11-20T10:00:30Z");
    private static final Instant FINISH_BY = Instant.parse("2026-11-20T10:05:00Z");

    private JdbcClient jdbc;
    private TaskRepository repository;
    private TransactionTemplate transaction;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        dataSource.setDriverClassName(POSTGRES.getDriverClassName());

        jdbc = JdbcClient.create(dataSource);
        repository = new TaskRepository(jdbc);
        transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @Test
    void insertsAndFindsTaskForUpdate() {
        UUID taskId = UUID.randomUUID();

        transaction.executeWithoutResult(status -> {
            repository.insert(newTask(taskId));

            TaskRecord task = repository.findByIdForUpdate(taskId).orElseThrow();
            assertEquals(taskId, task.taskId());
            assertEquals("courier.deliver_order", task.type());
            assertEquals("{\"orderId\":42}", task.payload().replace(" ", ""));
            assertEquals(TaskState.NEW, task.state());
            assertEquals(-1L, task.lastEventVersion());
            assertNull(task.progressStage());
            assertNull(task.assignedBot());
            assertNull(task.result());
            assertNull(task.error());
            assertEquals(EXPIRES_AT, task.expiresAt());
            assertEquals(ACCEPT_BY, task.acceptBy());
            assertEquals(FINISH_BY, task.finishBy());
            assertNull(task.cancelRequestedAt());
            assertNull(task.processedAt());
            assertNotNull(task.createdAt());
            assertNotNull(task.updatedAt());

            assertTrue(repository.findByIdForUpdate(UUID.randomUUID()).isEmpty());
            status.setRollbackOnly();
        });
    }

    @Test
    void updatesTaskAfterAppliedEvents() {
        UUID taskId = UUID.randomUUID();
        UUID failedId = UUID.randomUUID();
        UUID missingId = UUID.randomUUID();

        transaction.executeWithoutResult(status -> {
            repository.insert(newTask(taskId));
            repository.insert(newTask(failedId));

            assertTrue(repository.updateAfterEvent(taskId,
                    new TaskUpdate(TaskState.ACCEPTED, 0, null, "courier_andy", null, null)));

            TaskRecord accepted = repository.findByIdForUpdate(taskId).orElseThrow();
            assertEquals(TaskState.ACCEPTED, accepted.state());
            assertEquals(0L, accepted.lastEventVersion());
            assertEquals("courier_andy", accepted.assignedBot());
            assertNull(accepted.progressStage());
            assertNull(accepted.result());
            assertNull(accepted.error());

            assertTrue(repository.updateAfterEvent(taskId,
                    new TaskUpdate(TaskState.ACCEPTED, 1, "NAVIGATING", "courier_andy", null, null)));

            TaskRecord progressing = repository.findByIdForUpdate(taskId).orElseThrow();
            assertEquals(TaskState.ACCEPTED, progressing.state());
            assertEquals(1L, progressing.lastEventVersion());
            assertEquals("NAVIGATING", progressing.progressStage());
            assertEquals("courier_andy", progressing.assignedBot());

            assertTrue(repository.updateAfterEvent(taskId,
                    new TaskUpdate(TaskState.COMPLETED, 2, "FINISHED", "courier_andy",
                            "{\"orderId\":42,\"outcome\":\"DELIVERED\"}", null)));

            TaskRecord completed = repository.findByIdForUpdate(taskId).orElseThrow();
            assertEquals(TaskState.COMPLETED, completed.state());
            assertEquals(2L, completed.lastEventVersion());
            assertEquals("FINISHED", completed.progressStage());
            assertEquals("courier_andy", completed.assignedBot());
            assertEquals("{\"orderId\":42,\"outcome\":\"DELIVERED\"}",
                    completed.result().replace(" ", ""));
            assertNull(completed.error());

            assertTrue(repository.updateAfterEvent(failedId,
                    new TaskUpdate(TaskState.FAILED, 0, null, "courier_andy", null,
                            "{\"message\":\"boom\",\"retryable\":false,\"outcomeUnknown\":false}")));

            TaskRecord failed = repository.findByIdForUpdate(failedId).orElseThrow();
            assertEquals(TaskState.FAILED, failed.state());
            assertNull(failed.result());
            assertEquals("{\"message\":\"boom\",\"retryable\":false,\"outcomeUnknown\":false}",
                    failed.error().replace(" ", ""));

            assertFalse(repository.updateAfterEvent(missingId,
                    new TaskUpdate(TaskState.ACCEPTED, 0, null, "courier_andy", null, null)));
            status.setRollbackOnly();
        });
    }

    @Test
    void listsActiveTasks() {
        UUID newId = UUID.randomUUID();
        UUID acceptedId = UUID.randomUUID();
        UUID completedId = UUID.randomUUID();

        transaction.executeWithoutResult(status -> {
            repository.insert(newTask(newId));
            repository.insert(newTask(acceptedId));
            repository.insert(newTask(completedId));

            assertTrue(repository.updateAfterEvent(acceptedId,
                    new TaskUpdate(TaskState.ACCEPTED, 0, null, "courier_andy", null, null)));
            assertTrue(repository.updateAfterEvent(completedId,
                    new TaskUpdate(TaskState.COMPLETED, 0, null, "courier_andy",
                            "{\"orderId\":42}", null)));

            Set<UUID> active = repository.listActive().stream()
                    .map(TaskRecord::taskId)
                    .collect(Collectors.toSet());

            assertTrue(active.contains(newId));
            assertTrue(active.contains(acceptedId));
            assertFalse(active.contains(completedId));
            status.setRollbackOnly();
        });
    }

    @Test
    void listsTerminalUnprocessedTasksAndMarksThemProcessed() {
        UUID completedId = UUID.randomUUID();
        UUID failedId = UUID.randomUUID();
        UUID activeId = UUID.randomUUID();
        UUID missingId = UUID.randomUUID();

        transaction.executeWithoutResult(status -> {
            repository.insert(newTask(completedId));
            repository.insert(newTask(failedId));
            repository.insert(newTask(activeId));

            assertTrue(repository.updateAfterEvent(completedId,
                    new TaskUpdate(TaskState.COMPLETED, 0, null, "courier_andy",
                            "{\"orderId\":42}", null)));
            assertTrue(repository.updateAfterEvent(failedId,
                    new TaskUpdate(TaskState.FAILED, 0, null, "courier_andy", null,
                            "{\"message\":\"boom\",\"retryable\":false,\"outcomeUnknown\":false}")));
            assertTrue(repository.updateAfterEvent(activeId,
                    new TaskUpdate(TaskState.ACCEPTED, 0, null, "courier_andy", null, null)));

            Set<UUID> unprocessed = repository.listTerminalUnprocessed().stream()
                    .map(TaskRecord::taskId)
                    .collect(Collectors.toSet());

            assertTrue(unprocessed.contains(completedId));
            assertTrue(unprocessed.contains(failedId));
            assertFalse(unprocessed.contains(activeId));

            assertTrue(repository.markProcessed(completedId));

            TaskRecord processed = repository.findByIdForUpdate(completedId).orElseThrow();
            assertNotNull(processed.processedAt());
            assertFalse(repository.markProcessed(completedId));
            assertFalse(repository.markProcessed(missingId));

            unprocessed = repository.listTerminalUnprocessed().stream()
                    .map(TaskRecord::taskId)
                    .collect(Collectors.toSet());

            assertFalse(unprocessed.contains(completedId));
            assertTrue(unprocessed.contains(failedId));
            status.setRollbackOnly();
        });
    }

    @Test
    void findByIdForUpdateBlocksSecondTransactionUntilFirstCommits() throws Exception {
        UUID taskId = UUID.randomUUID();
        transaction.executeWithoutResult(status -> repository.insert(newTask(taskId)));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch firstLocked = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch secondFinished = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);

        try {
            Future<TaskRecord> first = executor.submit(() -> transaction.execute(status -> {
                TaskRecord task = repository.findByIdForUpdate(taskId).orElseThrow();
                firstLocked.countDown();
                await(releaseFirst);
                return task;
            }));

            assertTrue(firstLocked.await(5, TimeUnit.SECONDS));

            Future<TaskRecord> second = executor.submit(() -> transaction.execute(status -> {
                secondStarted.countDown();
                TaskRecord task = repository.findByIdForUpdate(taskId).orElseThrow();
                secondFinished.countDown();
                return task;
            }));

            assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
            assertFalse(secondFinished.await(300, TimeUnit.MILLISECONDS),
                    "The second transaction must wait for the first row lock");

            releaseFirst.countDown();

            assertNotNull(first.get(5, TimeUnit.SECONDS));
            assertNotNull(second.get(5, TimeUnit.SECONDS));
            assertTrue(secondFinished.await(5, TimeUnit.SECONDS));
        } finally {
            releaseFirst.countDown();
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            jdbc.sql("DELETE FROM task WHERE task_id = ?")
                    .param(1, taskId)
                    .update();
        }
    }

    private static NewTask newTask(UUID taskId) {
        return new NewTask(
                taskId,
                "courier.deliver_order",
                "{\"orderId\":42}",
                EXPIRES_AT,
                ACCEPT_BY,
                FINISH_BY);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the first transaction");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the first transaction", exception);
        }
    }
}
