package ru.astrosoup.mineberries.messaging.common;

import static org.junit.jupiter.api.Assertions.*;
import static ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.SCAN_PACKAGES;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.TransactionTemplate;

import ru.astrosoup.mineberries.integration.AbstractIntegrationTest;
import ru.astrosoup.mineberries.messaging.banker.tasks.SendPayment;
import ru.astrosoup.mineberries.messaging.common.codec.ResultMetaRegistry;
import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.common.codec.TaskMetaRegistry;
import ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.ExpiringTask;
import ru.astrosoup.mineberries.messaging.outbox.OutboxMessageSender;
import ru.astrosoup.mineberries.messaging.support.Events;
import ru.astrosoup.mineberries.messaging.support.InMemoryBroker;
import ru.astrosoup.mineberries.messaging.tasks.TaskRepository;

class TaskManagerIntegrationTest extends AbstractIntegrationTest {
    private final UUID id = UUID.randomUUID();
    private final UUID otherId = UUID.randomUUID();
    private final InMemoryBroker events = new InMemoryBroker();
    private JdbcClient jdbc;
    private TaskRepository repository;
    private DataSourceTransactionManager transactionManager;
    private TransactionTemplate transaction;
    private TaskManager manager;

    @BeforeEach
    void setUp() {
        var dataSource = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        jdbc = JdbcClient.create(dataSource);
        repository = new TaskRepository(jdbc);
        transactionManager = new DataSourceTransactionManager(dataSource);
        transaction = new TransactionTemplate(transactionManager);
        var mapper = TaskCodec.defaultMapper();
        var results = new ResultMetaRegistry(SCAN_PACKAGES);
        var codec = new TaskCodec(new TaskMetaRegistry(results, SCAN_PACKAGES), results, mapper);
        manager = new TaskManager(new OutboxMessageSender(dataSource, mapper), events, codec,
                repository, mapper, transactionManager);
    }

    @AfterEach
    void deleteRows() {
        var ids = List.of(id, otherId);
        jdbc.sql("DELETE FROM outbox WHERE message_id IN (:ids)").param("ids", ids).update();
        jdbc.sql("DELETE FROM task WHERE task_id IN (:ids)").param("ids", ids).update();
    }

    @Test
    void commitPersistsBothRowsAndActivatesTheTaskOnlyAfterCommit() {
        var task = payment(id);
        var future = task.result();
        transaction.executeWithoutResult(status -> {
            assertSame(task, manager.submit(task));
            assertRows(id, 1);
            events.publish(Events.accepted(id, 0, "banker_bot"));
            assertEquals(TaskState.NEW, task.state());
        });

        assertRows(id, 1);
        var stored = transaction.execute(status -> repository.findByIdForUpdate(id).orElseThrow());
        assertEquals(TaskState.NEW, stored.state());
        assertEquals("banker.send_payment", stored.type());
        assertEquals("seller", TaskCodec.defaultMapper().readTree(stored.payload()).get("recipientNickname").asString());
        assertNull(stored.expiresAt());
        assertNull(stored.acceptBy());
        assertNull(stored.finishBy());
        events.publish(Events.accepted(id, 0, "banker_bot"));
        assertEquals(TaskState.ACCEPTED, task.state());
        assertFalse(future.isDone());
    }

    @Test
    void submitPersistsDeadlinesSuppliedByTheTask() {
        var acceptBy = Instant.parse("2026-11-21T12:00:10Z");
        var finishBy = Instant.parse("2026-11-21T12:02:00Z");
        var expiresAt = Instant.parse("2026-11-21T12:04:00Z");
        var task = new ExpiringTask(id, expiresAt, acceptBy, finishBy);

        transaction.executeWithoutResult(status -> manager.submit(task));

        var stored = transaction.execute(status -> repository.findByIdForUpdate(id).orElseThrow());
        assertEquals(expiresAt, stored.expiresAt());
        assertEquals(acceptBy, stored.acceptBy());
        assertEquals(finishBy, stored.finishBy());
    }

    @Test
    void rollbackLeavesNeitherRowAndTheTaskCanBeSubmittedAgain() {
        var task = payment(id);
        transaction.executeWithoutResult(status -> {
            manager.submit(task);
            status.setRollbackOnly();
        });

        assertRows(id, 0);
        events.publish(Events.accepted(id, 0, "banker_bot"));
        assertEquals(TaskState.NEW, task.state());
        assertFalse(task.result().isDone());
        assertSame(task, transaction.execute(status -> manager.submit(task)));
        events.publish(Events.accepted(id, 0, "banker_bot"));
        assertEquals(TaskState.ACCEPTED, task.state());
    }

    @Test
    void savepointRollbackDoesNotActivateAnUndoneSubmission() {
        var nested = new TransactionTemplate(transactionManager);
        nested.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
        var kept = payment(id);
        var undone = payment(otherId);

        transaction.executeWithoutResult(status -> {
            manager.submit(kept);
            nested.executeWithoutResult(inner -> {
                manager.submit(undone);
                inner.setRollbackOnly();
            });
        });

        assertRows(id, 1);
        assertRows(otherId, 0);
        events.publish(Events.accepted(id, 0, "banker_bot"));
        events.publish(Events.accepted(otherId, 0, "banker_bot"));
        assertEquals(TaskState.ACCEPTED, kept.state());
        assertEquals(TaskState.NEW, undone.state());
    }

    @Test
    void rejectsDuplicateIdBeforeAndAfterActivation() {
        var task = payment(id);
        assertThrows(IllegalStateException.class, () -> transaction.executeWithoutResult(status -> {
            manager.submit(task);
            manager.submit(payment(id));
        }));
        assertRows(id, 0);

        transaction.executeWithoutResult(status -> manager.submit(task));
        assertThrows(IllegalStateException.class,
                () -> transaction.executeWithoutResult(status -> manager.submit(payment(id))));
        assertRows(id, 1);
    }

    @Test
    void enqueueFailureMarksTheTransactionForRollbackEvenIfTheCallerCatchesIt() {
        var mapper = TaskCodec.defaultMapper();
        var results = new ResultMetaRegistry(SCAN_PACKAGES);
        var codec = new TaskCodec(new TaskMetaRegistry(results, SCAN_PACKAGES), results, mapper);
        var failure = new IllegalStateException("enqueue failed");
        var failingManager = new TaskManager(dto -> CompletableFuture.failedFuture(failure),
                events, codec, repository, mapper, transactionManager);
        var task = payment(id);

        assertThrows(UnexpectedRollbackException.class, () -> transaction.executeWithoutResult(status ->
                assertSame(failure, assertThrows(IllegalStateException.class, () -> failingManager.submit(task)))));

        assertRows(id, 0);
        events.publish(Events.accepted(id, 0, "banker_bot"));
        assertEquals(TaskState.NEW, task.state());
        assertFalse(task.result().isDone());
    }

    @Test
    void persistentSubmitRequiresACallerTransaction() {
        assertThrows(IllegalTransactionStateException.class, () -> manager.submit(payment(id)));
        assertRows(id, 0);
    }

    private static SendPayment payment(UUID id) {
        return new SendPayment(id, "seller", new BigDecimal("900.00"));
    }

    private void assertRows(UUID id, long count) {
        assertEquals(count, jdbc.sql("SELECT count(*) FROM task WHERE task_id = ?")
                .param(id).query(Long.class).single());
        assertEquals(count, jdbc.sql("SELECT count(*) FROM outbox WHERE message_id = ?")
                .param(id).query(Long.class).single());
    }
}
