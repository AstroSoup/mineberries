package ru.astrosoup.mineberries.messaging.common;

import static org.junit.jupiter.api.Assertions.*;
import static ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.SCAN_PACKAGES;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import ru.astrosoup.mineberries.integration.AbstractIntegrationTest;
import ru.astrosoup.mineberries.messaging.banker.tasks.CollectPayment;
import ru.astrosoup.mineberries.messaging.banker.tasks.SendPayment;
import ru.astrosoup.mineberries.messaging.banker.tasks.SendVerificationCode;
import ru.astrosoup.mineberries.messaging.common.codec.ResultMetaRegistry;
import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.common.codec.TaskMetaRegistry;
import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;
import ru.astrosoup.mineberries.messaging.common.exceptions.TaskFailedException;
import ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.ExpiringTask;
import ru.astrosoup.mineberries.messaging.outbox.OutboxMessageSender;
import ru.astrosoup.mineberries.messaging.support.Events;
import ru.astrosoup.mineberries.messaging.support.InMemoryBroker;
import ru.astrosoup.mineberries.messaging.tasks.NewTask;
import ru.astrosoup.mineberries.messaging.tasks.TaskRecord;
import ru.astrosoup.mineberries.messaging.tasks.TaskRepository;
import tools.jackson.databind.ObjectMapper;

class TaskManagerIntegrationTest extends AbstractIntegrationTest {
    private final UUID id = UUID.randomUUID();
    private final UUID otherId = UUID.randomUUID();
    private final HashSet<UUID> testIds = new HashSet<>(List.of(id, otherId));
    private final InMemoryBroker events = new InMemoryBroker();
    private final ObjectMapper mapper = TaskCodec.defaultMapper();
    private JdbcClient jdbc;
    private TaskRepository repository;
    private DataSourceTransactionManager transactionManager;
    private TransactionTemplate transaction;
    private OutboxMessageSender outbox;
    private TaskCodec codec;
    private TaskManager manager;

    @BeforeEach
    void setUp() {
        var dataSource = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        jdbc = JdbcClient.create(dataSource);
        repository = new TaskRepository(jdbc);
        transactionManager = new DataSourceTransactionManager(dataSource);
        transaction = new TransactionTemplate(transactionManager);
        outbox = new OutboxMessageSender(dataSource, mapper);
        var results = new ResultMetaRegistry(SCAN_PACKAGES);
        codec = new TaskCodec(new TaskMetaRegistry(results, SCAN_PACKAGES), results, mapper);
        manager = new TaskManager(outbox, events, codec, repository, mapper, transactionManager);
    }

    @AfterEach
    void deleteRows() {
        jdbc.sql("DELETE FROM outbox WHERE message_id IN (:ids)").param("ids", testIds).update();
        jdbc.sql("DELETE FROM task WHERE task_id IN (:ids)").param("ids", testIds).update();
    }

    @Test
    void commitPersistsBothRowsAndActivatesTheTaskOnlyAfterCommit() {
        var task = payment(id);
        var future = task.result();
        var delivery = new TransactionTemplate(transactionManager);
        delivery.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transaction.executeWithoutResult(status -> {
            assertSame(task, manager.submit(task));
            assertRows(id, 1);
            delivery.executeWithoutResult(deliveryStatus -> events.publish(Events.accepted(id, 0, "banker_bot")));
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

    @Test
    void persistsTheLifecycleAndNotifiesTheTaskAfterCommit() {
        var task = transaction.execute(status -> manager.submit(payment(id)));
        var stages = new ArrayList<String>();
        task.onProgress(progress -> stages.add(progress.stage()));
        var future = task.result();

        transaction.executeWithoutResult(status -> {
            events.publish(Events.accepted(id, 0, "banker_bot"));
            var accepted = stored(id);
            assertEquals(TaskState.ACCEPTED, accepted.state());
            assertEquals("banker_bot", accepted.assignedBot());
            assertEquals(0L, accepted.lastEventVersion());
            assertEquals(TaskState.NEW, task.state());

            events.publish(Events.progress(id, 1, "waiting"));
            var progressing = stored(id);
            assertEquals(TaskState.ACCEPTED, progressing.state());
            assertEquals("banker_bot", progressing.assignedBot());
            assertEquals("waiting", progressing.progressStage());
            assertEquals(1L, progressing.lastEventVersion());
            assertTrue(stages.isEmpty());
        });

        assertEquals(TaskState.ACCEPTED, task.state());
        assertEquals(List.of("waiting"), stages);
        transaction.executeWithoutResult(status -> {
            events.publish(Events.completed(id, 2, paymentResult()));
            assertEquals(TaskState.COMPLETED, stored(id).state());
            assertFalse(future.isDone());
        });

        var completed = stored(id);
        assertEquals(TaskState.COMPLETED, completed.state());
        assertEquals(2L, completed.lastEventVersion());
        assertEquals("waiting", completed.progressStage());
        assertEquals("banker_bot", completed.assignedBot());
        assertEquals(mapper.readTree(mapper.writeValueAsString(paymentResult())), mapper.readTree(completed.result()));
        assertNull(completed.error());
        assertEquals("seller", future.join().recipientNickname());
    }

    @Test
    void ignoresDuplicatesOlderVersionsLateEventsAndUnknownTasksWithoutChangingRows() {
        transaction.executeWithoutResult(status -> manager.submit(payment(id)));
        events.publish(Events.accepted(id, 0, "banker_bot"));
        events.publish(Events.progress(id, 3, "latest"));
        var before = stored(id);

        events.publish(Events.progress(id, 3, "duplicate"));
        events.publish(Events.accepted(id, 2, "other_bot"));
        events.publish(Events.accepted(otherId, 0, "unknown_bot"));
        assertEquals(before, stored(id));
        assertRows(otherId, 0);

        events.publish(Events.completed(id, 4, paymentResult()));
        var completed = stored(id);
        events.publish(Events.failed(id, 5, "late", false, false));
        events.publish(Events.progress(id, 6, "also late"));
        assertEquals(completed, stored(id));
    }

    @Test
    void rollsBackATerminalEventWithoutSettlingItsFuture() {
        var task = transaction.execute(status -> manager.submit(payment(id)));
        var before = stored(id);
        var future = task.result();
        var completed = Events.completed(id, 0, paymentResult());
        transaction.executeWithoutResult(status -> {
            events.publish(completed);
            assertEquals(TaskState.COMPLETED, stored(id).state());
            assertFalse(future.isDone());
            status.setRollbackOnly();
        });

        assertEquals(before, stored(id));
        assertEquals(TaskState.NEW, task.state());
        assertFalse(future.isDone());
        events.publish(completed);
        assertEquals(TaskState.COMPLETED, stored(id).state());
        assertEquals("seller", future.join().recipientNickname());
    }

    @Test
    void newerCommitCallbackUsesPersistedStateWhenTheEarlierCallbackIsDelayed() throws Exception {
        var task = transaction.execute(status -> manager.submit(payment(id)));
        var afterCommit = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var accepted = executor.submit(() -> transaction.executeWithoutResult(status -> {
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        afterCommit.countDown();
                        try {
                            assertTrue(release.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                });
                events.publish(Events.accepted(id, 0, "banker_bot"));
            }));
            try {
                assertTrue(afterCommit.await(5, TimeUnit.SECONDS));
                assertEquals(TaskState.NEW, task.state());
                events.publish(Events.progress(id, 1, "waiting"));
                assertEquals(TaskState.ACCEPTED, task.state());
                assertEquals("waiting", task.progress().stage());
            } finally {
                release.countDown();
            }
            accepted.get(5, TimeUnit.SECONDS);
        }
        assertEquals("waiting", task.progress().stage());
    }

    @Test
    void appliesEventsWithoutAnInMemoryTask() {
        transaction.executeWithoutResult(status -> {
            for (UUID taskId : testIds) {
                repository.insert(new NewTask(taskId, "banker.send_payment",
                        "{\"recipientNickname\":\"seller\",\"amount\":900.00}", null, null, null));
            }
        });
        events.publish(Events.accepted(id, 0, "banker_bot"));
        events.publish(Events.failed(id, 1, "Unknown outcome", false, true));
        var failed = stored(id);
        assertEquals(TaskState.FAILED, failed.state());
        assertEquals("banker_bot", failed.assignedBot());
        assertEquals(1L, failed.lastEventVersion());
        assertNull(failed.result());
        assertEquals(new TaskError("Unknown outcome", false, true), mapper.readValue(failed.error(), TaskError.class));

        events.publish(Events.completed(otherId, 0, paymentResult()));
        var completed = stored(otherId);
        assertEquals(TaskState.COMPLETED, completed.state());
        assertEquals("seller", mapper.readTree(completed.result()).path("recipientNickname").asString());
    }

    @Test
    void restartsWithActiveAndUnprocessedTerminalTasksWithoutReplayingEvents() {
        var running = payment(id);
        var queued = payment(otherId);
        var completed = payment(testId());
        var failed = payment(testId());
        var cancelled = payment(testId());
        var processed = payment(testId());
        transaction.executeWithoutResult(status ->
                List.of(running, queued, completed, failed, cancelled, processed).forEach(manager::submit));
        events.publish(Events.accepted(running.id(), 0, "banker_bot"));
        events.publish(Events.progress(running.id(), 3, "waiting"));
        events.publish(Events.completed(completed.id(), 0, paymentResult()));
        events.publish(Events.failed(failed.id(), 0, "Unknown outcome", false, true));
        events.publish(Events.canceled(cancelled.id(), 0));
        events.publish(Events.completed(processed.id(), 0, paymentResult()));
        assertTrue(repository.markProcessed(processed.id()));

        var newEvents = new InMemoryBroker();
        var restarted = new MessagingConfig().taskManager(outbox, newEvents, codec, repository, mapper, transactionManager,
                "5m", TaskDeadlines.defaults());
        assertEquals(Set.of(running.id(), queued.id()), taskIds(restarted.activeTasks()));
        var terminal = restarted.terminalUnprocessedTasks();
        assertEquals(Set.of(completed.id(), failed.id(), cancelled.id()), taskIds(terminal));
        assertEquals(TaskState.NEW, taskWithId(restarted.activeTasks(), queued.id()).state());

        var recoveredResult = taskWithId(terminal, completed.id());
        assertEquals(TaskState.COMPLETED, recoveredResult.state());
        assertTrue(recoveredResult.result().isDone());
        assertEquals(completed.result().join(), recoveredResult.result().join());
        var recoveredFailure = assertThrows(CompletionException.class, taskWithId(terminal, failed.id()).result()::join);
        assertEquals(new TaskError("Unknown outcome", false, true),
                assertInstanceOf(TaskFailedException.class, recoveredFailure.getCause()).error());
        var recoveredCancellation = taskWithId(terminal, cancelled.id());
        assertEquals(TaskState.CANCELLED, recoveredCancellation.state());
        assertInstanceOf(CancellationException.class,
                assertThrows(CompletionException.class, recoveredCancellation.result()::join).getCause());
        assertEquals(taskIds(terminal), repository.listTerminalUnprocessed().stream()
                .map(TaskRecord::taskId).collect(Collectors.toSet()));

        var live = taskWithId(restarted.activeTasks(), running.id());
        assertNotSame(running, live);
        assertEquals(TaskState.ACCEPTED, live.state());
        assertEquals("waiting", live.progress().stage());
        var stages = new ArrayList<String>();
        live.onProgress(progress -> stages.add(progress.stage()));
        newEvents.publish(Events.progress(id, 2, "stale"));
        newEvents.publish(Events.progress(id, 3, "duplicate"));
        assertEquals("waiting", live.progress().stage());
        assertTrue(stages.isEmpty());
        newEvents.publish(Events.progress(id, 4, "confirmed"));
        assertEquals(List.of("confirmed"), stages);
        assertEquals("confirmed", stored(id).progressStage());
        assertFalse(live.result().isDone());
        newEvents.publish(Events.completed(id, 5, paymentResult()));
        assertEquals(TaskState.COMPLETED, live.state());
        assertTrue(live.result().isDone());
        assertEquals(Set.of(queued.id()), taskIds(restarted.activeTasks()));
    }

    @Test
    @ExtendWith(OutputCaptureExtension.class)
    void skipsAndLogsUndecodableRowsWithoutExposingPayloads(CapturedOutput output) {
        var invalid = testId();
        var unknown = testId();
        var marker = "test-sensitive-value";
        transaction.executeWithoutResult(status -> {
            repository.insert(new NewTask(unknown, "unknown.task", "{}", null, null, null));
            repository.insert(new NewTask(invalid, "banker.send_payment",
                    "{\"recipientNickname\":\"" + marker + "\"}", null, null, null));
            manager.submit(payment(id));
        });
        var originalInvalid = stored(invalid);
        var originalUnknown = stored(unknown);

        var restarted = new TaskManager(outbox, new InMemoryBroker(), codec, repository, mapper, transactionManager);

        assertEquals(Set.of(id), taskIds(restarted.activeTasks()));
        assertTrue(restarted.terminalUnprocessedTasks().isEmpty());
        for (var skipped : List.of(invalid, unknown)) {
            assertTrue(output.getAll().lines()
                    .anyMatch(line -> line.contains("ERROR") && line.contains("Could not rehydrate task " + skipped)));
        }
        assertFalse(output.getAll().contains(marker));
        assertEquals(originalInvalid, stored(invalid));
        assertEquals(originalUnknown, stored(unknown));
    }

    @Nested
    class Cancellation {
        @ParameterizedTest
        @EnumSource(value = TaskState.class, names = {"NEW", "ACCEPTED"})
        void requestsCancellationWithoutChangingStateUntilTheBotResponds(TaskState initialState) {
            var task = transaction.execute(status -> manager.submit(payment(id)));
            if (initialState == TaskState.ACCEPTED) {
                events.publish(Events.accepted(id, 0, "banker_bot"));
            }
            var before = stored(id);
            var result = task.result();

            var cancelled = manager.cancel(task);

            var requested = stored(id);
            assertNotNull(requested.cancelRequestedAt());
            assertEquals(initialState, requested.state());
            assertEquals(before.lastEventVersion(), requested.lastEventVersion());
            assertEquals(before.assignedBot(), requested.assignedBot());
            assertEquals(initialState, task.state());
            assertFalse(cancelled.isDone());
            assertFalse(result.isDone());
            assertCancelMessage();

            events.publish(Events.canceled(id, before.lastEventVersion() + 1));
            assertEquals(TaskState.CANCELLED, task.state());
            assertEquals(TaskState.CANCELLED, stored(id).state());
            assertInstanceOf(CancellationException.class,
                    assertThrows(CompletionException.class, cancelled::join).getCause());
            assertTrue(result.isCompletedExceptionally());
        }

        @Test
        void alreadyCompletedTaskKeepsItsResultWithoutEnqueuingAnything() {
            var task = transaction.execute(status -> manager.submit(payment(id)));
            events.publish(Events.completed(id, 0, paymentResult()));
            var completed = stored(id);

            assertEquals(task.result().join(), manager.cancel(task).join());
            assertEquals(completed, stored(id));
            assertTrue(cancelMessages().isEmpty());
        }

        @Test
        void repeatedCancellationDoesNotEnqueueAnotherMessageEvenAfterRestart() {
            var task = transaction.execute(status -> manager.submit(payment(id)));
            manager.cancel(task);
            var firstRequest = stored(id);
            manager.cancel(task);

            var restarted = new TaskManager(outbox, new InMemoryBroker(), codec, repository, mapper, transactionManager);
            restarted.cancel(taskWithId(restarted.activeTasks(), id));

            assertEquals(firstRequest, stored(id));
            assertCancelMessage();
        }

        @Test
        void completionCanWinEvenAfterCancellationWasRequested() {
            var task = transaction.execute(status -> manager.submit(payment(id)));
            events.publish(Events.accepted(id, 0, "banker_bot"));
            var result = manager.cancel(task);

            events.publish(Events.completed(id, 1, paymentResult()));
            var completed = stored(id);
            events.publish(Events.canceled(id, 2));

            assertEquals("seller", result.join().recipientNickname());
            assertEquals(TaskState.COMPLETED, task.state());
            assertEquals(TaskState.COMPLETED, completed.state());
            assertNotNull(completed.cancelRequestedAt());
            assertEquals(completed, stored(id));
            assertCancelMessage();
        }

        @Test
        void rollingBackCancellationLeavesNoMarkerOrCancelMessageAndAllowsRetry() {
            var task = transaction.execute(status -> manager.submit(payment(id)));
            transaction.executeWithoutResult(status -> {
                manager.cancel(task);
                assertNotNull(stored(id).cancelRequestedAt());
                assertCancelMessage();
                status.setRollbackOnly();
            });

            assertNull(stored(id).cancelRequestedAt());
            assertTrue(cancelMessages().isEmpty());
            manager.cancel(task);
            assertNotNull(stored(id).cancelRequestedAt());
            assertCancelMessage();
        }

        private List<String> cancelMessages() {
            return jdbc.sql("SELECT payload::text FROM outbox WHERE message_id = ? AND kind = 'cancel'")
                    .param(id).query(String.class).list();
        }

        private void assertCancelMessage() {
            var messages = cancelMessages();
            assertEquals(1, messages.size());
            assertEquals(mapper.readTree("""
                    {"messageId":"%s","kind":"cancel","schemaVersion":1}
                    """.formatted(id)), mapper.readTree(messages.getFirst()));
        }
    }

    @Nested
    class Deadlines {
        private final InMemoryBroker timedEvents = new InMemoryBroker();
        private final Instant submittedAt = Instant.parse("2026-11-21T12:00:00Z");
        private final AtomicReference<Instant> time = new AtomicReference<>(submittedAt);
        private final InstantSource instantSource = time::get;
        private final Clock clock = instantSource.withZone(ZoneOffset.UTC);
        private final List<LateTaskCompletion> notices = new ArrayList<>();
        private final Duration codeBuffer = Duration.ofMinutes(2);
        private final Duration paymentBuffer = Duration.ofMinutes(5);
        private final Duration cancelGrace = Duration.ofMinutes(2);
        private final TaskDeadlines deadlines =
                new TaskDeadlines(clock, codeBuffer, paymentBuffer, cancelGrace, event -> notices.add((LateTaskCompletion) event));

        @ParameterizedTest
        @CsvSource({
                "false, -60,  -30, false", // On time.
                "false,  60,   90, false", // Inside the buffer.
                "false, 120,  121, false", // Inclusive end of the buffer.
                "false, 121,  122, true",
                "false, -60, 3600, false", // Delayed event, on-time result.
                "true,  -60,  -30, false",
                "true,   60,   90, false",
                "true,  300,  301, false",
                "true,  301,  302, true",
                "true,  -60, 3600, false"
        })
        void comparesResultTimestampWithThePersistedDeadline(
                boolean collectPayment, long resultOffset, long arrivalOffset, boolean late) {
            var timed = timedManager(timedEvents, deadlines);
            var task = collectPayment ? collection(id) : verification(id);
            transaction.executeWithoutResult(status -> timed.submit(task));
            var deadline = submittedAt.plusSeconds(collectPayment ? 900 : 120);
            var resultAt = deadline.plusSeconds(resultOffset);
            assertEquals(collectPayment ? null : deadline, stored(id).expiresAt());
            assertEquals(collectPayment ? deadline : null, stored(id).finishBy());
            var envelope = mapper.readTree(jdbc.sql("SELECT payload::text FROM outbox WHERE message_id = ?")
                    .param(id).query(String.class).single());
            assertEquals(deadline.toString(), envelope.path("expiresAt").asString());
            if (collectPayment) assertEquals(900, envelope.path("payload").path("timeoutSeconds").asInt());

            time.set(deadline.plusSeconds(arrivalOffset));
            var completed = new EventDto(id, 0, EventType.COMPLETED, time.get(), collectPayment
                    ? Map.of("payerNickname", "buyer", "amount", new BigDecimal("12.00"), "paidAt", resultAt.toString())
                    : codeResult(resultAt));
            transaction.executeWithoutResult(status -> {
                timedEvents.publish(completed);
                assertTrue(notices.isEmpty());
                assertFalse(task.result().isDone());
            });
            timedEvents.publish(completed);

            assertEquals(TaskState.COMPLETED, stored(id).state());
            assertEquals(TaskState.COMPLETED, task.state());
            assertNotNull(task.result().join());
            assertEquals(late, stored(id).completedLate());
            var limit = deadline.plus(collectPayment ? paymentBuffer : codeBuffer);
            assertEquals(late ? List.of(new LateTaskCompletion(id, stored(id).type(), deadline, limit, resultAt))
                    : List.of(), notices);
        }

        @Test
        void rollsBackLateCompletionWithoutNotifyingAndUsesTheConfiguredBuffer() {
            var timed = timedManager(timedEvents, new TaskDeadlines(clock, Duration.ZERO, paymentBuffer, cancelGrace,
                    event -> notices.add((LateTaskCompletion) event)));
            var task = transaction.execute(status -> timed.submit(verification(id)));
            var completion = Events.completed(id, 0, codeResult(submittedAt.plusSeconds(121)));
            time.set(submittedAt.plusSeconds(125));
            transaction.executeWithoutResult(status -> {
                timedEvents.publish(completion);
                assertTrue(stored(id).completedLate());
                status.setRollbackOnly();
            });
            assertEquals(TaskState.NEW, stored(id).state());
            assertFalse(stored(id).completedLate());
            assertFalse(task.result().isDone());
            assertTrue(notices.isEmpty());
            timedEvents.publish(completion);
            assertTrue(stored(id).completedLate());
            assertEquals(1, notices.size());
        }

        @Test
        void watchdogCancelsOnlyTimedTasksAndFailsAfterGraceEvenAcrossRestart() {
            var timed = timedManager(timedEvents, deadlines);
            transaction.executeWithoutResult(status -> {
                timed.submit(verification(id));
                timed.submit(collection(otherId));
            });
            var untimed = transaction.execute(status -> timed.submit(payment(testId())));

            time.set(submittedAt.plus(Duration.ofMinutes(4)));
            timed.scanDeadlines();
            assertEquals(0, cancelCount(id));
            time.set(time.get().plusSeconds(1));
            timed.scanDeadlines();
            timed.scanDeadlines();
            assertEquals(1, cancelCount(id));
            assertEquals(time.get(), stored(id).cancelRequestedAt());
            assertEquals(TaskState.NEW, stored(id).state());
            assertEquals(0, cancelCount(otherId));

            var newEvents = new InMemoryBroker();
            var restarted = timedManager(newEvents, deadlines);
            var recovered = taskWithId(restarted.activeTasks(), id);
            var recoveredPayment = taskWithId(restarted.activeTasks(), otherId);
            time.set(time.get().plus(cancelGrace).minusSeconds(1));
            restarted.scanDeadlines();
            assertFalse(recovered.result().isDone());
            time.set(time.get().plusSeconds(1));
            restarted.scanDeadlines();
            assertTimedOut(recovered);
            assertEquals(-1, stored(id).lastEventVersion());
            newEvents.publish(Events.completed(id, 0, codeResult(submittedAt.plusSeconds(30))));
            assertEquals(TaskState.FAILED, stored(id).state());
            assertEquals(1, cancelCount(id));

            time.set(submittedAt.plusSeconds(20 * 60 + 1));
            restarted.scanDeadlines();
            assertEquals(1, cancelCount(otherId));
            time.set(time.get().plus(cancelGrace));
            restarted.scanDeadlines();
            assertTimedOut(recoveredPayment);
            assertEquals(TaskState.NEW, stored(untimed.id()).state());
            assertEquals(0, cancelCount(untimed.id()));
            assertTrue(notices.isEmpty());
        }

        @Test
        void newerProgressExtendsGraceButDuplicateProgressDoesNot() {
            var timed = timedManager(timedEvents, deadlines);
            var task = transaction.execute(status -> timed.submit(verification(id)));
            timedEvents.publish(Events.accepted(id, 0, "banker_bot"));

            time.set(submittedAt.plusSeconds(241));
            timed.scanDeadlines();
            time.set(time.get().plusSeconds(90));
            var progress = Events.progress(id, 1, "delivering");
            timedEvents.publish(progress);
            time.set(time.get().plusSeconds(60));
            timedEvents.publish(progress);
            timed.scanDeadlines();
            assertEquals(TaskState.ACCEPTED, task.state());
            assertFalse(task.result().isDone());
            time.set(time.get().plusSeconds(60));
            timed.scanDeadlines();
            assertTimedOut(task);
            assertEquals(1, stored(id).lastEventVersion());
            assertEquals("delivering", task.progress().stage());
            assertEquals(1, cancelCount(id));
        }

        @Test
        void completionDuringCancellationGraceWinsAndUsesTheResultTime() {
            var timed = timedManager(timedEvents, deadlines);
            var task = transaction.execute(status -> timed.submit(verification(id)));
            time.set(submittedAt.plusSeconds(241));
            timed.scanDeadlines();
            timedEvents.publish(Events.completed(id, 0, codeResult(submittedAt.plusSeconds(10))));
            time.set(time.get().plus(cancelGrace));
            timed.scanDeadlines();
            assertEquals(TaskState.COMPLETED, task.state());
            assertEquals(submittedAt.plusSeconds(10), task.result().join().deliveredAt());
            assertFalse(stored(id).completedLate());
            assertEquals(1, cancelCount(id));
            assertTrue(notices.isEmpty());
        }

        private TaskManager timedManager(InMemoryBroker source, TaskDeadlines policy) {
            return new TaskManager(outbox, source, codec, repository, mapper, transactionManager,
                    Duration.ofMinutes(5), policy);
        }

        private void assertTimedOut(AbstractTask<?> task) {
            assertEquals(TaskState.FAILED, stored(task.id()).state());
            assertEquals(TaskState.FAILED, task.state());
            var cause = assertThrows(CompletionException.class, task.result()::join).getCause();
            var error = assertInstanceOf(TaskFailedException.class, cause).error();
            assertTrue(error.outcomeUnknown());
            assertFalse(error.retryable());
            assertTrue(mapper.readTree(stored(task.id()).error()).path("outcomeUnknown").asBoolean());
        }

        private long cancelCount(UUID taskId) {
            return jdbc.sql("SELECT count(*) FROM outbox WHERE message_id = ? AND kind = 'cancel'")
                    .param(taskId).query(Long.class).single();
        }

        private Map<String, Object> codeResult(Instant deliveredAt) {
            return Map.of("nickname", "buyer", "deliveredAt", deliveredAt.toString());
        }

        private SendVerificationCode verification(UUID taskId) {
            return new SendVerificationCode(taskId, "buyer", "123456");
        }

        private CollectPayment collection(UUID taskId) {
            return new CollectPayment(taskId, "buyer", new BigDecimal("12.00"), 900);
        }
    }

    @Nested
    class ExistingScenarios extends TaskManagerTest {
        @Override
        TaskManager createManager(InMemoryBroker broker, TaskCodec codec, ObjectMapper mapper) {
            return new TaskManager(dto -> broker.send(dto).thenCompose(ignored -> outbox.send(dto)),
                    handler -> broker.subscribe(dto -> transaction.executeWithoutResult(status -> handler.accept(dto))),
                    codec, repository, mapper, transactionManager);
        }

        @Override
        <T extends AbstractTask<?>> T submit(T task) {
            testIds.add(task.id());
            return transaction.execute(status -> super.submit(task));
        }
    }

    private TaskRecord stored(UUID id) {
        return transaction.execute(status -> repository.findByIdForUpdate(id).orElseThrow());
    }

    private UUID testId() {
        var id = UUID.randomUUID();
        testIds.add(id);
        return id;
    }

    private static AbstractTask<?> taskWithId(List<AbstractTask<?>> tasks, UUID id) {
        return tasks.stream().filter(task -> task.id().equals(id)).findFirst().orElseThrow();
    }

    private static Set<UUID> taskIds(List<AbstractTask<?>> tasks) {
        return tasks.stream().map(AbstractTask::id).collect(Collectors.toSet());
    }

    private static Map<String, Object> paymentResult() {
        return Map.of("recipientNickname", "seller", "amount", new BigDecimal("900.00"),
                "paidAt", "2026-11-21T12:00:00Z");
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
