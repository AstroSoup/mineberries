package ru.astrosoup.mineberries.messaging.common;

import static org.junit.jupiter.api.Assertions.*;
import static ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.common.codec.ResultMetaRegistry;
import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.common.codec.TaskMetaRegistry;
import ru.astrosoup.mineberries.messaging.common.exceptions.TaskFailedException;
import ru.astrosoup.mineberries.messaging.support.Events;
import ru.astrosoup.mineberries.messaging.support.InMemoryBroker;

class TaskManagerTest {
    private final InMemoryBroker broker = new InMemoryBroker();
    private final TaskManager manager;

    TaskManagerTest() {
        var results = new ResultMetaRegistry(PACKAGE);
        var tasks = new TaskMetaRegistry(results, PACKAGE);
        manager = new TaskManager(broker, broker, new TaskCodec(tasks, results, TaskCodec.defaultMapper()));
    }

    private static Throwable failureOf(CompletableFuture<?> future) {
        var ex = assertThrows(CompletionException.class, future::join);
        return ex.getCause();
    }

    // ---- submit ----

    @Test
    void submitSendsEncodedTaskAndReturnsIt() {
        var task = new GreetTask("Ann", 3);

        assertSame(task, manager.submit(task));

        assertEquals(1, broker.sent().size());
        var msg = broker.lastSent();
        assertEquals(task.id(), msg.messageId());
        assertEquals("greet", msg.type());
        assertEquals(Map.of("name", "Ann", "count", 3), msg.payload());
        assertEquals(TaskState.NEW, task.state());
        assertFalse(task.result().isDone());
    }

    @Test
    void submitRejectsUnregisteredTask() {
        var unregistered = new AbstractTask<Void>() {};
        assertThrows(IllegalArgumentException.class, () -> manager.submit(unregistered));
        assertTrue(broker.sent().isEmpty());
    }

    @Test
    void submittingTheSameTaskTwiceFails() {
        var task = manager.submit(new PingTask());
        assertThrows(IllegalStateException.class, () -> manager.submit(task));
        assertEquals(1, broker.sent().size());
    }

    @Test
    void sendFailureFailsTheTask() {
        var boom = new IllegalStateException("broker down");
        broker.failNextSend(boom);

        var task = manager.submit(new PingTask());

        assertSame(boom, failureOf(task.result()));
    }

    // ---- lifecycle ----

    @Test
    void acceptedMovesTaskToAccepted() {
        var task = manager.submit(new PingTask());
        broker.publish(Events.accepted(task.id(), 0));
        assertEquals(TaskState.ACCEPTED, task.state());
        assertFalse(task.result().isDone());
    }

    @Test
    void progressNotifiesListenersAndUpdatesProgress() {
        var task = manager.submit(new PingTask());
        var stages = new ArrayList<String>();
        task.onProgress(p -> stages.add(p.stage()));

        broker.publish(Events.accepted(task.id(), 0));
        broker.publish(Events.progress(task.id(), 1, "packing"));
        broker.publish(Events.progress(task.id(), 2, "shipping"));

        assertEquals(List.of("packing", "shipping"), stages);
        assertEquals("shipping", task.progress().stage());
    }

    @Test
    void completedDeliversDecodedResult() {
        var task = manager.submit(new GreetTask("Ann", 1));

        broker.publish(Events.accepted(task.id(), 0));
        broker.publish(Events.completed(task.id(), 1, Map.of("greeting", "hi Ann", "length", 6)));

        assertEquals(TaskState.COMPLETED, task.state());
        GreetResult r = task.result().join();
        assertEquals("hi Ann", r.greeting());
        assertEquals(6, r.length());
    }

    @Test
    void completedVoidTaskResolvesToNull() {
        var task = manager.submit(new PingTask());
        broker.publish(Events.of(task.id(), 0, EventType.COMPLETED, null));
        assertEquals(TaskState.COMPLETED, task.state());
        assertNull(task.result().join());
    }

    @Test
    void failedCompletesExceptionallyWithTaskError() {
        var task = manager.submit(new PingTask());

        broker.publish(Events.failed(task.id(), 0, "boom", true));

        assertEquals(TaskState.FAILED, task.state());
        var ex = assertInstanceOf(TaskFailedException.class, failureOf(task.result()));
        assertEquals("boom", ex.getMessage());
        assertEquals(new TaskError("boom", true), ex.error());
    }

    @Test
    void canceledCancelsTheResult() {
        var task = manager.submit(new PingTask());

        broker.publish(Events.canceled(task.id(), 0));

        assertEquals(TaskState.CANCELLED, task.state());
        assertTrue(task.result().isCompletedExceptionally());
    }

    // ---- ordering / robustness ----

    @Test
    void duplicateAndOutOfOrderEventsAreIgnored() {
        var task = manager.submit(new PingTask());
        var stages = new ArrayList<String>();
        task.onProgress(p -> stages.add(p.stage()));

        broker.publish(Events.accepted(task.id(), 0));
        broker.publish(Events.progress(task.id(), 2, "b"));
        broker.publish(Events.progress(task.id(), 1, "a"));  // older version
        broker.publish(Events.progress(task.id(), 2, "b2")); // duplicate version

        assertEquals(List.of("b"), stages);
        assertEquals("b", task.progress().stage());
    }

    @Test
    void eventsAfterTerminalStateAreIgnored() {
        var task = manager.submit(new PingTask());
        broker.publish(Events.of(task.id(), 0, EventType.COMPLETED, null));

        broker.publish(Events.failed(task.id(), 1, "late", false));

        assertEquals(TaskState.COMPLETED, task.state());
        assertFalse(task.result().isCompletedExceptionally());
    }

    @Test
    void eventsForUnknownTasksAreIgnored() {
        assertDoesNotThrow(() -> broker.publish(Events.accepted(UUID.randomUUID(), 0)));
    }

    @Test
    void undecodableEventFailsTheTask() {
        var task = manager.submit(new GreetTask("Ann", 1));

        broker.publish(Events.completed(task.id(), 0, Map.of("greeting", "hi"))); // "length" missing

        assertInstanceOf(IllegalStateException.class, failureOf(task.result()));
        assertFalse(task.state().isTerminal());
    }

    @Test
    void concurrentTasksAreRoutedById() {
        var a = manager.submit(new GreetTask("A", 1));
        var b = manager.submit(new GreetTask("B", 1));

        broker.publish(Events.completed(b.id(), 0, Map.of("greeting", "hi B", "length", 4)));

        assertFalse(a.result().isDone());
        assertEquals("hi B", ((GreetResult) b.result().join()).greeting());

        broker.publish(Events.completed(a.id(), 0, Map.of("greeting", "hi A", "length", 4)));
        assertEquals("hi A", ((GreetResult) a.result().join()).greeting());
    }
}
