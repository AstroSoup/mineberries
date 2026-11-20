package ru.astrosoup.mineberries.messaging.common;

import static org.junit.jupiter.api.Assertions.*;
import static ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.banker.results.PaymentSentResult;
import ru.astrosoup.mineberries.messaging.banker.tasks.SendPayment;
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
        var results = new ResultMetaRegistry(SCAN_PACKAGES);
        var tasks = new TaskMetaRegistry(results, SCAN_PACKAGES);
        manager = new TaskManager(broker, broker, new TaskCodec(tasks, results, TaskCodec.defaultMapper()));
    }

    private static Throwable failureOf(CompletableFuture<?> future) {
        var ex = assertThrows(CompletionException.class, future::join);
        return ex.getCause();
    }

    private static SendPayment payment(String recipient, String amount) {
        return new SendPayment(recipient, new BigDecimal(amount));
    }

    private static Map<String, Object> paymentPayload(String recipient, String amount) {
        return Map.of(
                "recipientNickname", recipient,
                "amount", new BigDecimal(amount),
                "paidAt", "2026-11-18T10:17:00Z");
    }

    // ---- submit ----

    @Test
    void submitSendsEncodedTaskAndReturnsIt() {
        var task = payment("seller", "900.00");

        assertSame(task, manager.submit(task));

        assertEquals(1, broker.sent().size());
        var msg = broker.lastSent();
        assertEquals(task.id(), msg.messageId());
        assertEquals("banker.send_payment", msg.type());
        assertEquals("seller", msg.payload().get("recipientNickname"));
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
        var task = manager.submit(payment("seller", "900.00"));
        assertThrows(IllegalStateException.class, () -> manager.submit(task));
        assertEquals(1, broker.sent().size());
    }

    @Test
    void sendFailureThrowsFromSubmitAndDoesNotSettleTheTaskResult() {
        var boom = new IllegalStateException("enqueue failed");
        broker.failNextSend(boom);
        var task = payment("seller", "900.00");

        assertSame(boom, assertThrows(IllegalStateException.class, () -> manager.submit(task)));
        assertFalse(task.result().isDone());
        assertTrue(broker.sent().isEmpty());

        assertSame(task, manager.submit(task));
        assertEquals(1, broker.sent().size());
    }

    // ---- lifecycle ----

    @Test
    void acceptedMovesTaskToAccepted() {
        var task = manager.submit(payment("seller", "900.00"));
        broker.publish(Events.accepted(task.id(), 0, "banker_bot"));
        assertEquals(TaskState.ACCEPTED, task.state());
        assertFalse(task.result().isDone());
    }

    @Test
    void progressNotifiesListenersAndUpdatesProgress() {
        var task = manager.submit(payment("seller", "900.00"));
        var stages = new ArrayList<String>();
        task.onProgress(p -> stages.add(p.stage()));

        broker.publish(Events.accepted(task.id(), 0, "banker_bot"));
        broker.publish(Events.progress(task.id(), 1, "packing"));
        broker.publish(Events.progress(task.id(), 2, "shipping"));

        assertEquals(List.of("packing", "shipping"), stages);
        assertEquals("shipping", task.progress().stage());
    }

    @Test
    void completedDeliversDecodedResult() {
        var task = manager.submit(payment("seller", "900.00"));

        broker.publish(Events.accepted(task.id(), 0, "banker_bot"));
        broker.publish(Events.completed(task.id(), 1, paymentPayload("seller", "900.00")));

        assertEquals(TaskState.COMPLETED, task.state());
        PaymentSentResult r = task.result().join();
        assertEquals("seller", r.recipientNickname());
        assertEquals(new BigDecimal("900.00"), r.amount());
        assertEquals(Instant.parse("2026-11-18T10:17:00Z"), r.paidAt());
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
        var task = manager.submit(payment("seller", "900.00"));

        broker.publish(Events.failed(task.id(), 0, "boom", true, false));

        assertEquals(TaskState.FAILED, task.state());
        var ex = assertInstanceOf(TaskFailedException.class, failureOf(task.result()));
        assertEquals("boom", ex.getMessage());
        assertEquals(new TaskError("boom", true, false), ex.error());
    }

    @Test
    void failedWithUnknownOutcomeExposesTheFlag() {
        var task = manager.submit(payment("seller", "900.00"));

        broker.publish(Events.failed(task.id(), 0, "payment outcome unknown", false, true));

        var ex = assertInstanceOf(TaskFailedException.class, failureOf(task.result()));
        assertEquals(new TaskError("payment outcome unknown", false, true), ex.error());
        assertTrue(ex.error().outcomeUnknown());
    }

    @Test
    void canceledCancelsTheResult() {
        var task = manager.submit(payment("seller", "900.00"));

        broker.publish(Events.canceled(task.id(), 0));

        assertEquals(TaskState.CANCELLED, task.state());
        assertTrue(task.result().isCompletedExceptionally());
    }

    // ---- ordering / robustness ----

    @Test
    void duplicateAndOutOfOrderEventsAreIgnored() {
        var task = manager.submit(payment("seller", "900.00"));
        var stages = new ArrayList<String>();
        task.onProgress(p -> stages.add(p.stage()));

        broker.publish(Events.accepted(task.id(), 0, "banker_bot"));
        broker.publish(Events.progress(task.id(), 2, "b"));
        broker.publish(Events.progress(task.id(), 1, "a"));  // older version
        broker.publish(Events.progress(task.id(), 2, "b2")); // duplicate version

        assertEquals(List.of("b"), stages);
        assertEquals("b", task.progress().stage());
    }

    @Test
    void eventsAfterTerminalStateAreIgnored() {
        var task = manager.submit(payment("seller", "900.00"));
        broker.publish(Events.completed(task.id(), 0, paymentPayload("seller", "900.00")));

        broker.publish(Events.failed(task.id(), 1, "late", false, false));

        assertEquals(TaskState.COMPLETED, task.state());
        assertFalse(task.result().isCompletedExceptionally());
    }

    @Test
    void eventsForUnknownTasksAreIgnored() {
        assertDoesNotThrow(() -> broker.publish(Events.accepted(UUID.randomUUID(), 0, "banker_bot")));
    }

    @Test
    void undecodableEventFailsTheTask() {
        var task = manager.submit(payment("seller", "900.00"));

        broker.publish(Events.completed(task.id(), 0, Map.of("recipientNickname", "seller"))); // "amount" missing

        assertInstanceOf(IllegalStateException.class, failureOf(task.result()));
        assertFalse(task.state().isTerminal());
    }

    @Test
    void concurrentTasksAreRoutedById() {
        var a = manager.submit(payment("seller_a", "100.00"));
        var b = manager.submit(payment("seller_b", "200.00"));

        broker.publish(Events.completed(b.id(), 0, paymentPayload("seller_b", "200.00")));

        assertFalse(a.result().isDone());
        assertEquals("seller_b", ((PaymentSentResult) b.result().join()).recipientNickname());

        broker.publish(Events.completed(a.id(), 0, paymentPayload("seller_a", "100.00")));
        assertEquals("seller_a", ((PaymentSentResult) a.result().join()).recipientNickname());
    }
}
