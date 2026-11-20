package ru.astrosoup.mineberries.messaging.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.common.exceptions.TaskFailedException;

class AbstractTaskTest {

    private static final class StringTask extends AbstractTask<String> {
        private StringTask() {
            super(UUID.randomUUID());
        }
    }

    @Test
    void restoresNewState() {
        var task = new StringTask();
        var progress = new TaskProgress("queued");

        task.restore(TaskState.NEW, 0, progress, null, null);

        assertEquals(TaskState.NEW, task.state());
        assertEquals(progress, task.progress());
        assertFalse(task.result().isDone());

        task.apply(new TaskEvent.Accepted<>(1, "bot"));
        assertEquals(TaskState.ACCEPTED, task.state());
    }

    @Test
    void restoresAcceptedStateAndKeepsVersionOrdering() {
        var task = new StringTask();
        var progress = new TaskProgress("moving");
        var notified = new AtomicReference<TaskProgress>();
        task.onProgress(notified::set);

        task.restore(TaskState.ACCEPTED, 3, progress, null, null);

        assertEquals(TaskState.ACCEPTED, task.state());
        assertEquals(progress, task.progress());
        assertFalse(task.result().isDone());

        task.apply(new TaskEvent.Progress<>(2, new TaskProgress("stale")));
        task.apply(new TaskEvent.Progress<>(3, new TaskProgress("duplicate")));
        assertNull(notified.get());
        assertEquals("moving", task.progress().stage());
        assertFalse(task.result().isDone());

        task.apply(new TaskEvent.Progress<>(4, new TaskProgress("fresh")));
        assertEquals("fresh", task.progress().stage());
        assertEquals(task.progress(), notified.get());

        task.apply(new TaskEvent.Completed<>(5, "done"));
        assertEquals(TaskState.COMPLETED, task.state());
        assertEquals("done", task.result().join());
    }

    @Test
    void restoresCompletedStateWithAnImmediatelyCompletedFuture() {
        var task = new StringTask();
        var progress = new TaskProgress("finished");

        task.restore(TaskState.COMPLETED, 5, progress, "done", null);

        assertEquals(TaskState.COMPLETED, task.state());
        assertEquals(progress, task.progress());
        assertTrue(task.result().isDone());
        assertFalse(task.result().isCompletedExceptionally());
        assertEquals("done", task.result().join());

        var seen = new AtomicReference<String>();
        task.result().thenAccept(seen::set);
        assertEquals("done", seen.get());
    }

    @Test
    void restoresFailedStateExceptionally() {
        var task = new StringTask();
        var error = new TaskError("boom", false, true);

        task.restore(TaskState.FAILED, 4, null, null, error);

        assertEquals(TaskState.FAILED, task.state());
        assertEquals(TaskProgress.NONE, task.progress());
        assertTrue(task.result().isCompletedExceptionally());

        var failure = assertThrows(CompletionException.class, task.result()::join);
        var exception = assertInstanceOf(TaskFailedException.class, failure.getCause());
        assertSame(error, exception.error());
        assertEquals("boom", exception.getMessage());

        task.apply(new TaskEvent.Completed<>(5, "late"));
        assertEquals(TaskState.FAILED, task.state());
    }

    @Test
    void restoresCancelledState() {
        var task = new StringTask();

        task.restore(TaskState.CANCELLED, 2, null, null, null);

        assertEquals(TaskState.CANCELLED, task.state());
        assertEquals(TaskProgress.NONE, task.progress());
        assertTrue(task.result().isCompletedExceptionally());
        var failure = assertThrows(CompletionException.class, task.result()::join);
        assertInstanceOf(CancellationException.class, failure.getCause());
    }
}
