package ru.astrosoup.mineberries.messaging.common;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;
import ru.astrosoup.mineberries.messaging.common.exceptions.TaskFailedException;

/**
 * Base class for a task sent to a bot.
 *
 * <p>Each task has a unique id, a lifecycle state, progress listeners, and a
 * future that completes with the decoded result. Events are applied in version
 * order; terminal states are final.
 *
 * @param <R> result type produced by the task
 */
@AllArgsConstructor @Accessors(fluent = true)
public abstract class AbstractTask<R> {

    private final @Getter UUID id;
    private final AtomicReference<TaskState> state = new AtomicReference<>(TaskState.NEW);
    private final List<Consumer<TaskProgress>> progressListeners = new CopyOnWriteArrayList<>();
    private volatile @Getter TaskProgress progress = TaskProgress.NONE;
    private final CompletableFuture<R> result = new CompletableFuture<>();

    private long version = -1;
    
    // Constructors
    protected AbstractTask(UUID id) { 
        this.id = id; 
    }
    
    protected AbstractTask() { 
        this(UUID.randomUUID()); 
    }
    // Custom getters
    public TaskState state() {
        return state.get();
    }
    public CompletableFuture<R> result() {
        return result.copy();
    }

    /**
     * Optional absolute deadline for this task.
     *
     * <p>Tasks without a deadline inherit the default {@code null}. A task with
     * a deadline overrides this method and returns a stable instant.
     */
    public Instant expiresAt() {
        return null;
    }

    // public methods
    public void onProgress(Consumer<TaskProgress> listener) {
        progressListeners.add(listener);
    }

    // non-public methods
    synchronized void apply(TaskEvent<R> event) {
        if (this.state().isTerminal() || event.version() <= this.version) {
            return; // duplicate
        }
        
        switch(event) {
            case TaskEvent.Accepted<R> a -> state.set(TaskState.ACCEPTED);
            case TaskEvent.Progress<R> p -> {
                progress = p.progress();
                progressListeners.forEach(listener -> {
                    listener.accept(p.progress());
                });
            }
            case TaskEvent.Completed<R> c -> {
                state.set(TaskState.COMPLETED);
                result.complete(c.result());
            }
            case TaskEvent.Failed<R> f -> {
                state.set(TaskState.FAILED);
                result.completeExceptionally(new TaskFailedException(f.error()));
            }
            case TaskEvent.Cancelled<R> c -> {
                state.set(TaskState.CANCELLED);
                result.cancel(false);
            }
            default -> {}
        }
        version = event.version();
    }
    void failLocally(Throwable t) { result.completeExceptionally(t); }
}