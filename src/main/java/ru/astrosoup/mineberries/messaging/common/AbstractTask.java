package ru.astrosoup.mineberries.messaging.common;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
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
 * <p>Concrete classes annotated with
 * {@link ru.astrosoup.mineberries.messaging.common.annotations.Task @Task}
 * must provide a public constructor that takes an existing {@link UUID} first,
 * followed by the {@code @Property} values in metadata order. It must pass
 * the supplied id to {@link #AbstractTask(UUID)}. Constructors for new tasks
 * can delegate to this constructor with a randomly generated id.
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

    /** Optional local deadline for the bot to accept this task. */
    public Instant acceptBy() {
        return null;
    }

    /** Optional local deadline for the bot to finish this task. */
    public Instant finishBy() {
        return null;
    }

    // public methods
    public void onProgress(Consumer<TaskProgress> listener) {
        progressListeners.add(listener);
    }

    // non-public methods
    synchronized void apply(TaskEvent<R> event) {
        if (canApply(event)) applyNewEvent(event);
    }

    /** Applies an event with its persisted predecessor, even if earlier commit callbacks lag behind. */
    synchronized void applyCommitted(TaskEvent<R> event, TaskState priorState, TaskProgress priorProgress) {
        if (!canApply(event)) return;
        state.set(priorState);
        progress = priorProgress;
        applyNewEvent(event);
    }

    private boolean canApply(TaskEvent<R> event) {
        return !state().isTerminal() && event.version() > version;
    }

    private void applyNewEvent(TaskEvent<R> event) {
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

    /**
     * Initializes a freshly reconstructed task without replaying persisted events.
     *
     * <p>Terminal states complete the result future immediately. A restored
     * non-terminal task continues to accept newer event versions and ignores
     * older or duplicate ones through {@link #apply(TaskEvent)}.
     *
     * @param state    persisted lifecycle state
     * @param version  last applied event version
     * @param progress persisted progress, or {@code null} for no progress
     * @param result   persisted result for {@code COMPLETED}, may be {@code null}
     * @param error    persisted error for {@code FAILED}
     */
    synchronized void restore(TaskState state, long version, TaskProgress progress,
                              R result, TaskError error) {
        Objects.requireNonNull(state, "state");
        if (state == TaskState.FAILED) Objects.requireNonNull(error, "A failed task requires an error");
        this.version = version;
        this.progress = progress == null ? TaskProgress.NONE : progress;
        this.state.set(state);

        switch (state) {
            case COMPLETED -> this.result.complete(result);
            case FAILED -> this.result.completeExceptionally(new TaskFailedException(error));
            case CANCELLED -> this.result.cancel(false);
            default -> { }
        }
    }

    void failLocally(Throwable t) { result.completeExceptionally(t); }
}
