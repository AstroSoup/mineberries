package ru.astrosoup.mineberries.messaging.common;

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
* 
* AbstractTask - parent class for any task sent to a bot.
**/
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