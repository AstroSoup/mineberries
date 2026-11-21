package ru.astrosoup.mineberries.messaging.common;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.tasks.TaskRepository;
import tools.jackson.databind.ObjectMapper;

/** Tracks ownership and terminal work separately from the live task futures. */
@Slf4j
final class TaskContinuations {
    private final TaskCodec codec;
    private final Storage storage;
    private final Map<String, Consumer<AbstractTask<?>>> owners = new ConcurrentHashMap<>();
    private final Map<UUID, AbstractTask<?>> pending = new ConcurrentHashMap<>();
    private final Set<UUID> attached = ConcurrentHashMap.newKeySet();
    private final Set<UUID> processing = ConcurrentHashMap.newKeySet();

    TaskContinuations(TaskCodec codec) {
        this.codec = codec;
        storage = null;
    }

    TaskContinuations(TaskCodec codec, TaskRepository tasks, ObjectMapper mapper,
                      PlatformTransactionManager transactionManager, Duration grace) {
        if (grace.isNegative()) throw new IllegalArgumentException("Continuation grace period must not be negative");
        this.codec = codec;
        var transaction = new TransactionTemplate(transactionManager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        storage = new Storage(tasks, transaction, new TaskRehydrator(codec, mapper), grace, Instant.now());
    }

    <T extends AbstractTask<?>> void register(Class<T> taskClass, Consumer<T> continuation,
                                              Supplier<List<AbstractTask<?>>> active) {
        Objects.requireNonNull(continuation, "continuation");
        var type = codec.taskType(taskClass);
        synchronized (owners) {
            if (owners.putIfAbsent(type, task -> continuation.accept(taskClass.cast(task))) != null) {
                throw new IllegalStateException("A continuation is already registered for " + type);
            }
        }
        active.get().stream().filter(task -> task.getClass() == taskClass).forEach(this::attachActive);
        terminalTasks().stream().filter(task -> task.getClass() == taskClass).forEach(this::processTerminal);
    }

    void attachActive(AbstractTask<?> task) {
        var owner = ownerOf(task);
        if (owner == null) return;
        synchronized (task) {
            if (task.state().isTerminal() || task.result().isDone() || !attached.add(task.id())) return;
            try {
                owner.accept(task);
            } catch (RuntimeException e) {
                log.error("Could not attach continuation for task {}", task.id());
            }
        }
    }

    void detach(AbstractTask<?> task) {
        attached.remove(task.id());
    }

    void retainTerminal(AbstractTask<?> task) {
        pending.putIfAbsent(task.id(), task);
    }

    List<AbstractTask<?>> terminalTasks() {
        return List.copyOf(pending.values());
    }

    void terminal(AbstractTask<?> task) {
        retainTerminal(task);
        processTerminal(pending.getOrDefault(task.id(), task));
    }

    void storedTerminal(UUID id) {
        // An event can complete a durable task even if this process has no corresponding live instance.
        AbstractTask<?> task;
        try {
            task = storage.transaction().execute(status -> storage.tasks().findByIdForUpdate(id)
                    .filter(row -> row.state().isTerminal() && row.processedAt() == null)
                    .map(storage.rehydrator()::restore).orElse(null));
        } catch (RuntimeException e) {
            log.error("Could not load terminal task {} for continuation", id);
            return;
        }
        if (task != null) terminal(task);
    }

    private void processTerminal(AbstractTask<?> task) {
        var owner = ownerOf(task);
        if (owner == null || !processing.add(task.id())) return;
        try {
            if (pending.get(task.id()) != task) return;
            if (storage == null) {
                owner.accept(task);
            } else {
                storage.transaction().executeWithoutResult(status -> {
                    var row = storage.tasks().findByIdForUpdate(task.id()).orElse(null);
                    if (row == null || !row.state().isTerminal() || row.processedAt() != null) return;
                    owner.accept(task);
                    if (status.isRollbackOnly() || !storage.tasks().markProcessed(task.id())) {
                        throw new IllegalStateException("Could not mark task processed");
                    }
                });
            }
            pending.remove(task.id(), task);
        } catch (RuntimeException e) {
            // Continuation failures can contain task data.
            log.error("Continuation failed for task {}; left unprocessed", task.id());
        } finally {
            processing.remove(task.id());
        }
    }

    void markExpiredUnownedTasks() {
        if (storage == null) return;
        var cutoff = Instant.now().minus(storage.grace());
        if (cutoff.isBefore(storage.startedAt())) return;
        for (var row : storage.tasks().listTerminalUnprocessed()) {
            if (row.updatedAt().isAfter(cutoff)) continue;
            // Registration and discarding a task with no owner must not race.
            synchronized (owners) {
                if (owners.containsKey(row.type())) continue;
                var marked = storage.transaction().execute(status -> storage.tasks().markProcessed(row.taskId()));
                if (Boolean.TRUE.equals(marked)) {
                    pending.remove(row.taskId());
                    log.warn("Marked task {} processed without a registered continuation", row.taskId());
                }
            }
        }
    }

    private Consumer<AbstractTask<?>> ownerOf(AbstractTask<?> task) {
        return owners.isEmpty() ? null : owners.get(codec.taskType(task.getClass()));
    }

    private record Storage(TaskRepository tasks, TransactionTemplate transaction, TaskRehydrator rehydrator,
                           Duration grace, Instant startedAt) {}
}
