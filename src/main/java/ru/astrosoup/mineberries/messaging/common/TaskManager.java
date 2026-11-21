package ru.astrosoup.mineberries.messaging.common;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import ru.astrosoup.mineberries.messaging.common.broker.EventSource;
import ru.astrosoup.mineberries.messaging.common.broker.MessageSender;
import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;
import ru.astrosoup.mineberries.messaging.tasks.NewTask;
import ru.astrosoup.mineberries.messaging.tasks.TaskRecord;
import ru.astrosoup.mineberries.messaging.tasks.TaskRepository;
import ru.astrosoup.mineberries.messaging.tasks.TaskUpdate;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Submits tasks to the bot and tracks their lifecycle.
 *
 * <p>Submitted tasks are kept until a terminal event arrives. Events are
 * matched to tasks by message id and applied in version order; late or
 * duplicate events are ignored.
 */
@Slf4j
public class TaskManager {
    private static final int CANCEL_SCHEMA_VERSION = 1;

    private final MessageSender sender;
    private final TaskCodec codec;
    private final Persistence persistence;
    private final Map<UUID, AbstractTask<?>> active = new ConcurrentHashMap<>();
    private final Set<UUID> localCancelRequests = ConcurrentHashMap.newKeySet();
    private final TaskContinuations continuations;

    /** Creates a non-persistent manager for in-memory tests. */
    public TaskManager(MessageSender sender, EventSource events, TaskCodec codec) {
        this(sender, events, codec, null, new TaskContinuations(codec));
    }

    public TaskManager(MessageSender sender, EventSource events, TaskCodec codec,
                       TaskRepository tasks, ObjectMapper mapper, PlatformTransactionManager transactionManager) {
        this(sender, events, codec, tasks, mapper, transactionManager, Duration.ofMinutes(5));
    }

    TaskManager(MessageSender sender, EventSource events, TaskCodec codec,
                TaskRepository tasks, ObjectMapper mapper, PlatformTransactionManager transactionManager,
                Duration continuationGrace) {
        this(sender, events, codec, new Persistence(tasks, mapper, mandatoryTransaction(transactionManager),
                new TransactionTemplate(transactionManager)),
                new TaskContinuations(codec, tasks, mapper, transactionManager, continuationGrace));
    }

    private TaskManager(MessageSender sender, EventSource events, TaskCodec codec, Persistence persistence,
                        TaskContinuations continuations) {
        this.sender = sender;
        this.codec = codec;
        this.persistence = persistence;
        this.continuations = continuations;
        if (persistence != null) rehydrate();
        events.subscribe(this::onEvent);
    }

    private void rehydrate() {
        var rehydrator = new TaskRehydrator(codec, persistence.mapper());
        for (var row : persistence.tasks().listActive()) {
            rehydrateRow(rehydrator, row, this::activate);
        }
        for (var row : persistence.tasks().listTerminalUnprocessed()) {
            rehydrateRow(rehydrator, row, continuations::retainTerminal);
        }
    }

    private void rehydrateRow(TaskRehydrator rehydrator, TaskRecord row, Consumer<AbstractTask<?>> store) {
        AbstractTask<?> task;
        try {
            task = rehydrator.restore(row);
        } catch (RuntimeException e) {
            // Decoder exceptions can include raw stored values.
            log.error("Could not rehydrate task {}", row.taskId());
            return;
        }
        store.accept(task);
    }

    List<AbstractTask<?>> activeTasks() {
        return List.copyOf(active.values());
    }

    List<AbstractTask<?>> terminalUnprocessedTasks() {
        return continuations.terminalTasks();
    }

    /**
     * Registers the single owning module's continuation for this exact task class.
     *
     * <p>For an active task the continuation is invoked to attach observers such as
     * progress or result listeners; it must not block waiting for that task to finish.
     * It is invoked again for terminal handling with an already settled result future.
     * This also applies to tasks submitted after registration and to unprocessed
     * terminal tasks recovered at startup.
     *
     * <p>Terminal continuations run at least once and must be idempotent. Return
     * only when processing is finished, and propagate any failure out of this
     * callback (e.g. join a dependent future so its failure is not hidden).
     * Successful terminal handling sets {@code processed_at}; failures leave it
     * unset so the continuation can run again on restart.
     *
     * @throws IllegalStateException if this task class already has an owner
     */
    public <T extends AbstractTask<?>> void reattach(Class<T> taskClass, Consumer<T> continuation) {
        continuations.register(taskClass, continuation, this::activeTasks);
    }

    @Scheduled(fixedDelayString = "${mineberries.messaging.continuation-scan-interval:1m}")
    void processUnownedTerminalTasks() {
        continuations.markExpiredUnownedTasks();
    }

    /**
     * Persists and enqueues a new task in the caller's transaction. The task starts
     * receiving events only after commit. Write failures throw from this method
     * and mark the transaction for rollback, rather than settling the task's result.
     */
    public <T extends AbstractTask<?>> T submit(T task) {
        if (!codec.supports(task.getClass()))
            throw new IllegalArgumentException("Unregistered task class: " + task.getClass().getName());

        MessageDto dto = codec.encode(task);
        if (persistence != null) {
            persistence.submissionTransaction().executeWithoutResult(status -> persistAndEnqueue(task, dto));
        } else {
            reserveActive(task);
            try {
                awaitEnqueue(sender.send(dto));
            } catch (RuntimeException | Error e) {
                active.remove(task.id(), task);
                throw e;
            }
            removeWhenFinished(task);
            continuations.attachActive(task);
        }
        return task;
    }

    /**
     * Requests cancellation and returns the task's result future. For a durable
     * task the request and outbox message are written in one transaction, joining
     * the caller's transaction if present. Repeated requests, terminal tasks, and
     * tasks that have not been submitted do not enqueue a message.
     *
     * <p>Only a terminal event from the bot settles the result; requesting
     * cancellation does not cancel the future or change the task state.
     */
    public <R> CompletableFuture<R> cancel(AbstractTask<R> task) {
        if (persistence != null) {
            persistence.taskTransaction().executeWithoutResult(status -> {
                if (persistence.tasks().requestCancellation(task.id())) enqueueCancel(task.id());
            });
        } else {
            requestLocalCancellation(task.id());
        }
        return task.result();
    }

    private void requestLocalCancellation(UUID id) {
        var task = active.get(id);
        if (task == null) return;
        synchronized (task) {
            if (task.state().isTerminal() || !localCancelRequests.add(id)) return;
            try {
                enqueueCancel(id);
            } catch (RuntimeException | Error e) {
                localCancelRequests.remove(id);
                throw e;
            }
        }
    }

    private void enqueueCancel(UUID id) {
        awaitEnqueue(sender.sendCancel(id, CANCEL_SCHEMA_VERSION));
    }

    private void persistAndEnqueue(AbstractTask<?> task, MessageDto dto) {
        if (active.containsKey(task.id())) throw duplicate(task.id());
        String payload;
        try {
            payload = persistence.mapper().writeValueAsString(dto.payload());
        } catch (JacksonException e) {
            throw new IllegalArgumentException("Could not serialize task payload");
        }
        try {
            persistence.tasks().insert(new NewTask(task.id(), dto.type(), payload,
                    dto.expiresAt(), task.acceptBy(), task.finishBy()));
        } catch (DuplicateKeyException e) {
            throw duplicate(task.id());
        } catch (DataAccessException e) {
            // Database errors can contain the payload.
            throw new IllegalStateException("Could not store task " + task.id());
        }
        awaitEnqueue(sender.send(dto));
        afterCommit(() -> activate(task));
    }

    private static void afterCommit(Runnable action) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            private final HashSet<Object> laterSavepoints = new HashSet<>();
            private boolean rolledBack;

            @Override
            public void savepoint(Object savepoint) {
                laterSavepoints.add(savepoint);
            }

            @Override
            public void savepointRollback(Object savepoint) {
                // A rollback to a savepoint created before registration undoes the writes.
                if (!laterSavepoints.contains(savepoint)) rolledBack = true;
            }

            @Override
            public void afterCommit() {
                if (!rolledBack) action.run();
            }
        });
    }
    
    private static void awaitEnqueue(CompletableFuture<Void> accepted) {
        try {
            accepted.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException cause) throw cause;
            throw e;
        }
    }

    private void activate(AbstractTask<?> task) {
        reserveActive(task);
        removeWhenFinished(task);
        continuations.attachActive(task);
    }

    private void reserveActive(AbstractTask<?> task) {
        if (active.putIfAbsent(task.id(), task) != null)
            throw duplicate(task.id());
    }

    private void removeWhenFinished(AbstractTask<?> task) {
        task.result().whenComplete((r, ex) -> {
            if (active.remove(task.id(), task) && persistence == null) {
                localCancelRequests.remove(task.id());
            }
            continuations.detach(task);
        });
    }

    private static IllegalStateException duplicate(UUID id) {
        return new IllegalStateException("Task already submitted: " + id);
    }

    private static TransactionTemplate mandatoryTransaction(PlatformTransactionManager transactionManager) {
        var transaction = new TransactionTemplate(transactionManager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_MANDATORY);
        return transaction;
    }

    private record Persistence(TaskRepository tasks, ObjectMapper mapper, TransactionTemplate submissionTransaction,
                                TransactionTemplate taskTransaction) {}

    private void onEvent(EventDto dto) {
        if (persistence != null) {
            persistence.taskTransaction().executeWithoutResult(status -> persistEvent(dto));
            return;
        }

        var task = activeTask(dto.messageId());
        if (task == null) {
            log.debug("Event for unknown/finished task {}: {}", dto.messageId(), dto.type());
            return;
        }
        try {
            task.apply(codec.decode(task.getClass(), dto));
        } catch (RuntimeException e) {
            log.warn("Can't process event {} for task {}", dto.type(), dto.messageId());
            task.failLocally(e);
        }
        if (task.state().isTerminal()) continuations.terminal(task);
    }

    private void persistEvent(EventDto dto) {
        var task = persistence.tasks().findByIdForUpdate(dto.messageId()).orElse(null);
        if (task == null) {
            log.debug("Event for unknown task {}: {}", dto.messageId(), dto.type());
            return;
        }
        if (task.state().isTerminal() || dto.version() <= task.lastEventVersion()) return;

        TaskEvent<Object> event;
        try {
            event = codec.decodeEvent(task.type(), dto);
        } catch (RuntimeException e) {
            // Preserve local failure semantics without changing durable task state.
            log.warn("Can't decode event {} for task {}", dto.type(), task.taskId());
            afterCommit(() -> failActiveTask(task.taskId(), "Could not decode task event"));
            return;
        }

        var update = eventUpdate(task, event, dto);
        if (!persistence.tasks().updateAfterEvent(task.taskId(), update)) {
            throw new IllegalStateException("Could not update task " + task.taskId());
        }
        afterCommit(() -> applyCommittedEvent(task, event, update.state().isTerminal()));
    }

    private TaskUpdate eventUpdate(TaskRecord row, TaskEvent<Object> event, EventDto dto) {
        var state = row.state();
        String progress = row.progressStage();
        String bot = row.assignedBot();
        String result = row.result();
        String error = row.error();
        switch (event) {
            case TaskEvent.Accepted<Object> accepted -> {
                state = TaskState.ACCEPTED;
                bot = accepted.bot();
            }
            case TaskEvent.Progress<Object> reporting -> progress = reporting.progress().stage();
            case TaskEvent.Completed<Object> completed -> {
                state = TaskState.COMPLETED;
                // Keep the result's wire property names for later reconstruction.
                result = completed.result() == null ? null : eventJson(dto.payload());
            }
            case TaskEvent.Failed<Object> failed -> {
                state = TaskState.FAILED;
                error = eventJson(failed.error());
            }
            case TaskEvent.Cancelled<Object> ignored -> state = TaskState.CANCELLED;
            default -> throw new IllegalStateException("Unsupported task event");
        }
        return new TaskUpdate(state, event.version(), progress, bot, result, error);
    }

    private String eventJson(Object payload) {
        try {
            return persistence.mapper().writeValueAsString(payload);
        } catch (JacksonException e) {
            throw new IllegalStateException("Could not serialize task event data");
        }
    }

    private void applyCommittedEvent(TaskRecord prior, TaskEvent<Object> event, boolean terminal) {
        var task = activeTask(prior.taskId());
        if (task == null) {
            if (terminal) continuations.storedTerminal(prior.taskId());
            return;
        }
        try {
            task.applyCommitted(event, prior.state(), new TaskProgress(prior.progressStage()));
        } catch (RuntimeException e) {
            log.warn("Can't notify listeners for task {} at version {}", prior.taskId(), event.version());
            task.failLocally(new IllegalStateException("Could not notify task listeners"));
        }
        if (terminal) continuations.terminal(task);
    }

    private void failActiveTask(UUID id, String message) {
        var task = activeTask(id);
        if (task != null) task.failLocally(new IllegalStateException(message));
    }

    @SuppressWarnings("unchecked")
    private AbstractTask<Object> activeTask(UUID id) {
        return (AbstractTask<Object>) active.get(id);
    }
}
