package ru.astrosoup.mineberries.messaging.common;

import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
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
import ru.astrosoup.mineberries.messaging.tasks.TaskRepository;
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

    private final MessageSender sender;
    private final TaskCodec codec;
    private final Persistence persistence;
    private final Map<UUID, AbstractTask<?>> active = new ConcurrentHashMap<>();

    /** Creates a non-persistent manager for in-memory tests. */
    public TaskManager(MessageSender sender, EventSource events, TaskCodec codec) {
        this(sender, events, codec, null);
    }

    public TaskManager(MessageSender sender, EventSource events, TaskCodec codec,
                       TaskRepository tasks, ObjectMapper mapper, PlatformTransactionManager transactionManager) {
        this(sender, events, codec, new Persistence(tasks, mapper, mandatoryTransaction(transactionManager)));
    }

    private TaskManager(MessageSender sender, EventSource events, TaskCodec codec, Persistence persistence) {
        this.sender = sender;
        this.codec = codec;
        this.persistence = persistence;
        events.subscribe(this::onEvent);
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
            persistence.transaction().executeWithoutResult(status -> persistAndEnqueue(task, dto));
        } else {
            reserveActive(task);
            try {
                enqueue(dto);
            } catch (RuntimeException | Error e) {
                active.remove(task.id(), task);
                throw e;
            }
            removeWhenFinished(task);
        }
        return task;
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
        enqueue(dto);
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            private final HashSet<Object> laterSavepoints = new HashSet<>();
            private boolean rolledBack;

            @Override
            public void savepoint(Object savepoint) {
                laterSavepoints.add(savepoint);
            }

            @Override
            public void savepointRollback(Object savepoint) {
                // A rollback to a savepoint created before this submission undoes both inserts.
                if (!laterSavepoints.contains(savepoint)) rolledBack = true;
            }

            @Override
            public void afterCommit() {
                if (!rolledBack) {
                    reserveActive(task);
                    removeWhenFinished(task);
                }
            }
        });
    }
    
    private void enqueue(MessageDto dto) {
        try {
            sender.send(dto).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException cause) throw cause;
            throw e;
        }
    }

    private void reserveActive(AbstractTask<?> task) {
        if (active.putIfAbsent(task.id(), task) != null)
            throw duplicate(task.id());
    }

    private void removeWhenFinished(AbstractTask<?> task) {
        task.result().whenComplete((r, ex) -> active.remove(task.id(), task));
    }

    private static IllegalStateException duplicate(UUID id) {
        return new IllegalStateException("Task already submitted: " + id);
    }

    private static TransactionTemplate mandatoryTransaction(PlatformTransactionManager transactionManager) {
        var transaction = new TransactionTemplate(transactionManager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_MANDATORY);
        return transaction;
    }

    private record Persistence(TaskRepository tasks, ObjectMapper mapper, TransactionTemplate transaction) {}

    @SuppressWarnings("unchecked")
    private void onEvent(EventDto dto) {
        AbstractTask<Object> task = (AbstractTask<Object>) active.get(dto.messageId());
        if (task == null) {
            log.debug("Event for unknown/finished task {}: {}", dto.messageId(), dto.type());
            return;
        }
        try {
            task.apply(codec.decode(task.getClass(), dto));
        } catch (RuntimeException e) {
            log.error("Can't process event {} for task {}", dto.type(), dto.messageId(), e);
            task.failLocally(e);
        }
    }
}
