package ru.astrosoup.mineberries.messaging.common.codec;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.TaskError;
import ru.astrosoup.mineberries.messaging.common.TaskEvent;
import ru.astrosoup.mineberries.messaging.common.TaskProgress;
import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Converts tasks to broker messages, reconstructs tasks from stored payloads,
 * and decodes broker events.
 *
 * <p>Encoding uses the registered {@link TaskMeta}; decoding resolves the
 * result class through the {@link ResultMetaRegistry}.
 */
public final class TaskCodec {
    private static final String TASK_KIND = "task";
    private static final int SCHEMA_VERSION = 1;

    private final TaskMetaRegistry registry;
    private final ResultMetaRegistry results;
    private final ObjectMapper mapper;

    public TaskCodec(TaskMetaRegistry registry, ResultMetaRegistry results, ObjectMapper mapper) {
        this.registry = registry;
        this.results = results;
        this.mapper = mapper;
    }

    public boolean supports(Class<?> taskClass) { return registry.find(taskClass).isPresent(); }

    public MessageDto encode(AbstractTask<?> task) {
        TaskMeta meta = registry.require(task.getClass());
        var payload = new LinkedHashMap<String, Object>();
        for (var p : meta.params()) {
            Object v = p.get(task);
            payload.put(p.name(), v == null ? null : mapper.convertValue(v, Object.class));
        }
        return new MessageDto(task.id(), TASK_KIND, meta.type(), SCHEMA_VERSION, task.expiresAt(), payload);
    }

    /**
     * Reconstructs a task with its original ID and payload properties.
     * This does not restore lifecycle state or metadata outside the payload.
     *
     * @throws IllegalArgumentException if the type is unknown, the ID is missing,
     *                                  or the payload is missing or invalid
     */
    public AbstractTask<?> decodeTask(String type, UUID id, Map<String, Object> payload) {
        return registry.requireType(type).create(id, payload, mapper);
    }

    public TaskEvent<Object> decode(Class<?> taskClass, EventDto dto) {
        return decodeEvent(registry.require(taskClass), dto);
    }

    /** Decodes an event using the task type stored in the database. */
    public TaskEvent<Object> decodeEvent(String taskType, EventDto dto) {
        return decodeEvent(registry.requireType(taskType), dto);
    }

    private TaskEvent<Object> decodeEvent(TaskMeta meta, EventDto dto) {
        long version = dto.version();
        Map<String, Object> p = dto.payload() == null ? Map.of() : dto.payload();

        return switch (dto.type()) {
            case ACCEPTED  -> new TaskEvent.Accepted<>(version, (String) p.get("bot"));
            case CANCELED -> new TaskEvent.Cancelled<>(version);
            case PROGRESS  -> new TaskEvent.Progress<>(version, new TaskProgress((String) p.get("stage")));
            case FAILED    -> new TaskEvent.Failed<>(version, new TaskError(
                    (String) p.get("message"),
                    Boolean.TRUE.equals(p.get("retryable")),
                    Boolean.TRUE.equals(p.get("outcomeUnknown"))));
            case COMPLETED -> new TaskEvent.Completed<>(version, decodeResult(meta, dto.payload()));
        };
    }

    private Object decodeResult(TaskMeta meta, Map<String, Object> payload) {
        if (meta.resultClass() == Void.class) return null;
        if (payload == null)
            throw new IllegalStateException("COMPLETED without payload for " + meta.type());
        return results.require(meta.resultClass()).create(payload, mapper);
    }

    public static ObjectMapper defaultMapper() {
        return JsonMapper.builder()
            .findAndAddModules()
            .build();
    }
}
