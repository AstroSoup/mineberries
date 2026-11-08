package ru.astrosoup.mineberries.messaging.common.codec;

import java.util.LinkedHashMap;
import java.util.Map;

import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.TaskError;
import ru.astrosoup.mineberries.messaging.common.TaskEvent;
import ru.astrosoup.mineberries.messaging.common.TaskProgress;
import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

public final class TaskCodec {
    private final TaskMetaRegistry registry;
    private final ObjectMapper mapper;

    public TaskCodec(TaskMetaRegistry registry, ObjectMapper mapper) {
        this.registry = registry;
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
        return new MessageDto(task.id(), meta.type(), payload);
    }

    public TaskEvent<Object> decode(Class<?> taskClass, EventDto dto) {
        TaskMeta meta = registry.require(taskClass);
        long version = dto.version();
        Map<String, Object> p = dto.payload() == null ? Map.of() : dto.payload();

        return switch (dto.type()) {
            case ACCEPTED  -> new TaskEvent.Accepted<>(version);
            case CANCELED -> new TaskEvent.Cancelled<>(version);
            case PROGRESS  -> new TaskEvent.Progress<>(version, new TaskProgress((String) p.get("stage")));
            case FAILED    -> new TaskEvent.Failed<>(version, new TaskError((String) p.get("message"), Boolean.TRUE.equals(p.get("retryable"))));
            case COMPLETED -> new TaskEvent.Completed<>(version, decodeResult(meta, dto.payload()));
        };
    }

    private Object decodeResult(TaskMeta meta, Map<String, Object> payload) {
        if (meta.resultClass() == Void.class) return null;
        if (payload == null)
            throw new IllegalStateException("COMPLETED without payload for " + meta.type());
        return mapper.convertValue(payload, meta.resultClass());
    }

    public static ObjectMapper defaultMapper() {
        return JsonMapper.builder()
            .findAndAddModules()
            .build();
    }
}