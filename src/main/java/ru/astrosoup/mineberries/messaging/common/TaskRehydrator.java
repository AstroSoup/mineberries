package ru.astrosoup.mineberries.messaging.common;

import java.util.Map;

import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.tasks.TaskRecord;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;

/** Rebuilds the task and its result from a stored snapshot without replaying events. */
final class TaskRehydrator {
    private final TaskCodec codec;
    private final ObjectReader payloadReader;
    private final ObjectReader errorReader;

    TaskRehydrator(TaskCodec codec, ObjectMapper mapper) {
        this.codec = codec;
        payloadReader = mapper.readerFor(new TypeReference<Map<String, Object>>() {})
                .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        errorReader = mapper.readerFor(TaskError.class).with(
                DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
                DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES);
    }

    AbstractTask<?> restore(TaskRecord row) {
        // The result decoder uses the same registered task type as the task decoder.
        @SuppressWarnings("unchecked")
        var task = (AbstractTask<Object>) codec.decodeTask(row.type(), row.taskId(), payloadReader.readValue(row.payload()));
        Object result = row.state() == TaskState.COMPLETED
                ? codec.decodeResult(row.type(), row.result() == null ? null : payloadReader.readValue(row.result()))
                : null;
        TaskError error = row.state() == TaskState.FAILED ? errorReader.readValue(row.error()) : null;
        task.restore(row.state(), row.lastEventVersion(), new TaskProgress(row.progressStage()), result, error);
        return task;
    }
}
