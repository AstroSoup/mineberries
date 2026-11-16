package ru.astrosoup.mineberries.messaging.support;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import ru.astrosoup.mineberries.messaging.common.EventType;
import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;

/** Shorthand builders for the events a worker would emit. */
public final class Events {
    private Events() {}

    public static EventDto accepted(UUID id, long version) { return of(id, version, EventType.ACCEPTED, null); }
    public static EventDto canceled(UUID id, long version) { return of(id, version, EventType.CANCELED, null); }
    public static EventDto progress(UUID id, long version, String stage) {
        return of(id, version, EventType.PROGRESS, Map.of("stage", stage));
    }
    public static EventDto completed(UUID id, long version, Map<String, Object> payload) {
        return of(id, version, EventType.COMPLETED, payload);
    }
    public static EventDto failed(UUID id, long version, String message, boolean retryable) {
        return of(id, version, EventType.FAILED, Map.of("message", message, "retryable", retryable));
    }
    public static EventDto of(UUID id, long version, EventType type, Map<String, Object> payload) {
        return new EventDto(id, version, type, Instant.now(), payload);
    }
}
