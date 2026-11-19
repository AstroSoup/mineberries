package ru.astrosoup.mineberries.messaging.common.broker;

import java.util.function.Consumer;

import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;

/** Source of task lifecycle events delivered by the broker. */
public interface EventSource {
    /** Registers a handler that receives every incoming event. */
    void subscribe(Consumer<EventDto> handler);
}