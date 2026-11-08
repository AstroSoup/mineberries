package ru.astrosoup.mineberries.messaging.common.brocker;

import java.util.function.Consumer;

import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;

public interface EventSource   { void subscribe(Consumer<EventDto> handler); }