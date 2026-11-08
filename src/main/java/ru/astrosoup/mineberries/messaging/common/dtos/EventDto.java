package ru.astrosoup.mineberries.messaging.common.dtos;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import ru.astrosoup.mineberries.messaging.common.EventType;

public record EventDto(UUID messageId, long version, EventType type, Instant occurredAt, Map<String, Object> payload) {}