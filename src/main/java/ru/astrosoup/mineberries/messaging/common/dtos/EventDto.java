package ru.astrosoup.mineberries.messaging.common.dtos;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import ru.astrosoup.mineberries.messaging.common.EventType;

/**
 * Event emitted by a bot for a submitted task.
 *
 * @param messageId  id of the task the event belongs to
 * @param version    monotonically increasing event version
 * @param type       event type
 * @param occurredAt time the event was produced
 * @param payload    event-specific data, or {@code null} when there is none
 */
public record EventDto(UUID messageId, long version, EventType type, Instant occurredAt, Map<String, Object> payload) {}