package ru.astrosoup.mineberries.messaging.common.dtos;

import java.util.Map;
import java.util.UUID;

/**
 * Encoded task message sent to a bot.
 *
 * @param messageId id of the task
 * @param type      task type identifier
 * @param payload   property names mapped to their values
 */
public record MessageDto(UUID messageId, String type, Map<String, Object> payload) {}