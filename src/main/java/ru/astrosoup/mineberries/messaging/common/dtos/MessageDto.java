package ru.astrosoup.mineberries.messaging.common.dtos;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Encoded message sent to a bot.
 *
 * @param messageId     id of the task
 * @param kind          message kind, either {@code task} or {@code cancel}
 * @param type          task type identifier, or {@code null} for a cancel message
 * @param schemaVersion version of the payload schema for this message
 * @param expiresAt     optional deadline, or {@code null} when the task has none
 * @param payload       property names mapped to their values, or {@code null} when
 *                      the message has no payload
 */
public record MessageDto(
        UUID messageId,
        String kind,
        String type,
        int schemaVersion,
        Instant expiresAt,
        Map<String, Object> payload) {
}
