package ru.astrosoup.mineberries.messaging.tasks;

import java.time.Instant;
import java.util.UUID;

import ru.astrosoup.mineberries.messaging.common.TaskState;

/**
 * Immutable snapshot of a row in the {@code task} table.
 *
 * @param taskId            task id
 * @param type              task type identifier
 * @param payload           task payload as raw JSON text
 * @param state             current lifecycle state
 * @param lastEventVersion  highest applied event version, or {@code -1} before any event
 * @param progressStage     latest progress stage, or {@code null}
 * @param assignedBot       bot that accepted the task, or {@code null}
 * @param result            completed result as raw JSON text, or {@code null}
 * @param error             failure details as raw JSON text, or {@code null}
 * @param expiresAt         optional task deadline, or {@code null}
 * @param acceptBy          optional deadline for the bot to accept the task, or {@code null}
 * @param finishBy          optional deadline for the task to finish, or {@code null}
 * @param cancelRequestedAt time cancellation was requested, or {@code null}
 * @param processedAt       time the continuation finished, or {@code null}
 * @param createdAt         creation time
 * @param updatedAt         last update time
 */
public record TaskRecord(
        UUID taskId,
        String type,
        String payload,
        TaskState state,
        long lastEventVersion,
        String progressStage,
        String assignedBot,
        String result,
        String error,
        Instant expiresAt,
        Instant acceptBy,
        Instant finishBy,
        Instant cancelRequestedAt,
        Instant processedAt,
        Instant createdAt,
        Instant updatedAt) {
}
