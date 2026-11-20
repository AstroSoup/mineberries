package ru.astrosoup.mineberries.messaging.tasks;

import java.time.Instant;
import java.util.UUID;

/**
 * Data needed to insert a new task.
 *
 * <p>The repository inserts the task in {@code NEW} state with no result or error.
 * Only the task types that have deadlines set the expiry fields.
 *
 * @param taskId    task id
 * @param type      task type identifier
 * @param payload   task payload as raw JSON text
 * @param expiresAt optional task deadline, or {@code null}
 * @param acceptBy  optional deadline for the bot to accept the task, or {@code null}
 * @param finishBy  optional deadline for the task to finish, or {@code null}
 */
public record NewTask(
        UUID taskId,
        String type,
        String payload,
        Instant expiresAt,
        Instant acceptBy,
        Instant finishBy) {
}
