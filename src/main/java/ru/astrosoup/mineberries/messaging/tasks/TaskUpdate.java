package ru.astrosoup.mineberries.messaging.tasks;

import ru.astrosoup.mineberries.messaging.common.TaskState;

/**
 * State changes applied after a task event.
 *
 * <p>The repository writes every field, so callers must pass the previous values
 * for fields that the event does not change. JSON fields are raw JSON text.
 *
 * @param state            new lifecycle state
 * @param lastEventVersion event version that produced this update
 * @param progressStage    latest progress stage, or {@code null}
 * @param assignedBot      bot that accepted the task, or {@code null}
 * @param result           completed result as raw JSON text, or {@code null}
 * @param error            failure details as raw JSON text, or {@code null}
 */
public record TaskUpdate(
        TaskState state,
        long lastEventVersion,
        String progressStage,
        String assignedBot,
        String result,
        String error) {
}
