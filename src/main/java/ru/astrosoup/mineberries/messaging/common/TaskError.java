package ru.astrosoup.mineberries.messaging.common;

/**
 * Failure reported by a task.
 *
 * @param message        human-readable failure description
 * @param retryable      whether the task may be submitted again
 * @param outcomeUnknown whether the bot cannot determine whether a side effect
 *                       already happened; such a task must not be retried
 *                       automatically
 */
public record TaskError(String message, boolean retryable, boolean outcomeUnknown) {}
