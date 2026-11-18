package ru.astrosoup.mineberries.messaging.common;

/**
 * Failure reported by a task.
 *
 * @param message   human-readable failure description
 * @param retryable whether the task may be submitted again
 */
public record TaskError(String message, boolean retryable) {}