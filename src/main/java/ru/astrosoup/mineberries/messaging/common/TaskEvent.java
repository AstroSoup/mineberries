package ru.astrosoup.mineberries.messaging.common;

/**
 * Decoded lifecycle event of a task.
 *
 * @param <R> result type of the task
 */
public interface TaskEvent<R> {
    /** Monotonically increasing event version within one task. */
    long version();

    /**
     * The bot accepted the task.
     *
     * @param version event version
     * @param bot     name of the bot that took the task
     */
    record Accepted<R>(long version, String bot) implements TaskEvent<R> {}

    /** The task reported progress. */
    record Progress<R>(long version, TaskProgress progress) implements TaskEvent<R> {}

    /** The task completed with a result. */
    record Completed<R>(long version, R result) implements TaskEvent<R> {}

    /** The task failed with an error. */
    record Failed<R>(long version, TaskError error) implements TaskEvent<R> {}

    /** The task was cancelled before completion. */
    record Cancelled<R>(long version) implements TaskEvent<R> {}
}