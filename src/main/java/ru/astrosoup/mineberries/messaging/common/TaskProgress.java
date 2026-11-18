package ru.astrosoup.mineberries.messaging.common;

/**
 * Progress reported by a running task.
 *
 * @param stage current stage name, or {@code null} when the task did not
 *              report any stages
 */
public record TaskProgress(String stage) {
    /** Progress value used before a task reports a stage. */
    public static final TaskProgress NONE = new TaskProgress(null);
}