package ru.astrosoup.mineberries.messaging.common;

/** Lifecycle event types emitted by a bot for a task. */
public enum EventType {
    /** The bot accepted the task and started working on it. */
    ACCEPTED,
    /** The task finished successfully; the payload contains the result. */
    COMPLETED,
    /** The task reported intermediate progress. */
    PROGRESS,
    /** The task failed; the payload contains the error. */
    FAILED,
    /** The task was cancelled before completion. */
    CANCELED,
}