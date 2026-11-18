package ru.astrosoup.mineberries.messaging.common;
/** Lifecycle state of a submitted task. */
public enum TaskState {
    /** Created locally and not yet accepted by the bot. */
    NEW,
    /** Accepted by the bot and in progress. */
    ACCEPTED,
    /** Finished successfully. */
    COMPLETED,
    /** Finished with an error. */
    FAILED,
    /** Cancelled before completion. */
    CANCELLED;

    /** Returns whether no further events can change the state. */
    public boolean isTerminal() { return this == COMPLETED || this == FAILED || this == CANCELLED; }
}