package ru.astrosoup.mineberries.messaging.common;
/**
* 
* TaskState
 */
public enum TaskState {
    NEW, 
    ACCEPTED, 
    COMPLETED, 
    FAILED, 
    CANCELLED;

    public boolean isTerminal() { return this == COMPLETED || this == FAILED || this == CANCELLED; }
}