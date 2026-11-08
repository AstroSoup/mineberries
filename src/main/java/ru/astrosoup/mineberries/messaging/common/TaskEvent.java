package ru.astrosoup.mineberries.messaging.common;

public interface TaskEvent<R> {
    long version();
    record Accepted<R>(long version) implements TaskEvent<R> {}
    record Progress<R>(long version, TaskProgress progress) implements TaskEvent<R> {}
    record Completed<R>(long version, R result) implements TaskEvent<R> {}
    record Failed<R>(long version, TaskError error) implements TaskEvent<R> {}
    record Cancelled<R>(long version) implements TaskEvent<R> {}
}