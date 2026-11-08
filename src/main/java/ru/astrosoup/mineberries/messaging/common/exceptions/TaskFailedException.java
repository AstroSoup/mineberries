package ru.astrosoup.mineberries.messaging.common.exceptions;

import lombok.Getter;
import lombok.experimental.Accessors;
import ru.astrosoup.mineberries.messaging.common.TaskError;

@Accessors(fluent = true)
public class TaskFailedException extends RuntimeException {
    private final @Getter TaskError error;
    
    public TaskFailedException(TaskError error) {
        super(error.message());
        this.error = error;
    }
}