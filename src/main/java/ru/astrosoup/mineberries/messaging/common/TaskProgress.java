package ru.astrosoup.mineberries.messaging.common;

/**
* 
* TaskProgress - this record will be returned to task progress listeners.
* @param stage - the stage that task currently holds. Can be different based 
* on a task itself.
**/
public record TaskProgress(String stage) {
    /**
     * if a task does not support stage tracking this value will be used
     * as a listener return.
     */
    public static final TaskProgress NONE = new TaskProgress(null);
}