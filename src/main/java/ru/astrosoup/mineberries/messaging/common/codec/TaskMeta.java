package ru.astrosoup.mineberries.messaging.common.codec;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.List;

import ru.astrosoup.mineberries.messaging.common.AbstractTask;

/**
 * Runtime metadata for a class annotated with
 * {@link ru.astrosoup.mineberries.messaging.common.annotations.Task}.
 *
 * @param type        task type identifier used on the wire
 * @param taskClass   annotated task class
 * @param resultClass result type produced by the task, or {@link Void}
 * @param params      task properties in constructor order
 * @param constructor public constructor accepting the task id and property values
 */
public record TaskMeta(
        String type,
        Class<? extends AbstractTask<?>> taskClass,
        Class<?> resultClass,
        List<Param> params,
        Constructor<? extends AbstractTask<?>> constructor) {
    /** A single task property and the field that holds its value. */
    public record Param(String name, Field field) {
        Object get(Object target) {
            try { return field.get(target); }
            catch (IllegalAccessException e) { throw new IllegalStateException(e); }
        }
    }
}
