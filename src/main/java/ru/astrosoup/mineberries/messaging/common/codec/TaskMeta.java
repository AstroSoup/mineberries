package ru.astrosoup.mineberries.messaging.common.codec;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

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

    AbstractTask<?> create(UUID id, Map<String, Object> payload, ObjectMapper mapper) {
        if (id == null) throw new IllegalArgumentException("Missing ID for task " + type);
        if (payload == null) throw new IllegalArgumentException("Missing payload for task " + type);

        Object[] args = new Object[params.size() + 1];
        args[0] = id;
        for (int i = 0; i < params.size(); i++) {
            Param param = params.get(i);
            String name = param.name();
            if (!payload.containsKey(name))
                throw new IllegalArgumentException("Missing property \"" + name + "\" for task " + type);

            Object value = payload.get(name);
            if (value == null && param.field().getType().isPrimitive())
                throw new IllegalArgumentException("Null property \"" + name + "\" for task " + type);

            try {
                args[i + 1] = mapper.convertValue(value, mapper.constructType(param.field().getGenericType()));
            } catch (JacksonException | IllegalArgumentException e) {
                // Conversion and validation errors may contain secret payload values.
                throw new IllegalArgumentException("Invalid property \"" + name + "\" for task " + type);
            }
        }

        try {
            return constructor.newInstance(args);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Error error) throw error;
            throw new IllegalArgumentException("Invalid payload for task " + type + ": constructor validation failed");
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Can't create task " + type, e);
        }
    }

    /** A single task property and the field that holds its value. */
    public record Param(String name, Field field) {
        Object get(Object target) {
            try { return field.get(target); }
            catch (IllegalAccessException e) { throw new IllegalStateException(e); }
        }
    }
}
