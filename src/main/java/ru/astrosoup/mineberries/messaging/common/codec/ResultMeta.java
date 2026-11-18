package ru.astrosoup.mineberries.messaging.common.codec;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import tools.jackson.databind.ObjectMapper;

/**
 * Runtime metadata for a class annotated with {@link ru.astrosoup.mineberries.messaging.common.annotations.Result}.
 *
 * @param type        result type identifier used on the wire
 * @param resultClass annotated result class
 * @param params      properties in declaration order
 * @param constructor constructor that accepts the property values
 */
public record ResultMeta(String type, Class<?> resultClass, List<TaskMeta.Param> params, Constructor<?> constructor) {

    /** Builds a result instance from a COMPLETED payload. Every @Property key must be present. */
    public Object create(Map<String, Object> payload, ObjectMapper mapper) {
        Object[] args = new Object[params.size()];
        for (int i = 0; i < args.length; i++) {
            TaskMeta.Param p = params.get(i);
            if (!payload.containsKey(p.name()))
                throw new IllegalStateException("Missing property \"" + p.name() + "\" in result " + type);
            Field f = p.field();
            args[i] = mapper.convertValue(payload.get(p.name()), mapper.constructType(f.getGenericType()));
        }
        try {
            return constructor.newInstance(args);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Can't create result " + type, e.getCause());
        } catch (ReflectiveOperationException | IllegalArgumentException e) {
            throw new IllegalStateException("Can't create result " + type, e);
        }
    }
}
