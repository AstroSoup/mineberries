package ru.astrosoup.mineberries.messaging.common.codec;

import java.lang.reflect.Field;
import java.util.List;

import ru.astrosoup.mineberries.messaging.common.AbstractTask;

public record TaskMeta(String type, Class<? extends AbstractTask<?>> taskClass, Class<?> resultClass, List<Param> params) {
    public record Param(String name, Field field) {
        Object get(Object target) {
            try { return field.get(target); }
            catch (IllegalAccessException e) { throw new IllegalStateException(e); }
        }
    }
}
