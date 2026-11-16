package ru.astrosoup.mineberries.messaging.common.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a task result. Result fields annotated with {@link Property}
 * are read from the COMPLETED event payload and passed, in declaration order,
 * to the all-args constructor.
 */
@Retention(RetentionPolicy.RUNTIME) @Target(ElementType.TYPE)
public @interface Result {
    String value() default "";
}
