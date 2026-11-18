package ru.astrosoup.mineberries.messaging.common.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field of a {@link Task} or {@link Result} as part of the contract.
 *
 * <p>The property name defaults to the field name and can be overridden with
 * {@link #value()}. Properties are serialized in declaration order.
 */
@Retention(RetentionPolicy.RUNTIME) @Target(ElementType.FIELD)
public @interface Property {
    /** Payload name of the property; defaults to the field name when empty. */
    String value() default "";
}