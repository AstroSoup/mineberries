package ru.astrosoup.mineberries.messaging.common.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a bot task and assigns its type identifier.
 *
 * <p>The class must extend
 * {@link ru.astrosoup.mineberries.messaging.common.AbstractTask}. Fields marked
 * with {@link Property} make up the task payload. When {@link #value()} is
 * empty, the simple class name is used as the type.
 *
 * <p>To support reconstruction, a concrete task must have a public constructor
 * with a {@link java.util.UUID} first, followed by one parameter of the exact
 * field type for each {@link Property}. Use declaration order within each class,
 * starting with the concrete class and walking up its superclasses. With no
 * properties the constructor takes only the UUID. Pass that UUID to the
 * {@code AbstractTask(UUID)} constructor, directly or through a superclass.
 */
@Retention(RetentionPolicy.RUNTIME) @Target(ElementType.TYPE)
public @interface Task {
    /** Type identifier sent to the bot; defaults to the simple class name when empty. */
    String value() default "";
}
