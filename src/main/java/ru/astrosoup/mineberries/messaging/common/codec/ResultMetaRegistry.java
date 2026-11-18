package ru.astrosoup.mineberries.messaging.common.codec;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;

import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Result;

/**
 * Registry of all classes annotated with
 * {@link ru.astrosoup.mineberries.messaging.common.annotations.Result} in the
 * given packages.
 *
 * <p>Registration validates each result class and builds its {@link ResultMeta}.
 * Duplicate result type identifiers fail at startup.
 */
public final class ResultMetaRegistry {
    private final Map<Class<?>, ResultMeta> byClass = new HashMap<>();
    private final Map<String, ResultMeta> byType = new HashMap<>();

    public ResultMetaRegistry(String... basePackages) {
        var scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(Result.class));
        for (String pkg : basePackages) {
            for (BeanDefinition bd : scanner.findCandidateComponents(pkg)) {
                register(load(bd.getBeanClassName()));
            }
        }
    }

    public Optional<ResultMeta> find(Class<?> resultClass) { return Optional.ofNullable(byClass.get(resultClass)); }
    public ResultMeta require(Class<?> resultClass) {
        return find(resultClass).orElseThrow(() -> new IllegalArgumentException("Not a registered result: " + resultClass.getName()));
    }
    public Collection<ResultMeta> all() { return List.copyOf(byClass.values()); }

    private void register(Class<?> clazz) {
        if (byClass.containsKey(clazz)) return;

        String type = clazz.getAnnotation(Result.class).value();
        if (type.isBlank()) type = clazz.getSimpleName();

        List<TaskMeta.Param> params = collectProperties(clazz);
        var meta = new ResultMeta(type, clazz, params, findConstructor(clazz, params));
        ResultMeta prev = byType.putIfAbsent(type, meta);
        if (prev != null)
            throw new IllegalStateException("Duplicate @Result \"" + type + "\": " + prev.resultClass().getName() + " and " + clazz.getName());
        byClass.put(clazz, meta);
    }

    private List<TaskMeta.Param> collectProperties(Class<?> clazz) {
        var params = new ArrayList<TaskMeta.Param>();
        var names = new HashSet<String>();
        for (Field f : clazz.getDeclaredFields()) {
            Property ann = f.getAnnotation(Property.class);
            if (ann == null) continue;
            if (Modifier.isStatic(f.getModifiers()))
                throw new IllegalStateException(clazz.getName() + "." + f.getName() + ": @Property on static field");
            String name = ann.value().isEmpty() ? f.getName() : ann.value();
            if (!names.add(name))
                throw new IllegalStateException(clazz.getName() + ": duplicate property name \"" + name + "\"");
            f.setAccessible(true);
            params.add(new TaskMeta.Param(name, f));
        }
        return List.copyOf(params);
    }

    /** The constructor must take exactly the @Property field types, in declaration order. */
    private Constructor<?> findConstructor(Class<?> clazz, List<TaskMeta.Param> params) {
        Class<?>[] types = params.stream().map(p -> p.field().getType()).toArray(Class<?>[]::new);
        try {
            Constructor<?> c = clazz.getDeclaredConstructor(types);
            c.setAccessible(true);
            return c;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(clazz.getName() + ": @Result needs a constructor with parameters "
                + Arrays.toString(types) + " (@Property fields in declaration order)");
        }
    }

    private static Class<?> load(String name) {
        try { return ClassUtils.forName(name, ResultMetaRegistry.class.getClassLoader()); }
        catch (ClassNotFoundException e) { throw new IllegalStateException(e); }
    }
}
