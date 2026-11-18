package ru.astrosoup.mineberries.messaging.common.codec;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.ResolvableType;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;

import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.*;

/**
 * Registry of all classes annotated with
 * {@link ru.astrosoup.mineberries.messaging.common.annotations.Task} in the
 * given packages.
 *
 * <p>Registration validates the task hierarchy, resolves its result type, and
 * collects its {@link ru.astrosoup.mineberries.messaging.common.annotations.Property}
 * fields. Duplicate task type identifiers fail at startup.
 */
public final class TaskMetaRegistry {
    private final Map<Class<?>, TaskMeta> byClass = new HashMap<>();
    private final Map<String, TaskMeta> byType = new HashMap<>();
    private final ResultMetaRegistry results;

    public TaskMetaRegistry(ResultMetaRegistry results, String... basePackages) {
        this.results = results;
        var scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(Task.class));
        for (String pkg : basePackages) {
            for (BeanDefinition bd : scanner.findCandidateComponents(pkg)) {
                register(load(bd.getBeanClassName()));
            }
        }
    }

    public Optional<TaskMeta> find(Class<?> taskClass) { return Optional.ofNullable(byClass.get(taskClass)); }
    public TaskMeta require(Class<?> taskClass) {
        return find(taskClass).orElseThrow(() -> new IllegalArgumentException("Not a registered task: " + taskClass.getName()));
    }
    public Collection<TaskMeta> all() { return List.copyOf(byClass.values()); }

    @SuppressWarnings("unchecked")
    private void register(Class<?> clazz) {
        if (byClass.containsKey(clazz)) return;
        if (!AbstractTask.class.isAssignableFrom(clazz))
            throw new IllegalStateException(clazz.getName() + ": @Task on a class that doesn't extend AbstractTask");

        String type = clazz.getAnnotation(Task.class).value();
        if (type.isBlank()) {
            type = clazz.getSimpleName();
        }
            
        
        Class<?> result = ResolvableType.forClass(clazz).as(AbstractTask.class).getGeneric(0).resolve();
        if (result == null)
            throw new IllegalStateException(clazz.getName() + ": can't resolve result type R of AbstractTask<R>");
        if (result != Void.class && results.find(result).isEmpty())
            throw new IllegalStateException(clazz.getName() + ": result type " + result.getName() + " is not annotated with @Result");

        var meta = new TaskMeta(type, (Class<? extends AbstractTask<?>>) clazz, result, collectProperties(clazz));
        TaskMeta prev = byType.putIfAbsent(type, meta);
        if (prev != null)
            throw new IllegalStateException("Duplicate @Task \"" + type + "\": " + prev.taskClass().getName() + " and " + clazz.getName());
        byClass.put(clazz, meta);
    }

    private List<TaskMeta.Param> collectProperties(Class<?> clazz) {
        var params = new ArrayList<TaskMeta.Param>();
        var names = new HashSet<String>();
        for (Class<?> c = clazz; c != null && c != Task.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
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
        }
        return List.copyOf(params);
    }

    private static Class<?> load(String name) {
        try { return ClassUtils.forName(name, TaskMetaRegistry.class.getClassLoader()); }
        catch (ClassNotFoundException e) { throw new IllegalStateException(e); }
    }
}