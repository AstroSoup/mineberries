package ru.astrosoup.mineberries.messaging.common.codec.invalid.notabstract;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.common.annotations.Task;
import ru.astrosoup.mineberries.messaging.common.codec.ResultMetaRegistry;
import ru.astrosoup.mineberries.messaging.common.codec.TaskMetaRegistry;

/**
 * Kept in its own subpackage so its deliberately broken fixtures are not picked
 * up by scans of the application packages.
 */
class TaskNotAbstractTaskTest {

    @Task
    static class NotATask {}

    @Test
    void taskAnnotationOnNonAbstractTaskClassFailsAtStartup() {
        var pkg = getClass().getPackageName();
        var results = new ResultMetaRegistry(pkg);
        assertThrows(IllegalStateException.class, () -> new TaskMetaRegistry(results, pkg));
    }
}
