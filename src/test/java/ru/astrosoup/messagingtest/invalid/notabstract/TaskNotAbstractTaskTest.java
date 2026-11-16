package ru.astrosoup.messagingtest.invalid.notabstract;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.*;
import ru.astrosoup.mineberries.messaging.common.codec.*;

/**
 * Lives outside ru.astrosoup.mineberries on purpose: its deliberately broken fixtures
 * must not be picked up by the application's own classpath scan.
 */
class TaskNotAbstractTaskTest {

    @Task
    static class NotATask {}

    @Test
    void taskAnnotationOnNonAbstractTaskClassFailsAtStartup() {
        String pkg = "ru.astrosoup.messagingtest.invalid.notabstract";
        var results = new ResultMetaRegistry(pkg);
        assertThrows(IllegalStateException.class, () -> new TaskMetaRegistry(results, pkg));
    }
}
