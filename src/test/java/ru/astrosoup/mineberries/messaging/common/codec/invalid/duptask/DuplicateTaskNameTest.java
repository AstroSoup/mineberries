package ru.astrosoup.mineberries.messaging.common.codec.invalid.duptask;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.Task;
import ru.astrosoup.mineberries.messaging.common.codec.ResultMetaRegistry;
import ru.astrosoup.mineberries.messaging.common.codec.TaskMetaRegistry;

/**
 * Kept in its own subpackage so its deliberately broken fixtures are not picked
 * up by scans of the application packages.
 */
class DuplicateTaskNameTest {

    @Task("dup") static class First extends AbstractTask<Void> {}
    @Task("dup") static class Second extends AbstractTask<Void> {}

    @Test
    void duplicateTaskNamesFailAtStartup() {
        var pkg = getClass().getPackageName();
        var results = new ResultMetaRegistry(pkg);
        var ex = assertThrows(IllegalStateException.class, () -> new TaskMetaRegistry(results, pkg));
        assertTrue(ex.getMessage().contains("Duplicate"), ex.getMessage());
    }
}
