package ru.astrosoup.messagingtest.invalid.unannotated;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.*;
import ru.astrosoup.mineberries.messaging.common.codec.*;

/**
 * Lives outside ru.astrosoup.mineberries on purpose: its deliberately broken fixtures
 * must not be picked up by the application's own classpath scan.
 */
class UnannotatedResultTest {

    static class PlainResult {}

    @Task
    static class BadTask extends AbstractTask<PlainResult> {}

    @Test
    void taskWhoseResultIsNotAnnotatedFailsAtStartup() {
        String pkg = "ru.astrosoup.messagingtest.invalid.unannotated";
        var results = new ResultMetaRegistry(pkg);
        var ex = assertThrows(IllegalStateException.class, () -> new TaskMetaRegistry(results, pkg));
        assertTrue(ex.getMessage().contains("@Result"), ex.getMessage());
    }
}
