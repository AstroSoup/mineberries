package ru.astrosoup.messagingtest.invalid.dupresult;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.*;
import ru.astrosoup.mineberries.messaging.common.codec.*;

/**
 * Lives outside ru.astrosoup.mineberries on purpose: its deliberately broken fixtures
 * must not be picked up by the application's own classpath scan.
 */
class DuplicateResultNameTest {

    @Result("dup") static class First {}
    @Result("dup") static class Second {}

    @Test
    void duplicateResultNamesFailAtStartup() {
        var ex = assertThrows(IllegalStateException.class, () -> new ResultMetaRegistry("ru.astrosoup.messagingtest.invalid.dupresult"));
        assertTrue(ex.getMessage().contains("Duplicate"), ex.getMessage());
    }
}
