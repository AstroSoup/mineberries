package ru.astrosoup.mineberries.messaging.common.codec.invalid.dupresult;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.common.annotations.Result;
import ru.astrosoup.mineberries.messaging.common.codec.ResultMetaRegistry;

/**
 * Kept in its own subpackage so its deliberately broken fixtures are not picked
 * up by scans of the application packages.
 */
class DuplicateResultNameTest {

    @Result("dup") static class First {}
    @Result("dup") static class Second {}

    @Test
    void duplicateResultNamesFailAtStartup() {
        var ex = assertThrows(IllegalStateException.class,
                () -> new ResultMetaRegistry(getClass().getPackageName()));
        assertTrue(ex.getMessage().contains("Duplicate"), ex.getMessage());
    }
}
