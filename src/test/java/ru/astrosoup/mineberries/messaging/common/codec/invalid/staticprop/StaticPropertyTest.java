package ru.astrosoup.mineberries.messaging.common.codec.invalid.staticprop;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Result;
import ru.astrosoup.mineberries.messaging.common.codec.ResultMetaRegistry;

/**
 * Kept in its own subpackage so its deliberately broken fixtures are not picked
 * up by scans of the application packages.
 */
class StaticPropertyTest {

    @Result
    static class BadResult {
        @Property static String shared = "x";
    }

    @Test
    void staticPropertyFieldFailsAtStartup() {
        var ex = assertThrows(IllegalStateException.class,
                () -> new ResultMetaRegistry(getClass().getPackageName()));
        assertTrue(ex.getMessage().contains("static"), ex.getMessage());
    }
}
