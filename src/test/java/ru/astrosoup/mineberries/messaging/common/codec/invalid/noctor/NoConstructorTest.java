package ru.astrosoup.mineberries.messaging.common.codec.invalid.noctor;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Result;
import ru.astrosoup.mineberries.messaging.common.codec.ResultMetaRegistry;

/**
 * Kept in its own subpackage so its deliberately broken fixtures are not picked
 * up by scans of the application packages.
 */
class NoConstructorTest {

    @Result
    static class BadResult {
        @Property private final String a;
        BadResult(int a) { this.a = String.valueOf(a); }
    }

    @Test
    void resultWithoutMatchingConstructorFailsAtStartup() {
        var ex = assertThrows(IllegalStateException.class,
                () -> new ResultMetaRegistry(getClass().getPackageName()));
        assertTrue(ex.getMessage().contains("constructor"), ex.getMessage());
    }
}
