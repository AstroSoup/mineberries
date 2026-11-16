package ru.astrosoup.messagingtest.invalid.staticprop;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.*;
import ru.astrosoup.mineberries.messaging.common.codec.*;

/**
 * Lives outside ru.astrosoup.mineberries on purpose: its deliberately broken fixtures
 * must not be picked up by the application's own classpath scan.
 */
class StaticPropertyTest {

    @Result
    static class BadResult {
        @Property static String shared = "x";
    }

    @Test
    void staticPropertyFieldFailsAtStartup() {
        var ex = assertThrows(IllegalStateException.class, () -> new ResultMetaRegistry("ru.astrosoup.messagingtest.invalid.staticprop"));
        assertTrue(ex.getMessage().contains("static"), ex.getMessage());
    }
}
