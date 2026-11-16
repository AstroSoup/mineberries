package ru.astrosoup.messagingtest.invalid.noctor;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.*;
import ru.astrosoup.mineberries.messaging.common.codec.*;

/**
 * Lives outside ru.astrosoup.mineberries on purpose: its deliberately broken fixtures
 * must not be picked up by the application's own classpath scan.
 */
class NoConstructorTest {

    @Result
    static class BadResult {
        @Property private final String a;
        BadResult(int a) { this.a = String.valueOf(a); }
    }

    @Test
    void resultWithoutMatchingConstructorFailsAtStartup() {
        var ex = assertThrows(IllegalStateException.class, () -> new ResultMetaRegistry("ru.astrosoup.messagingtest.invalid.noctor"));
        assertTrue(ex.getMessage().contains("constructor"), ex.getMessage());
    }
}
