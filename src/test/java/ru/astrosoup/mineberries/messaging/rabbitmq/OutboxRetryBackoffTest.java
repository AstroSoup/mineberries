package ru.astrosoup.mineberries.messaging.rabbitmq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class OutboxRetryBackoffTest {
    @Test
    void doublesTheRetryCeilingUntilTheCapAndAppliesJitter() {
        var backoff = new OutboxRetryBackoff(1000, 5000);

        for (int sample = 0; sample < 20; sample++) {
            assertBetween(backoff.delayMillis(1), 500, 1000);
            assertBetween(backoff.delayMillis(2), 1000, 2000);
            assertBetween(backoff.delayMillis(3), 2000, 4000);
            assertBetween(backoff.delayMillis(4), 2500, 5000);
            assertBetween(backoff.delayMillis(Integer.MAX_VALUE), 2500, 5000);
        }
    }

    @Test
    void preventsZeroDelaysAndRejectsInvalidBounds() {
        assertEquals(1, new OutboxRetryBackoff(1, 1).delayMillis(1));
        assertThrows(IllegalArgumentException.class, () -> new OutboxRetryBackoff(0, 1000));
        assertThrows(IllegalArgumentException.class, () -> new OutboxRetryBackoff(1000, 999));
    }

    private static void assertBetween(long delay, long min, long max) {
        assertTrue(delay >= min && delay <= max, () -> delay + " ms not within [" + min + ", " + max + "]");
    }
}
