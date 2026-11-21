package ru.astrosoup.mineberries.messaging.rabbitmq;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

record OutboxRetryBackoff(long baseMillis, long capMillis) {

    OutboxRetryBackoff(Duration base, Duration cap) {
        this(base.toMillis(), cap.toMillis());
    }

    OutboxRetryBackoff {
        if (baseMillis < 1 || capMillis < baseMillis) {
            throw new IllegalArgumentException("Outbox relay requires 1ms <= retry-base <= retry-cap");
        }
    }

    long delayMillis(int attempt) {
        long ceiling = baseMillis;
        for (int i = 1; i < attempt && ceiling < capMillis; i++) {
            ceiling = ceiling > capMillis / 2 ? capMillis : ceiling * 2;
        }
        long floor = Math.max(1L, ceiling / 2);
        return floor + ThreadLocalRandom.current().nextLong(ceiling - floor + 1);
    }
}
