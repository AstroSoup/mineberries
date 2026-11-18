package ru.astrosoup.mineberries.messaging.fixtures;

import java.util.List;

import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Result;
import ru.astrosoup.mineberries.messaging.common.annotations.Task;

/** Valid @Task / @Result classes shared by the messaging tests. Scan {@link #PACKAGE} to register them. */
public final class MessagingFixtures {
    public static final String PACKAGE = MessagingFixtures.class.getPackageName();

    /**
     * Packages scanned by the tests: the real bot task packages plus these fixtures.
     * The invalid fixtures live outside these packages so the application scan never
     * picks them up.
     */
    public static final String[] SCAN_PACKAGES = {
            "ru.astrosoup.mineberries.messaging.banker",
            "ru.astrosoup.mineberries.messaging.supervisor",
            "ru.astrosoup.mineberries.messaging.courier",
            PACKAGE
    };

    private MessagingFixtures() {}

    @Task("greet")
    public static class GreetTask extends AbstractTask<GreetResult> {
        @Property private final String name;
        @Property("count") private final int times;

        public GreetTask(String name, int times) { this.name = name; this.times = times; }
    }

    @Result
    public static class GreetResult {
        @Property private final String greeting;
        @Property private final int length;

        public GreetResult(String greeting, int length) { this.greeting = greeting; this.length = length; }
        public String greeting() { return greeting; }
        public int length() { return length; }
    }

    @Result("Totals")
    public static class TotalsResult {
        @Property private final List<Integer> numbers;
        @Property("sum") private final long total;

        public TotalsResult(List<Integer> numbers, long total) { this.numbers = numbers; this.total = total; }
        public List<Integer> numbers() { return numbers; }
        public long total() { return total; }
    }

    @Task
    public static class PingTask extends AbstractTask<Void> {}

    @Task
    public static class AckTask extends AbstractTask<AckResult> {}

    @Result
    public static class AckResult {}

    public abstract static class OwnedTask<R> extends AbstractTask<R> {
        @Property protected final String owner;

        protected OwnedTask(String owner) { this.owner = owner; }
    }

    @Task("owned")
    public static class LabelTask extends OwnedTask<Void> {
        @Property private final String label;

        public LabelTask(String owner, String label) { super(owner); this.label = label; }
    }
}
