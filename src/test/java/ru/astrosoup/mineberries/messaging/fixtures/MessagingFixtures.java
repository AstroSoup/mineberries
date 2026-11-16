package ru.astrosoup.mineberries.messaging.fixtures;

import java.util.List;

import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Result;
import ru.astrosoup.mineberries.messaging.common.annotations.Task;

/** Valid @Task / @Result classes shared by the messaging tests. Scan {@link #PACKAGE} to register them. */
public final class MessagingFixtures {
    public static final String PACKAGE = MessagingFixtures.class.getPackageName();

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

    @Task
    public static class SumTask extends AbstractTask<TotalsResult> {
        @Property private final List<Integer> numbers;

        public SumTask(List<Integer> numbers) { this.numbers = numbers; }
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

    @Task
    public static class NoteTask extends AbstractTask<NoteResult> {}

    @Result
    public static class NoteResult {
        @Property private final String note;

        public NoteResult(String note) { this.note = note; }
        public String note() { return note; }
    }

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
