package ru.astrosoup.mineberries.messaging.support;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import ru.astrosoup.mineberries.messaging.common.brocker.EventSource;
import ru.astrosoup.mineberries.messaging.common.brocker.MessageSender;
import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;

/**
 * Test-only in-memory broker. Records every sent message and lets a test play the
 * worker side by publishing events by hand; delivery is synchronous.
 */
public class InMemoryBroker implements MessageSender, EventSource {
    private final List<MessageDto> sent = new CopyOnWriteArrayList<>();
    private final List<Consumer<EventDto>> handlers = new CopyOnWriteArrayList<>();
    private final AtomicReference<Throwable> nextFailure = new AtomicReference<>();

    @Override
    public CompletableFuture<Void> send(MessageDto dto) {
        Throwable failure = nextFailure.getAndSet(null);
        if (failure != null) return CompletableFuture.failedFuture(failure);
        sent.add(dto);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void subscribe(Consumer<EventDto> handler) { handlers.add(handler); }

    /** Delivers an event to every subscriber, as if a worker had emitted it. */
    public void publish(EventDto event) { handlers.forEach(h -> h.accept(event)); }

    /** Makes the next send() fail with the given error; later sends succeed again. */
    public void failNextSend(Throwable error) { nextFailure.set(error); }

    public List<MessageDto> sent() { return List.copyOf(sent); }

    public MessageDto lastSent() {
        if (sent.isEmpty()) throw new IllegalStateException("Nothing was sent");
        return sent.get(sent.size() - 1);
    }
}
