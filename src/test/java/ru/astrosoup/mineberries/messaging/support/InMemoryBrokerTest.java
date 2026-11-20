package ru.astrosoup.mineberries.messaging.support;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;

class InMemoryBrokerTest {
    private final InMemoryBroker broker = new InMemoryBroker();

    private static MessageDto message() {
        return new MessageDto(UUID.randomUUID(), "task", "t", 1, null, Map.of());
    }

    @Test
    void recordsSentMessagesInOrder() {
        MessageDto a = message(), b = message();
        assertTrue(broker.send(a).isDone());
        broker.send(b);
        assertEquals(List.of(a, b), broker.sent());
        assertSame(b, broker.lastSent());
    }

    @Test
    void recordsCancelsUsingTheSameMessageInterface() {
        var task = message();
        broker.sendCancel(task.messageId(), 1).join();
        assertEquals(new MessageDto(task.messageId(), "cancel", null, 1, null, null), broker.lastSent());
    }

    @Test
    void lastSentFailsWhenNothingWasSent() {
        assertThrows(IllegalStateException.class, broker::lastSent);
    }

    @Test
    void failNextSendFailsOnceAndRecordsNothing() {
        var boom = new IllegalStateException("down");
        broker.failNextSend(boom);

        var failed = broker.send(message());
        assertTrue(failed.isCompletedExceptionally());
        assertTrue(broker.sent().isEmpty());

        assertFalse(broker.send(message()).isCompletedExceptionally());
        assertEquals(1, broker.sent().size());
    }

    @Test
    void publishDeliversToEverySubscriber() {
        var first = new ArrayList<EventDto>();
        var second = new ArrayList<EventDto>();
        broker.subscribe(first::add);
        broker.subscribe(second::add);

        EventDto event = Events.accepted(UUID.randomUUID(), 0, "courier_andy");
        broker.publish(event);

        assertEquals(List.of(event), first);
        assertEquals(List.of(event), second);
    }

    @Test
    void publishWithoutSubscribersIsANoOp() {
        assertDoesNotThrow(() -> broker.publish(Events.accepted(UUID.randomUUID(), 0, "courier_andy")));
    }
}
