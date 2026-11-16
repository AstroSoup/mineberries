package ru.astrosoup.mineberries.messaging.common.codec;

import static org.junit.jupiter.api.Assertions.*;
import static ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.common.EventType;
import ru.astrosoup.mineberries.messaging.common.TaskError;
import ru.astrosoup.mineberries.messaging.common.TaskEvent;
import ru.astrosoup.mineberries.messaging.common.TaskProgress;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;
import ru.astrosoup.mineberries.messaging.support.Events;

class TaskCodecTest {
    private final ResultMetaRegistry results = new ResultMetaRegistry(PACKAGE);
    private final TaskMetaRegistry tasks = new TaskMetaRegistry(results, PACKAGE);
    private final TaskCodec codec = new TaskCodec(tasks, results, TaskCodec.defaultMapper());
    private final UUID id = UUID.randomUUID();

    // ---- encode ----

    @Test
    void encodesTypeIdAndProperties() {
        var task = new GreetTask("Ann", 3);

        MessageDto dto = codec.encode(task);

        assertEquals(task.id(), dto.messageId());
        assertEquals("greet", dto.type());
        assertEquals(Map.of("name", "Ann", "count", 3), dto.payload());
    }

    @Test
    void typeDefaultsToSimpleClassName() {
        assertEquals("PingTask", codec.encode(new PingTask()).type());
    }

    @Test
    void encodesNullPropertyAsNull() {
        var dto = codec.encode(new GreetTask(null, 1));
        assertTrue(dto.payload().containsKey("name"));
        assertNull(dto.payload().get("name"));
    }

    @Test
    void encodesCollectionProperty() {
        var dto = codec.encode(new SumTask(List.of(1, 2, 3)));
        assertEquals(List.of(1, 2, 3), dto.payload().get("numbers"));
    }

    @Test
    void encodesSuperclassProperties() {
        var dto = codec.encode(new LabelTask("bob", "urgent"));
        assertEquals("owned", dto.type());
        assertEquals(Map.of("owner", "bob", "label", "urgent"), dto.payload());
    }

    @Test
    void encodeRejectsUnregisteredTask() {
        var unregistered = new ru.astrosoup.mineberries.messaging.common.AbstractTask<Void>() {};
        assertFalse(codec.supports(unregistered.getClass()));
        assertThrows(IllegalArgumentException.class, () -> codec.encode(unregistered));
    }

    @Test
    void supportsRegisteredTasksOnly() {
        assertTrue(codec.supports(GreetTask.class));
        assertFalse(codec.supports(GreetResult.class));
    }

    // ---- decode: non-result events ----

    @Test
    void decodesAccepted() {
        var ev = codec.decode(GreetTask.class, Events.accepted(id, 1));
        assertEquals(new TaskEvent.Accepted<Object>(1), ev);
    }

    @Test
    void decodesCanceled() {
        var ev = codec.decode(GreetTask.class, Events.canceled(id, 4));
        assertEquals(new TaskEvent.Cancelled<Object>(4), ev);
    }

    @Test
    void decodesProgress() {
        var ev = codec.decode(GreetTask.class, Events.progress(id, 2, "packing"));
        assertEquals(new TaskEvent.Progress<Object>(2, new TaskProgress("packing")), ev);
    }

    @Test
    void decodesFailed() {
        var ev = codec.decode(GreetTask.class, Events.failed(id, 5, "boom", true));
        assertEquals(new TaskEvent.Failed<Object>(5, new TaskError("boom", true)), ev);
    }

    @Test
    void failedWithoutRetryableFlagIsNotRetryable() {
        var dto = Events.of(id, 5, EventType.FAILED, Map.of("message", "boom"));
        var ev = codec.decode(GreetTask.class, dto);
        assertEquals(new TaskEvent.Failed<Object>(5, new TaskError("boom", false)), ev);
    }

    @Test
    void nonResultEventsDecodeWithNullPayload() {
        assertEquals(new TaskEvent.Accepted<Object>(0), codec.decode(GreetTask.class, Events.of(id, 0, EventType.ACCEPTED, null)));
    }

    // ---- decode: COMPLETED / @Result ----

    @Test
    void decodesCompletedIntoResultObject() {
        var dto = Events.completed(id, 3, Map.of("greeting", "hi Ann", "length", 6));

        var completed = assertInstanceOf(TaskEvent.Completed.class, codec.decode(GreetTask.class, dto));

        GreetResult r = assertInstanceOf(GreetResult.class, completed.result());
        assertEquals("hi Ann", r.greeting());
        assertEquals(6, r.length());
        assertEquals(3, completed.version());
    }

    @Test
    void usesPropertyNameAndConvertsGenericTypes() {
        var dto = Events.completed(id, 3, Map.of("numbers", List.of(1, 2, 3), "sum", 6));

        var completed = assertInstanceOf(TaskEvent.Completed.class, codec.decode(SumTask.class, dto));

        TotalsResult r = assertInstanceOf(TotalsResult.class, completed.result());
        assertEquals(List.of(1, 2, 3), r.numbers());
        assertEquals(6L, r.total());
    }

    @Test
    void ignoresPayloadKeysThatAreNotProperties() {
        var dto = Events.completed(id, 3, Map.of("greeting", "hi", "length", 2, "extra", "x"));
        var completed = assertInstanceOf(TaskEvent.Completed.class, codec.decode(GreetTask.class, dto));
        assertInstanceOf(GreetResult.class, completed.result());
    }

    @Test
    void explicitNullValueIsAccepted() {
        var payload = new HashMap<String, Object>();
        payload.put("note", null);

        var completed = assertInstanceOf(TaskEvent.Completed.class,
            codec.decode(NoteTask.class, Events.completed(id, 1, payload)));

        assertNull(((NoteResult) completed.result()).note());
    }

    @Test
    void missingPropertyKeyFails() {
        var dto = Events.completed(id, 3, Map.of("greeting", "hi"));
        var ex = assertThrows(IllegalStateException.class, () -> codec.decode(GreetTask.class, dto));
        assertTrue(ex.getMessage().contains("length"), ex.getMessage());
    }

    @Test
    void completedWithoutPayloadFailsForNonVoidResult() {
        var dto = Events.of(id, 3, EventType.COMPLETED, null);
        assertThrows(IllegalStateException.class, () -> codec.decode(GreetTask.class, dto));
    }

    @Test
    void completedOfVoidTaskHasNullResult() {
        var completed = assertInstanceOf(TaskEvent.Completed.class,
            codec.decode(PingTask.class, Events.of(id, 1, EventType.COMPLETED, null)));
        assertNull(completed.result());
    }

    @Test
    void resultWithoutPropertiesDecodesFromEmptyPayload() {
        var completed = assertInstanceOf(TaskEvent.Completed.class,
            codec.decode(AckTask.class, Events.completed(id, 1, Map.of())));
        assertInstanceOf(AckResult.class, completed.result());
    }

    @Test
    void decodeRejectsUnregisteredTaskClass() {
        assertThrows(IllegalArgumentException.class, () -> codec.decode(GreetResult.class, Events.accepted(id, 0)));
    }
}
