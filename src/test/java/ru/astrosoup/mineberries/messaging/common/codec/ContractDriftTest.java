package ru.astrosoup.mineberries.messaging.common.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.EventType;
import ru.astrosoup.mineberries.messaging.common.TaskEvent;
import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;
import tools.jackson.databind.ObjectMapper;

/**
 * Keeps the executable contract fixtures aligned with the task/result metadata.
 *
 * <p>Adding a {@code @Task} or {@code @Result} without a fixture fails here, as
 * does changing a fixture so that it no longer round-trips through
 * {@link TaskCodec}.
 */
class ContractDriftTest {
    private static final String[] APPLICATION_PACKAGES = {
            "ru.astrosoup.mineberries.messaging.banker",
            "ru.astrosoup.mineberries.messaging.supervisor",
            "ru.astrosoup.mineberries.messaging.courier"
    };
    private static final String FIXTURE_ROOT = "/contract/";
    private static final Path DOCUMENTED_EXAMPLES = Path.of("documentation", "schemas", "examples");

    private final ResultMetaRegistry results = new ResultMetaRegistry(APPLICATION_PACKAGES);
    private final TaskMetaRegistry tasks = new TaskMetaRegistry(results, APPLICATION_PACKAGES);
    private final ObjectMapper mapper = TaskCodec.defaultMapper();
    private final TaskCodec codec = new TaskCodec(tasks, results, mapper);

    @Test
    void everyTaskCanBeReconstructedFromItsFixtureAndReencoded() throws Exception {
        for (TaskMeta meta : tasks.all()) {
            MessageDto fixture = readTaskFixture(meta.type());

            assertEquals("task", fixture.kind(), meta.type() + " fixture kind");
            assertEquals(meta.type(), fixture.type(), "fixture type");
            assertEquals(1, fixture.schemaVersion(), meta.type() + " fixture schemaVersion");
            assertNotNull(fixture.payload(), meta.type() + " fixture payload");

            AbstractTask<?> task = instantiate(meta, fixture.messageId(), fixture.payload());
            MessageDto encoded = codec.encode(task);

            assertEquals(fixture.messageId(), task.id(), meta.type() + " reconstructed id");
            assertEquals(fixture.messageId(), encoded.messageId(), meta.type() + " encoded id");
            assertEquals("task", encoded.kind(), meta.type() + " encoded kind");
            assertEquals(meta.type(), encoded.type(), meta.type() + " encoded type");
            assertEquals(1, encoded.schemaVersion(), meta.type() + " encoded schemaVersion");
            assertJsonEquals(fixture.payload(), encoded.payload(), meta.type() + " payload");

            if (meta.resultClass() != Void.class)
                readFixture("event.completed." + meta.type() + ".json");
        }
    }

    @Test
    void everyResultHasAFixtureAndDecodesThroughTaskCodec() throws Exception {
        for (ResultMeta result : results.all()) {
            TaskMeta owner = tasks.all().stream()
                    .filter(task -> task.resultClass().equals(result.resultClass()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError(
                            "No registered task produces result " + result.type()));

            EventDto fixture = readEventFixture(owner.type());
            assertEquals(EventType.COMPLETED, fixture.type(), result.type() + " fixture event type");

            TaskEvent<Object> event = codec.decode(owner.taskClass(), fixture);
            var completed = assertInstanceOf(TaskEvent.Completed.class, event, result.type() + " decode");
            assertInstanceOf(result.resultClass(), completed.result(), result.type() + " result class");

            assertJsonEquals(fixture.payload(),
                    mapper.convertValue(completed.result(), Object.class),
                    result.type() + " decoded payload");
        }
    }

    @Test
    void fixturesMatchTheDocumentedExamples() throws Exception {
        for (TaskMeta meta : tasks.all()) {
            assertSameJson("task." + meta.type() + ".json");
            if (meta.resultClass() != Void.class)
                assertSameJson("event.completed." + meta.type() + ".json");
        }
    }

    private MessageDto readTaskFixture(String taskType) throws Exception {
        return mapper.readValue(readFixture("task." + taskType + ".json"), MessageDto.class);
    }

    private EventDto readEventFixture(String taskType) throws Exception {
        return mapper.readValue(readFixture("event.completed." + taskType + ".json"), EventDto.class);
    }

    private AbstractTask<?> instantiate(TaskMeta meta, UUID id, Map<String, Object> payload) throws Exception {
        List<TaskMeta.Param> params = meta.params();
        Object[] args = new Object[params.size() + 1];
        args[0] = id;

        for (int i = 0; i < params.size(); i++) {
            TaskMeta.Param param = params.get(i);
            assertTrue(payload.containsKey(param.name()),
                    "Missing \"" + param.name() + "\" in fixture for " + meta.type());
            args[i + 1] = mapper.convertValue(payload.get(param.name()),
                    mapper.constructType(param.field().getGenericType()));
        }

        return meta.constructor().newInstance(args);
    }

    private void assertSameJson(String name) throws Exception {
        Path example = DOCUMENTED_EXAMPLES.resolve(name);
        assertTrue(Files.exists(example), "Missing documented example " + example);
        assertEquals(mapper.readTree(Files.readString(example)), mapper.readTree(readFixture(name)),
                "Fixture " + name + " differs from " + example);
    }

    private static String readFixture(String name) throws IOException {
        try (InputStream in = ContractDriftTest.class.getResourceAsStream(FIXTURE_ROOT + name)) {
            assertNotNull(in, "Missing contract fixture: src/test/resources/contract/" + name);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void assertJsonEquals(Object expected, Object actual, String label) {
        assertJsonEquals(expected, actual, label, "$");
    }

    private static void assertJsonEquals(Object expected, Object actual, String label, String path) {
        if (expected instanceof Map<?, ?> expectedMap && actual instanceof Map<?, ?> actualMap) {
            assertEquals(expectedMap.keySet(), actualMap.keySet(), label + ": keys at " + path);
            for (Object key : expectedMap.keySet())
                assertJsonEquals(expectedMap.get(key), actualMap.get(key), label, path + "." + key);
        } else if (expected instanceof List<?> expectedList && actual instanceof List<?> actualList) {
            assertEquals(expectedList.size(), actualList.size(), label + ": list size at " + path);
            for (int i = 0; i < expectedList.size(); i++)
                assertJsonEquals(expectedList.get(i), actualList.get(i), label, path + "[" + i + "]");
        } else if (expected instanceof Number expectedNumber && actual instanceof Number actualNumber) {
            assertEquals(0, new BigDecimal(expectedNumber.toString())
                            .compareTo(new BigDecimal(actualNumber.toString())),
                    label + ": number at " + path);
        } else {
            assertEquals(expected, actual, label + ": value at " + path);
        }
    }
}
