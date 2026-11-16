package ru.astrosoup.mineberries.messaging.common.codec;

import static org.junit.jupiter.api.Assertions.*;
import static ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.*;

import java.util.List;

import org.junit.jupiter.api.Test;

class MetaRegistryTest {
    private final ResultMetaRegistry results = new ResultMetaRegistry(PACKAGE);
    private final TaskMetaRegistry tasks = new TaskMetaRegistry(results, PACKAGE);

    private static List<String> names(List<TaskMeta.Param> params) {
        return params.stream().map(TaskMeta.Param::name).toList();
    }

    // ---- results ----

    @Test
    void resultNameDefaultsToSimpleClassName() {
        assertEquals("GreetResult", results.require(GreetResult.class).type());
    }

    @Test
    void resultNameCanBeOverridden() {
        assertEquals("Totals", results.require(TotalsResult.class).type());
    }

    @Test
    void resultPropertiesFollowDeclarationOrderAndUseCustomNames() {
        assertEquals(List.of("numbers", "sum"), names(results.require(TotalsResult.class).params()));
        assertEquals(List.of("greeting", "length"), names(results.require(GreetResult.class).params()));
    }

    @Test
    void resultWithoutPropertiesIsRegistered() {
        assertTrue(results.require(AckResult.class).params().isEmpty());
    }

    @Test
    void onlyAnnotatedResultClassesAreRegistered() {
        assertTrue(results.find(GreetTask.class).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> results.require(GreetTask.class));
        assertEquals(4, results.all().size()); // Greet, Totals, Ack, Note
    }

    // ---- tasks ----

    @Test
    void taskNameDefaultsToSimpleClassNameOrUsesValue() {
        assertEquals("greet", tasks.require(GreetTask.class).type());
        assertEquals("PingTask", tasks.require(PingTask.class).type());
    }

    @Test
    void taskResultClassComesFromGenericParameter() {
        assertEquals(GreetResult.class, tasks.require(GreetTask.class).resultClass());
        assertEquals(Void.class, tasks.require(PingTask.class).resultClass());
        assertEquals(Void.class, tasks.require(LabelTask.class).resultClass()); // via generic superclass
    }

    @Test
    void taskPropertiesIncludeSuperclassAndCustomNames() {
        assertEquals(List.of("name", "count"), names(tasks.require(GreetTask.class).params()));
        assertTrue(names(tasks.require(LabelTask.class).params()).containsAll(List.of("owner", "label")));
        assertEquals(2, tasks.require(LabelTask.class).params().size());
    }

    @Test
    void unknownTaskClass() {
        assertTrue(tasks.find(GreetResult.class).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> tasks.require(GreetResult.class));
    }

    @Test
    void allListsEveryTask() {
        assertEquals(6, tasks.all().size()); // Greet, Sum, Ping, Ack, Note, Label
    }
}
