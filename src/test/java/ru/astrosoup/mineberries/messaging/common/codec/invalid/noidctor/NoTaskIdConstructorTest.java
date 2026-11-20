package ru.astrosoup.mineberries.messaging.common.codec.invalid.noidctor;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Task;
import ru.astrosoup.mineberries.messaging.common.codec.ResultMetaRegistry;
import ru.astrosoup.mineberries.messaging.common.codec.TaskMetaRegistry;

class NoTaskIdConstructorTest {

    @Task
    static class BadTask extends AbstractTask<Void> {
        @Property private final String label;

        public BadTask(String label) {
            this.label = label;
        }
    }

    @Test
    void taskWithoutIdConstructorFailsAtStartup() {
        var pkg = getClass().getPackageName();
        var results = new ResultMetaRegistry(pkg);
        var ex = assertThrows(IllegalStateException.class, () -> new TaskMetaRegistry(results, pkg));
        assertTrue(ex.getMessage().contains(BadTask.class.getName()), ex.getMessage());
        assertTrue(ex.getMessage().contains("public constructor with UUID first"), ex.getMessage());
    }
}
