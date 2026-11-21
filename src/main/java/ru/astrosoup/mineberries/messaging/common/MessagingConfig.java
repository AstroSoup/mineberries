package ru.astrosoup.mineberries.messaging.common;



import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.common.broker.EventSource;
import ru.astrosoup.mineberries.messaging.common.broker.MessageSender;
import ru.astrosoup.mineberries.messaging.common.codec.ResultMetaRegistry;
import ru.astrosoup.mineberries.messaging.common.codec.TaskMetaRegistry;
import ru.astrosoup.mineberries.messaging.tasks.TaskRepository;
import tools.jackson.databind.ObjectMapper;

/** Spring configuration for the messaging registries, codec, and task manager. */
@Configuration
public class MessagingConfig {
    @Bean ResultMetaRegistry resultMetaRegistry() { return new ResultMetaRegistry("ru.astrosoup.mineberries"); }

    @Bean TaskMetaRegistry taskMetaRegistry(ResultMetaRegistry results) { return new TaskMetaRegistry(results, "ru.astrosoup.mineberries"); }

    @Bean TaskCodec taskCodec(TaskMetaRegistry registry, ResultMetaRegistry results) { return new TaskCodec(registry, results, TaskCodec.defaultMapper()); }



    @DependsOnDatabaseInitialization
    @Bean TaskManager taskManager(MessageSender sender, EventSource events, TaskCodec codec,
                                  TaskRepository tasks, ObjectMapper mapper, PlatformTransactionManager transactionManager) {
        return new TaskManager(sender, events, codec, tasks, mapper, transactionManager);
    }
}
