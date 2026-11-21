package ru.astrosoup.mineberries.messaging.common;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.ApplicationEventPublisher;
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
    @Bean Clock taskClock() { return Clock.systemUTC(); }

    @Bean TaskDeadlines taskDeadlines(Clock taskClock, ApplicationEventPublisher events,
                                     @Value("${mineberries.messaging.deadlines.code-buffer:2m}") String codeBuffer,
                                     @Value("${mineberries.messaging.deadlines.payment-buffer:5m}") String paymentBuffer,
                                     @Value("${mineberries.messaging.deadlines.cancel-grace:2m}") String cancelGrace) {
        return new TaskDeadlines(taskClock, DurationStyle.detectAndParse(codeBuffer),
                DurationStyle.detectAndParse(paymentBuffer), DurationStyle.detectAndParse(cancelGrace), events);
    }

    @DependsOnDatabaseInitialization
    @Bean TaskManager taskManager(MessageSender sender, EventSource events, TaskCodec codec,
                                  TaskRepository tasks, ObjectMapper mapper, PlatformTransactionManager transactionManager,
                                  @Value("${mineberries.messaging.continuation-grace:5m}") String continuationGrace,
                                  TaskDeadlines deadlines) {
        return new TaskManager(sender, events, codec, tasks, mapper, transactionManager,
                DurationStyle.detectAndParse(continuationGrace), deadlines);
    }
}
