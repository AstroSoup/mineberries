package ru.astrosoup.mineberries.messaging.common;



import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import ru.astrosoup.mineberries.messaging.common.brocker.EventSource;
import ru.astrosoup.mineberries.messaging.common.brocker.MessageSender;
import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.common.codec.TaskMetaRegistry;

@Configuration
public class MessagingConfig {
    @Bean TaskMetaRegistry taskMetaRegistry() { return new TaskMetaRegistry("ru.astrosoup.mineberries"); }

    @Bean TaskCodec taskCodec(TaskMetaRegistry registry) { return new TaskCodec(registry, TaskCodec.defaultMapper()); }



    @Bean TaskManager taskManager(MessageSender sender, EventSource events, TaskCodec codec) {
        return new TaskManager(sender, events, codec);
    }
}