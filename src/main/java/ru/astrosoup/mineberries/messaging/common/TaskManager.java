package ru.astrosoup.mineberries.messaging.common;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;
import ru.astrosoup.mineberries.messaging.common.brocker.*;
import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;
import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;
@Slf4j 
public class TaskManager {

    private final MessageSender sender;
    private final TaskCodec codec;
    private final Map<UUID, AbstractTask<?>> active = new ConcurrentHashMap<>();

    public TaskManager(MessageSender sender, EventSource events, TaskCodec codec) {
        this.sender = sender;
        this.codec = codec;
        events.subscribe(this::onEvent);
    }

    public <T extends AbstractTask<?>> T submit(T task) {
        if (!codec.supports(task.getClass()))
            throw new IllegalArgumentException("Unregistered task class: " + task.getClass().getName());

        MessageDto dto = codec.encode(task);
        if (active.putIfAbsent(task.id(), task) != null)
            throw new IllegalStateException("Task already submitted: " + task.id());

        task.result().whenComplete((r, ex) -> {
            active.remove(task.id());
        });

        sender.send(dto).whenComplete((v, ex) -> {
            if (ex != null) task.failLocally(ex);
        });
        return task;
    }
    
    @SuppressWarnings("unchecked")
    private void onEvent(EventDto dto) {
        AbstractTask<Object> task = (AbstractTask<Object>) active.get(dto.messageId());
        if (task == null) {
            log.debug("Event for unknown/finished task {}: {}", dto.messageId(), dto.type());
            return;
        }
        try {
            task.apply(codec.decode(task.getClass(), dto));
        } catch (RuntimeException e) {
            log.error("Can't process event {} for task {}", dto.type(), dto.messageId(), e);
            task.failLocally(e);
        }
    }
}