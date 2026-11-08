package ru.astrosoup.mineberries.messaging.common.brocker;

import java.util.concurrent.CompletableFuture;

import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;

public interface MessageSender {
    CompletableFuture<Void> send(MessageDto dto);
} 