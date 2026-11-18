package ru.astrosoup.mineberries.messaging.common.brocker;

import java.util.concurrent.CompletableFuture;

import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;

/** Sends encoded task messages to a bot through the broker. */
public interface MessageSender {
    /** Sends one message; the returned future completes when it is accepted. */
    CompletableFuture<Void> send(MessageDto dto);
}