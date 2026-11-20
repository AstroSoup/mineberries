package ru.astrosoup.mineberries.messaging.common.broker;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;

/** Enqueues encoded task and cancel messages for delivery to a bot. */
public interface MessageSender {
    /**
     * Enqueues one message. For a transactional outbox the future completes when
     * the row is written, not when the transaction commits or the broker confirms
     * publication.
     */
    CompletableFuture<Void> send(MessageDto dto);

    /** Enqueues a cancel message using the original task ID. */
    default CompletableFuture<Void> sendCancel(UUID taskId, int schemaVersion) {
        return send(new MessageDto(taskId, "cancel", null, schemaVersion, null, null));
    }
}
