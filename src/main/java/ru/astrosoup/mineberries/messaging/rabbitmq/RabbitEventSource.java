package ru.astrosoup.mineberries.messaging.rabbitmq;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import ru.astrosoup.mineberries.messaging.common.broker.EventSource;
import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;
import tools.jackson.databind.ObjectMapper;

/** Delivers validated RabbitMQ events to subscribers inside a database transaction. */
@Slf4j
public class RabbitEventSource implements EventSource {
    private final EventMessageDecoder decoder;
    private final TransactionTemplate transaction;
    private final CopyOnWriteArrayList<Consumer<EventDto>> handlers = new CopyOnWriteArrayList<>();

    RabbitEventSource(ObjectMapper mapper, PlatformTransactionManager transactionManager) {
        decoder = new EventMessageDecoder(mapper);
        transaction = new TransactionTemplate(transactionManager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Override
    public void subscribe(Consumer<EventDto> handler) {
        handlers.add(Objects.requireNonNull(handler, "handler"));
    }

    void receive(Message delivery, Channel channel) throws IOException {
        long tag = delivery.getMessageProperties().getDeliveryTag();
        EventDto event;
        try {
            event = decoder.decode(delivery.getBody());
        } catch (EventMessageDecoder.InvalidEventException e) {
            log.warn("Rejecting invalid task event (delivery tag {})", tag);
            channel.basicReject(tag, false);
            return;
        }

        if (handleAndCommit(event)) {
            channel.basicAck(tag, false);
        } else {
            log.warn("Requeuing uncommitted task event: message_id={}, version={}", event.messageId(), event.version());
            // With RabbitMQ 4.3+, unlike basicNack, this counts toward the quorum delivery limit.
            channel.basicReject(tag, true);
        }
    }

    private boolean handleAndCommit(EventDto event) {
        var committed = new AtomicBoolean();
        try {
            transaction.executeWithoutResult(status -> {
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        committed.set(true);
                    }
                });
                if (handlers.isEmpty()) throw new IllegalStateException("No task event handler registered");
                handlers.forEach(handler -> handler.accept(event));
            });
        } catch (RuntimeException e) {
            // Don't log arbitrary handler, database, or transaction exception contents.
            return false;
        }
        return committed.get();
    }
}
