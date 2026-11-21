package ru.astrosoup.mineberries.messaging.common;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.springframework.context.ApplicationEventPublisher;

import ru.astrosoup.mineberries.messaging.banker.results.PaymentCollectedResult;
import ru.astrosoup.mineberries.messaging.banker.results.VerificationCodeDeliveryResult;
import ru.astrosoup.mineberries.messaging.banker.tasks.CollectPayment;
import ru.astrosoup.mineberries.messaging.banker.tasks.SendVerificationCode;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;
import ru.astrosoup.mineberries.messaging.tasks.TaskRecord;

/** Deadline rules for the two timed task types only. */
record TaskDeadlines(Clock clock, Duration codeBuffer, Duration paymentBuffer, Duration cancelGrace,
                     ApplicationEventPublisher events) {
    static final String CODE_TYPE = "banker.send_verification_code";
    static final String PAYMENT_TYPE = "banker.collect_payment";

    TaskDeadlines {
        if (codeBuffer.isNegative() || paymentBuffer.isNegative() || cancelGrace.isNegative()) {
            throw new IllegalArgumentException("Deadline buffers and cancellation grace must not be negative");
        }
    }

    static TaskDeadlines defaults() {
        return new TaskDeadlines(Clock.systemUTC(), Duration.ofMinutes(2), Duration.ofMinutes(5),
                Duration.ofMinutes(2), ignored -> {});
    }

    Instant now() {
        // PostgreSQL timestamps have microsecond precision.
        return clock.instant().truncatedTo(ChronoUnit.MICROS);
    }

    Submission atSubmission(AbstractTask<?> task, MessageDto dto) {
        if (task instanceof SendVerificationCode) {
            var deadline = now().plus(Duration.ofMinutes(2));
            return new Submission(withExpiry(dto, deadline), deadline, null);
        }
        if (task instanceof CollectPayment payment) {
            if (payment.getTimeoutSeconds() != CollectPayment.PAYMENT_WINDOW_SECONDS) {
                throw new IllegalArgumentException("CollectPayment must use a 15-minute timeout");
            }
            var deadline = now().plusSeconds(payment.getTimeoutSeconds());
            return new Submission(withExpiry(dto, deadline), null, deadline);
        }
        return new Submission(dto, dto.expiresAt(), task.finishBy());
    }

    LateTaskCompletion lateCompletion(TaskRecord task, TaskEvent<Object> event) {
        if (!(event instanceof TaskEvent.Completed<Object> completed)) return null;
        var deadline = storedDeadline(task);
        if (deadline == null) return null;

        Instant resultAt = switch (completed.result()) {
            case VerificationCodeDeliveryResult code -> code.deliveredAt();
            case PaymentCollectedResult payment -> payment.paidAt();
            case null, default -> null;
        };
        var limit = deadline.plus(buffer(task));
        return resultAt != null && resultAt.isAfter(limit)
                ? new LateTaskCompletion(task.taskId(), task.type(), deadline, limit, resultAt)
                : null;
    }

    Instant bufferedDeadline(TaskRecord task) {
        var deadline = storedDeadline(task);
        return deadline == null ? null : deadline.plus(buffer(task));
    }

    boolean cancelGraceElapsed(TaskRecord task, Instant bufferedDeadline, Instant now) {
        var start = laterOf(bufferedDeadline, task.cancelRequestedAt());
        if (task.lastEventAt() != null) start = laterOf(start, task.lastEventAt());
        return !now.isBefore(start.plus(cancelGrace));
    }

    private static Instant laterOf(Instant first, Instant second) {
        return first.isAfter(second) ? first : second;
    }

    private static Instant storedDeadline(TaskRecord task) {
        return switch (task.type()) {
            case CODE_TYPE -> task.expiresAt();
            case PAYMENT_TYPE -> task.finishBy();
            default -> null;
        };
    }

    private Duration buffer(TaskRecord task) {
        return CODE_TYPE.equals(task.type()) ? codeBuffer : paymentBuffer;
    }

    private static MessageDto withExpiry(MessageDto dto, Instant deadline) {
        return new MessageDto(dto.messageId(), dto.kind(), dto.type(), dto.schemaVersion(), deadline, dto.payload());
    }

    record Submission(MessageDto message, Instant expiresAt, Instant finishBy) {}
}
