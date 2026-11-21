package ru.astrosoup.mineberries.messaging.banker.tasks;

import java.math.BigDecimal;
import java.util.UUID;

import lombok.Getter;
import ru.astrosoup.mineberries.messaging.banker.results.PaymentCollectedResult;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Task;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;

/**
 * Collects a payment from a player into the service account.
 *
 * <p>The payer is asked for the amount and has up to {@code timeoutSeconds} to
 * complete the transfer. The business operation behind the payment is tracked
 * by the service.
 */
@Task("banker.collect_payment")
@Getter
public class CollectPayment extends AbstractTask<PaymentCollectedResult> {
    public static final int PAYMENT_WINDOW_SECONDS = 15 * 60;

    @Property private final String payerNickname;
    @Property private final BigDecimal amount;
    @Property private final int timeoutSeconds;

    public CollectPayment(String payerNickname, BigDecimal amount) {
        this(payerNickname, amount, PAYMENT_WINDOW_SECONDS);
    }

    public CollectPayment(String payerNickname, BigDecimal amount, int timeoutSeconds) {
        this(UUID.randomUUID(), payerNickname, amount, timeoutSeconds);
    }

    public CollectPayment(UUID id, String payerNickname, BigDecimal amount, int timeoutSeconds) {
        super(id);
        this.payerNickname = DomainPreconditions.nickname(payerNickname, "payerNickname");
        this.amount = DomainPreconditions.amount(amount, "amount");
        this.timeoutSeconds = DomainPreconditions.positive(timeoutSeconds, "timeoutSeconds");
    }
}
