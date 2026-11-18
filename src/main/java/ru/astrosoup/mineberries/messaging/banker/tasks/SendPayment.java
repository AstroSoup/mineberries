package ru.astrosoup.mineberries.messaging.banker.tasks;

import java.math.BigDecimal;

import lombok.Getter;
import ru.astrosoup.mineberries.messaging.banker.results.PaymentSentResult;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Task;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;

/**
 * Pays a player from the service account.
 *
 * <p>The task carries only the transfer itself. The reason for the payment and
 * any related business record are tracked by the service.
 */
@Task("banker.send_payment")
@Getter
public class SendPayment extends AbstractTask<PaymentSentResult> {
    @Property private final String recipientNickname;
    @Property private final BigDecimal amount;

    public SendPayment(String recipientNickname, BigDecimal amount) {
        this.recipientNickname = DomainPreconditions.nickname(recipientNickname, "recipientNickname");
        this.amount = DomainPreconditions.amount(amount, "amount");
    }
}
