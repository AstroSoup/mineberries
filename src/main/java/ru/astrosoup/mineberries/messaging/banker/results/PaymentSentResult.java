package ru.astrosoup.mineberries.messaging.banker.results;

import java.math.BigDecimal;
import java.time.Instant;

import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Result;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;

/** Confirms that the service paid a player. */
@Result("banker.send_payment.result")
public record PaymentSentResult(
        @Property String recipientNickname,
        @Property BigDecimal amount,
        @Property Instant paidAt) {

    public PaymentSentResult {
        recipientNickname = DomainPreconditions.nickname(recipientNickname, "recipientNickname");
        amount = DomainPreconditions.amount(amount, "amount");
        paidAt = DomainPreconditions.nonNull(paidAt, "paidAt");
    }
}
