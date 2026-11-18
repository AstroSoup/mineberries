package ru.astrosoup.mineberries.messaging.banker.results;

import java.math.BigDecimal;
import java.time.Instant;

import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Result;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;

/** Confirms that a player's payment reached the service account. */
@Result("banker.collect_payment.result")
public record PaymentCollectedResult(
        @Property String payerNickname,
        @Property BigDecimal amount,
        @Property Instant paidAt) {

    public PaymentCollectedResult {
        payerNickname = DomainPreconditions.nickname(payerNickname, "payerNickname");
        amount = DomainPreconditions.amount(amount, "amount");
        paidAt = DomainPreconditions.nonNull(paidAt, "paidAt");
    }
}
