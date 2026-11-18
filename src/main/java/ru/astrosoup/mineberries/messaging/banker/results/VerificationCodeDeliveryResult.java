package ru.astrosoup.mineberries.messaging.banker.results;

import java.time.Instant;

import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Result;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;

/** Confirms that the banker delivered a verification code. */
@Result("banker.send_verification_code.result")
public record VerificationCodeDeliveryResult(
        @Property String nickname,
        @Property Instant deliveredAt) {

    public VerificationCodeDeliveryResult {
        nickname = DomainPreconditions.nickname(nickname, "nickname");
        deliveredAt = DomainPreconditions.nonNull(deliveredAt, "deliveredAt");
    }
}
