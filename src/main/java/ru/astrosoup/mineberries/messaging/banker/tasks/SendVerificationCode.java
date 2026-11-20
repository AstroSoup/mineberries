package ru.astrosoup.mineberries.messaging.banker.tasks;

import java.util.UUID;

import lombok.Getter;
import ru.astrosoup.mineberries.messaging.banker.results.VerificationCodeDeliveryResult;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Task;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;

/**
 * Delivers a one-time confirmation code to a player through the game server.
 *
 * <p>The service owns code generation, purpose, validation, and expiry. The
 * task carries only the recipient and the code itself.
 */
@Task("banker.send_verification_code")
@Getter
public class SendVerificationCode extends AbstractTask<VerificationCodeDeliveryResult> {
    @Property private final String nickname;
    @Property private final String code;

    public SendVerificationCode(String nickname, String code) {
        this(UUID.randomUUID(), nickname, code);
    }

    public SendVerificationCode(UUID id, String nickname, String code) {
        super(id);
        this.nickname = DomainPreconditions.nickname(nickname, "nickname");
        this.code = DomainPreconditions.text(code, "code");
    }
}
