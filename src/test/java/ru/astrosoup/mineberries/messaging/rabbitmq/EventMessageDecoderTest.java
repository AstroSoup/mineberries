package ru.astrosoup.mineberries.messaging.rabbitmq;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import ru.astrosoup.mineberries.messaging.common.EventType;
import ru.astrosoup.mineberries.messaging.common.codec.TaskCodec;

class EventMessageDecoderTest {
    private final EventMessageDecoder decoder = new EventMessageDecoder(TaskCodec.defaultMapper());

    @ParameterizedTest
    @ValueSource(strings = {
            "banker.send_verification_code", "banker.send_payment", "banker.collect_payment",
            "courier.deliver_order", "supervisor.run_inventory",
            "supervisor.load_courier_parcel", "supervisor.unload_courier_parcel"
    })
    void validatesEveryCompletedContractFixture(String taskType) throws Exception {
        try (var fixture = getClass().getResourceAsStream("/contract/event.completed." + taskType + ".json")) {
            assertNotNull(fixture);
            assertEquals(EventType.COMPLETED, decoder.decode(fixture.readAllBytes()).type());
        }
    }

    @Test
    void acceptsUnknownFieldsAndPreservesDecimalAmounts() {
        var id = UUID.randomUUID();
        var event = decoder.decode(bytes("""
                {
                  "messageId": "%s", "version": 2, "type": "COMPLETED",
                  "occurredAt": "2026-11-21T12:00:00Z", "additional": true,
                  "payload": {
                    "recipientNickname": "seller", "amount": 9999999999.99, "paidAt": "2026-11-21T12:00:00Z"
                  }
                }
                """.formatted(id)));

        assertEquals(id, event.messageId());
        assertEquals(2, event.version());
        assertEquals(new BigDecimal("9999999999.99"), event.payload().get("amount"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"version\":-1}",
            "{\"version\":1.5}",
            "{\"version\":9223372036854775808}",
            "{\"version\":\"0\"}",
            "{\"messageId\":\"1-1-1-1-1\"}",
            "{\"occurredAt\":\"2026-02-30T12:00:00Z\"}",
            "{\"type\":\"UNKNOWN\"}",
            "{\"payload\":null}",
            "{\"payload\":{}}",
            "{\"type\":\"COMPLETED\",\"payload\":{\"orderId\":1}}",
            "{\"type\":\"FAILED\",\"payload\":{\"message\":\"Unknown outcome\",\"retryable\":true,\"outcomeUnknown\":true}}"
    })
    void rejectsInvalidEventFields(String change) {
        var mapper = TaskCodec.defaultMapper();
        var body = mapper.createObjectNode()
                .put("messageId", UUID.randomUUID().toString())
                .put("version", 0)
                .put("type", "ACCEPTED")
                .put("occurredAt", "2026-11-21T12:00:00Z");
        body.putPOJO("payload", Map.of("bot", "banker_bot"));
        body.setAll(mapper.readTree(change).asObject());
        assertThrows(EventMessageDecoder.InvalidEventException.class, () -> decoder.decode(mapper.writeValueAsBytes(body)));
    }

    @Test
    void rejectsTrailingJsonInsteadOfProcessingOnlyTheFirstObject() {
        assertThrows(EventMessageDecoder.InvalidEventException.class, () -> decoder.decode(bytes("""
                {"messageId":"6a7b8c9d-1e2f-4a3b-9c4d-5e6f7a8b9c1d","version":0,"type":"CANCELED",
                 "occurredAt":"2026-11-21T12:00:00Z","payload":{}} {}
                """)));
    }

    private static byte[] bytes(String body) {
        return body.getBytes(StandardCharsets.UTF_8);
    }
}
