package ru.astrosoup.mineberries.messaging.common.codec;

import static org.junit.jupiter.api.Assertions.*;
import static ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.banker.results.PaymentCollectedResult;
import ru.astrosoup.mineberries.messaging.banker.results.PaymentSentResult;
import ru.astrosoup.mineberries.messaging.banker.results.VerificationCodeDeliveryResult;
import ru.astrosoup.mineberries.messaging.banker.tasks.CollectPayment;
import ru.astrosoup.mineberries.messaging.banker.tasks.SendPayment;
import ru.astrosoup.mineberries.messaging.banker.tasks.SendVerificationCode;
import ru.astrosoup.mineberries.messaging.common.EventType;
import ru.astrosoup.mineberries.messaging.common.TaskError;
import ru.astrosoup.mineberries.messaging.common.TaskEvent;
import ru.astrosoup.mineberries.messaging.common.TaskProgress;
import ru.astrosoup.mineberries.messaging.common.domain.ItemAmount;
import ru.astrosoup.mineberries.messaging.common.domain.WorldPoint;
import ru.astrosoup.mineberries.messaging.common.dtos.MessageDto;
import ru.astrosoup.mineberries.messaging.courier.model.DeliveryOutcome;
import ru.astrosoup.mineberries.messaging.courier.results.DeliveryResult;
import ru.astrosoup.mineberries.messaging.courier.tasks.DeliverOrder;
import ru.astrosoup.mineberries.messaging.supervisor.results.CourierParcelLoadResult;
import ru.astrosoup.mineberries.messaging.supervisor.results.CourierParcelUnloadResult;
import ru.astrosoup.mineberries.messaging.supervisor.results.InventoryReport;
import ru.astrosoup.mineberries.messaging.supervisor.tasks.LoadCourierParcel;
import ru.astrosoup.mineberries.messaging.supervisor.tasks.RunInventory;
import ru.astrosoup.mineberries.messaging.supervisor.tasks.UnloadCourierParcel;
import ru.astrosoup.mineberries.messaging.support.Events;

class TaskCodecTest {
    private static final WorldPoint WAREHOUSE = new WorldPoint("overworld", 0, 64, 0);
    private static final WorldPoint PARCEL = new WorldPoint("overworld", 2, 64, 0);
    private static final WorldPoint DROPOFF = new WorldPoint("overworld", 100, 70, -20);
    private static final ItemAmount DIAMONDS = new ItemAmount(1L, "DIAMOND", 64);

    private final ResultMetaRegistry results = new ResultMetaRegistry(SCAN_PACKAGES);
    private final TaskMetaRegistry tasks = new TaskMetaRegistry(results, SCAN_PACKAGES);
    private final TaskCodec codec = new TaskCodec(tasks, results, TaskCodec.defaultMapper());
    private final UUID id = UUID.randomUUID();

    // ---- encode ----

    @Test
    void encodesTypeIdAndProperties() {
        var task = new SendPayment("seller", new BigDecimal("900.00"));

        MessageDto dto = codec.encode(task);

        assertEquals(task.id(), dto.messageId());
        assertEquals("banker.send_payment", dto.type());
        assertEquals("seller", dto.payload().get("recipientNickname"));
        assertEquals(new BigDecimal("900.00"), new BigDecimal(dto.payload().get("amount").toString()));
    }

    @Test
    void encodesTaskEnvelopeFields() {
        var dto = codec.encode(new SendPayment("seller", new BigDecimal("900.00")));

        assertEquals("task", dto.kind());
        assertEquals(1, dto.schemaVersion());
        assertNull(dto.expiresAt());
    }

    @Test
    void encodesTaskExpiryWhenProvided() {
        var expiry = Instant.parse("2026-11-19T09:02:00Z");

        var dto = codec.encode(new ExpiringTask(expiry));

        assertEquals("expiring", dto.type());
        assertEquals(expiry, dto.expiresAt());
    }

    @Test
    void typeDefaultsToSimpleClassName() {
        assertEquals("PingTask", codec.encode(new PingTask()).type());
    }

    @Test
    void encodesNullPropertyAsNull() {
        var dto = codec.encode(new DeliverOrder(42L, WAREHOUSE, PARCEL, DROPOFF, null));
        assertTrue(dto.payload().containsKey("dropoffContainer"));
        assertNull(dto.payload().get("dropoffContainer"));
    }

    @Test
    void encodesCollectionProperty() {
        var task = new LoadCourierParcel(1L, PARCEL, List.of(DIAMONDS));

        var dto = codec.encode(task);

        assertEquals(1, ((List<?>) dto.payload().get("items")).size());
        assertEquals(itemMap(1L, "DIAMOND", 64), ((List<?>) dto.payload().get("items")).getFirst());
    }

    @Test
    void encodesSuperclassProperties() {
        var dto = codec.encode(new LabelTask("bob", "urgent"));
        assertEquals("owned", dto.type());
        assertEquals(Map.of("owner", "bob", "label", "urgent"), dto.payload());
    }

    @Test
    void encodesBankerTasks() {
        var code = codec.encode(new SendVerificationCode("buyer", "483920"));
        assertEquals("banker.send_verification_code", code.type());
        assertEquals("buyer", code.payload().get("nickname"));
        assertEquals("483920", code.payload().get("code"));
        assertFalse(code.payload().containsKey("action"));

        var collect = codec.encode(new CollectPayment("buyer", new BigDecimal("1000.00"), 120));
        assertEquals("banker.collect_payment", collect.type());
        assertEquals(120, collect.payload().get("timeoutSeconds"));
        assertFalse(collect.payload().containsKey("reference"));

        var send = codec.encode(new SendPayment("seller", new BigDecimal("900.00")));
        assertFalse(send.payload().containsKey("purpose"));
        assertFalse(send.payload().containsKey("reference"));
    }

    @Test
    void encodeRejectsUnregisteredTask() {
        var unregistered = new ru.astrosoup.mineberries.messaging.common.AbstractTask<Void>() {};
        assertFalse(codec.supports(unregistered.getClass()));
        assertThrows(IllegalArgumentException.class, () -> codec.encode(unregistered));
    }

    @Test
    void supportsRegisteredTasksOnly() {
        assertTrue(codec.supports(SendPayment.class));
        assertFalse(codec.supports(PaymentSentResult.class));
    }

    @Test
    void rejectsInvalidTaskPayloadsBeforeSending() {
        assertThrows(IllegalArgumentException.class,
                () -> new SendPayment("seller", new BigDecimal("1.001")));
        assertThrows(IllegalArgumentException.class,
                () -> new DeliverOrder(42L, WAREHOUSE, null, DROPOFF, null));
        assertThrows(IllegalArgumentException.class,
                () -> new DeliverOrder(0L, WAREHOUSE, PARCEL, DROPOFF, null));
    }

    // ---- decode: stored tasks ----

    @Test
    void restoresTheSuppliedIdAndUsesMetadataPropertyNamesAndOrder() {
        var greeting = codec.decodeTask("greet", id, Map.of("count", 3, "name", "buyer"));
        var label = codec.decodeTask("owned", id, Map.of("owner", "bob", "label", "urgent"));
        var ping = codec.decodeTask("PingTask", id, Map.of());

        assertInstanceOf(GreetTask.class, greeting);
        assertEquals(id, greeting.id());
        assertEquals(Map.of("name", "buyer", "count", 3), codec.encode(greeting).payload());
        assertInstanceOf(LabelTask.class, label);
        assertEquals(Map.of("owner", "bob", "label", "urgent"), codec.encode(label).payload());
        assertInstanceOf(PingTask.class, ping);
        assertEquals(id, ping.id());
    }

    @Test
    void decodesInstantAndEnumProperties() {
        var when = Instant.parse("2026-11-19T09:02:00.123456789Z");
        var meta = tasks.require(TimedTask.class);
        var original = new TimedTask(id, when, TimedTask.Mode.STOP);
        var encoded = codec.encode(original);
        var fromStorage = codec.decodeTask("timed", id, Map.of("scheduledAt", when.toString(), "mode", "STOP"));
        var roundTripped = codec.decodeTask(encoded.type(), encoded.messageId(), encoded.payload());

        for (var param : meta.params()) {
            assertEquals(param.get(original), param.get(fromStorage), param.name());
            assertEquals(param.get(original), param.get(roundTripped), param.name());
        }
    }

    @Test
    void acceptsExplicitNullAndIgnoresUnknownFields() {
        var encoded = codec.encode(new DeliverOrder(42L, WAREHOUSE, PARCEL, DROPOFF, null));
        var payload = new HashMap<>(encoded.payload());
        var pickup = new HashMap<>(pointMap(WAREHOUSE));
        pickup.put("extra", "ignored");
        payload.put("pickupPoint", pickup);
        payload.put("extra", "ignored");

        var task = assertInstanceOf(DeliverOrder.class, codec.decodeTask(encoded.type(), id, payload));

        assertEquals(WAREHOUSE, task.getPickupPoint());
        assertNull(task.getDropoffContainer());
    }

    @Test
    void rejectsUnknownStoredTaskType() {
        var ex = assertThrows(IllegalArgumentException.class, () -> codec.decodeTask("not.registered", id, Map.of()));
        assertEquals("Unknown task type: not.registered", ex.getMessage());
    }

    @Test
    void rejectsMissingTaskPropertyEvenIfItCanBeNull() {
        var encoded = codec.encode(new DeliverOrder(42L, WAREHOUSE, PARCEL, DROPOFF, null));
        var payload = new HashMap<>(encoded.payload());
        payload.remove("dropoffContainer");

        var ex = assertThrows(IllegalArgumentException.class, () -> codec.decodeTask(encoded.type(), id, payload));
        assertEquals("Missing property \"dropoffContainer\" for task courier.deliver_order", ex.getMessage());
    }

    @Test
    void rejectsAnUnconvertiblePropertyWithoutExposingItsValue() {
        var ex = assertThrows(IllegalArgumentException.class, () -> codec.decodeTask("banker.send_payment", id,
                Map.of("recipientNickname", "seller", "amount", "not-a-number")));

        assertEquals("Invalid property \"amount\" for task banker.send_payment", ex.getMessage());
        assertNull(ex.getCause());
    }

    @Test
    void rejectsPayloadThatFailsTaskConstructorValidation() {
        var ex = assertThrows(IllegalArgumentException.class, () -> codec.decodeTask("banker.send_payment", id,
                Map.of("recipientNickname", "seller", "amount", new BigDecimal("1.001"))));

        assertEquals("Invalid payload for task banker.send_payment: constructor validation failed", ex.getMessage());
        assertNull(ex.getCause());
    }

    // ---- decode: non-result events ----

    @Test
    void decodesAcceptedWithBotName() {
        var ev = codec.decode(SendPayment.class, Events.accepted(id, 1, "courier_andy"));
        assertEquals(new TaskEvent.Accepted<Object>(1, "courier_andy"), ev);
    }

    @Test
    void decodesCanceled() {
        var ev = codec.decode(SendPayment.class, Events.canceled(id, 4));
        assertEquals(new TaskEvent.Cancelled<Object>(4), ev);
    }

    @Test
    void decodesProgress() {
        var ev = codec.decode(SendPayment.class, Events.progress(id, 2, "packing"));
        assertEquals(new TaskEvent.Progress<Object>(2, new TaskProgress("packing")), ev);
    }

    @Test
    void decodesFailed() {
        var ev = codec.decode(SendPayment.class, Events.failed(id, 5, "boom", true, false));
        assertEquals(new TaskEvent.Failed<Object>(5, new TaskError("boom", true, false)), ev);
    }

    @Test
    void decodesFailedWithUnknownOutcome() {
        var ev = codec.decode(SendPayment.class, Events.failed(id, 5, "payment outcome unknown", false, true));
        assertEquals(new TaskEvent.Failed<Object>(5, new TaskError("payment outcome unknown", false, true)), ev);
    }

    @Test
    void failedWithoutRetryableOrOutcomeUnknownFlagsDefaultsToFalse() {
        var dto = Events.of(id, 5, EventType.FAILED, Map.of("message", "boom"));
        var ev = codec.decode(SendPayment.class, dto);
        assertEquals(new TaskEvent.Failed<Object>(5, new TaskError("boom", false, false)), ev);
    }

    @Test
    void acceptedWithoutPayloadHasNoBotName() {
        assertEquals(new TaskEvent.Accepted<Object>(0, null),
                codec.decode(SendPayment.class, Events.of(id, 0, EventType.ACCEPTED, null)));
    }

    // ---- decode: COMPLETED / @Result ----

    @Test
    void decodesCompletedIntoResultObject() {
        var dto = Events.completed(id, 3, paymentPayload("seller", "900.00", "2026-11-18T10:17:00Z"));

        var completed = assertInstanceOf(TaskEvent.Completed.class, codec.decode(SendPayment.class, dto));

        PaymentSentResult r = assertInstanceOf(PaymentSentResult.class, completed.result());
        assertEquals("seller", r.recipientNickname());
        assertEquals(new BigDecimal("900.00"), r.amount());
        assertEquals(Instant.parse("2026-11-18T10:17:00Z"), r.paidAt());
        assertEquals(3, completed.version());
    }

    @Test
    void decodesBankerVerificationCodeAndCollectionResults() {
        var code = assertInstanceOf(VerificationCodeDeliveryResult.class,
                completed(SendVerificationCode.class, Map.of(
                        "nickname", "buyer",
                        "deliveredAt", "2026-11-18T10:15:31Z")));
        assertEquals("buyer", code.nickname());
        assertEquals(Instant.parse("2026-11-18T10:15:31Z"), code.deliveredAt());

        var collected = assertInstanceOf(PaymentCollectedResult.class,
                completed(CollectPayment.class, Map.of(
                        "payerNickname", "buyer",
                        "amount", new BigDecimal("1000.00"),
                        "paidAt", "2026-11-18T10:16:00Z")));
        assertEquals(new BigDecimal("1000.00"), collected.amount());
    }

    @Test
    void usesPropertyNameAndConvertsGenericTypes() {
        var discrepancy = new HashMap<String, Object>();
        discrepancy.put("itemId", 1L);
        discrepancy.put("material", "DIAMOND");
        discrepancy.put("expectedQuantity", 64);
        discrepancy.put("actualQuantity", 60);

        var dto = Events.completed(id, 3, Map.of(
                "warehouseId", 1L,
                "actualItems", List.of(itemMap(1L, "DIAMOND", 60)),
                "discrepancies", List.of(discrepancy),
                "completedAt", "2026-11-18T10:20:00Z"));

        var completed = assertInstanceOf(TaskEvent.Completed.class, codec.decode(RunInventory.class, dto));

        InventoryReport r = assertInstanceOf(InventoryReport.class, completed.result());
        assertEquals(60, r.actualItems().getFirst().quantity());
        assertEquals(64, r.discrepancies().getFirst().expectedQuantity());
        assertEquals(60, r.discrepancies().getFirst().actualQuantity());
    }

    @Test
    void inventoryAcceptsItemsMissingFromTheCatalog() {
        var discrepancy = new HashMap<String, Object>();
        discrepancy.put("itemId", null);
        discrepancy.put("material", "DIRT");
        discrepancy.put("expectedQuantity", 0);
        discrepancy.put("actualQuantity", 3);

        var result = assertInstanceOf(InventoryReport.class, completed(RunInventory.class, Map.of(
                "warehouseId", 1L,
                "actualItems", List.of(itemMap(null, "DIRT", 3)),
                "discrepancies", List.of(discrepancy),
                "completedAt", "2026-11-18T10:21:00Z")));

        assertNull(result.actualItems().getFirst().itemId());
        assertNull(result.discrepancies().getFirst().itemId());
    }

    @Test
    void decodesParcelResults() {
        var loaded = assertInstanceOf(CourierParcelLoadResult.class,
                completed(LoadCourierParcel.class, Map.of(
                        "parcelPoint", pointMap(PARCEL),
                        "loadedItems", List.of(itemMap(1L, "DIAMOND", 32)),
                        "missingItems", List.of(itemMap(1L, "DIAMOND", 32)),
                        "loadedAt", "2026-11-18T10:18:00Z")));
        assertEquals(32, loaded.loadedItems().getFirst().quantity());
        assertEquals(32, loaded.missingItems().getFirst().quantity());

        var unloaded = assertInstanceOf(CourierParcelUnloadResult.class,
                completed(UnloadCourierParcel.class, Map.of(
                        "parcelPoint", pointMap(PARCEL),
                        "unloadedItems", List.of(itemMap(1L, "DIAMOND", 48)),
                        "remainingItems", List.of(itemMap(1L, "DIAMOND", 16)),
                        "unloadedAt", "2026-11-18T10:19:00Z")));
        assertEquals(48, unloaded.unloadedItems().getFirst().quantity());
        assertEquals(16, unloaded.remainingItems().getFirst().quantity());
    }

    @Test
    void decodesContainerDeliveryAndReportsMovedItems() {
        var payload = new HashMap<String, Object>();
        payload.put("orderId", 42L);
        payload.put("outcome", "DELIVERED");
        payload.put("dropoffContainer", pointMap(new WorldPoint("overworld", 101, 70, -20)));
        payload.put("usedFallbackContainer", true);
        payload.put("movedItems", List.of(itemMap(1L, "DIAMOND", 64)));
        payload.put("finishedAt", "2026-11-18T10:22:00Z");

        var result = assertInstanceOf(DeliveryResult.class,
                completed(DeliverOrder.class, payload));

        assertEquals(DeliveryOutcome.DELIVERED, result.outcome());
        assertTrue(result.usedFallbackContainer());
        assertEquals(64, result.movedItems().getFirst().quantity());
    }

    @Test
    void reportsCargoLostInAnAccident() {
        var payload = new HashMap<String, Object>();
        payload.put("orderId", 42L);
        payload.put("outcome", "CARGO_LOST");
        payload.put("dropoffContainer", null);
        payload.put("usedFallbackContainer", false);
        payload.put("movedItems", List.of());
        payload.put("finishedAt", "2026-11-18T10:22:00Z");

        var result = assertInstanceOf(DeliveryResult.class,
                completed(DeliverOrder.class, payload));

        assertEquals(DeliveryOutcome.CARGO_LOST, result.outcome());
        assertNull(result.dropoffContainer());
        assertTrue(result.movedItems().isEmpty());
    }

    @Test
    void ignoresPayloadKeysThatAreNotProperties() {
        var payload = new HashMap<>(paymentPayload("seller", "900.00", "2026-11-18T10:17:00Z"));
        payload.put("extra", "x");

        var completed = assertInstanceOf(TaskEvent.Completed.class,
                codec.decode(SendPayment.class, Events.completed(id, 3, payload)));
        assertInstanceOf(PaymentSentResult.class, completed.result());
    }

    @Test
    void explicitNullValueIsAccepted() {
        var payload = new HashMap<String, Object>();
        payload.put("orderId", 42L);
        payload.put("outcome", "DROPOFF_BLOCKED");
        payload.put("dropoffContainer", null);
        payload.put("usedFallbackContainer", false);
        payload.put("movedItems", List.of());
        payload.put("finishedAt", "2026-11-18T10:22:00Z");

        var completed = assertInstanceOf(TaskEvent.Completed.class,
                codec.decode(DeliverOrder.class, Events.completed(id, 1, payload)));

        assertNull(((DeliveryResult) completed.result()).dropoffContainer());
    }

    @Test
    void missingPropertyKeyFails() {
        var dto = Events.completed(id, 3, Map.of("recipientNickname", "seller"));
        var ex = assertThrows(IllegalStateException.class, () -> codec.decode(SendPayment.class, dto));
        assertTrue(ex.getMessage().contains("amount"), ex.getMessage());
    }

    @Test
    void completedWithoutPayloadFailsForNonVoidResult() {
        var dto = Events.of(id, 3, EventType.COMPLETED, null);
        assertThrows(IllegalStateException.class, () -> codec.decode(SendPayment.class, dto));
    }

    @Test
    void completedOfVoidTaskHasNullResult() {
        var completed = assertInstanceOf(TaskEvent.Completed.class,
                codec.decode(PingTask.class, Events.of(id, 1, EventType.COMPLETED, null)));
        assertNull(completed.result());
    }

    @Test
    void resultWithoutPropertiesDecodesFromEmptyPayload() {
        var completed = assertInstanceOf(TaskEvent.Completed.class,
                codec.decode(AckTask.class, Events.completed(id, 1, Map.of())));
        assertInstanceOf(AckResult.class, completed.result());
    }

    @Test
    void decodeRejectsUnregisteredTaskClass() {
        assertThrows(IllegalArgumentException.class,
                () -> codec.decode(PaymentSentResult.class, Events.accepted(id, 0, "courier_andy")));
    }

    private Object completed(Class<?> taskClass, Map<String, Object> payload) {
        var event = codec.decode(taskClass, Events.completed(id, 1, payload));
        return assertInstanceOf(TaskEvent.Completed.class, event).result();
    }

    private static Map<String, Object> paymentPayload(String recipient, String amount, String paidAt) {
        return Map.of(
                "recipientNickname", recipient,
                "amount", new BigDecimal(amount),
                "paidAt", paidAt);
    }

    private static Map<String, Object> pointMap(WorldPoint point) {
        return Map.of("world", point.world(), "x", point.x(), "y", point.y(), "z", point.z());
    }

    private static Map<String, Object> itemMap(Long itemId, String material, int quantity) {
        var map = new HashMap<String, Object>();
        map.put("itemId", itemId);
        map.put("material", material);
        map.put("quantity", quantity);
        return map;
    }
}
