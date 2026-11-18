package ru.astrosoup.mineberries.messaging.common.codec;

import static org.junit.jupiter.api.Assertions.*;
import static ru.astrosoup.mineberries.messaging.fixtures.MessagingFixtures.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import ru.astrosoup.mineberries.messaging.banker.results.PaymentCollectedResult;
import ru.astrosoup.mineberries.messaging.banker.results.PaymentSentResult;
import ru.astrosoup.mineberries.messaging.banker.results.VerificationCodeDeliveryResult;
import ru.astrosoup.mineberries.messaging.banker.tasks.CollectPayment;
import ru.astrosoup.mineberries.messaging.banker.tasks.SendPayment;
import ru.astrosoup.mineberries.messaging.banker.tasks.SendVerificationCode;
import ru.astrosoup.mineberries.messaging.courier.results.DeliveryResult;
import ru.astrosoup.mineberries.messaging.courier.tasks.DeliverOrder;
import ru.astrosoup.mineberries.messaging.supervisor.results.CourierParcelLoadResult;
import ru.astrosoup.mineberries.messaging.supervisor.results.CourierParcelUnloadResult;
import ru.astrosoup.mineberries.messaging.supervisor.results.InventoryReport;
import ru.astrosoup.mineberries.messaging.supervisor.tasks.LoadCourierParcel;
import ru.astrosoup.mineberries.messaging.supervisor.tasks.RunInventory;
import ru.astrosoup.mineberries.messaging.supervisor.tasks.UnloadCourierParcel;

class MetaRegistryTest {
    private final ResultMetaRegistry results = new ResultMetaRegistry(PACKAGE);
    private final TaskMetaRegistry tasks = new TaskMetaRegistry(results, PACKAGE);
    private final ResultMetaRegistry appResults = new ResultMetaRegistry(SCAN_PACKAGES);
    private final TaskMetaRegistry appTasks = new TaskMetaRegistry(appResults, SCAN_PACKAGES);

    private static List<String> names(List<TaskMeta.Param> params) {
        return params.stream().map(TaskMeta.Param::name).toList();
    }

    // ---- results ----

    @Test
    void resultNameDefaultsToSimpleClassName() {
        assertEquals("GreetResult", results.require(GreetResult.class).type());
    }

    @Test
    void resultNameCanBeOverridden() {
        assertEquals("Totals", results.require(TotalsResult.class).type());
    }

    @Test
    void resultPropertiesFollowDeclarationOrderAndUseCustomNames() {
        assertEquals(List.of("numbers", "sum"), names(results.require(TotalsResult.class).params()));
        assertEquals(List.of("greeting", "length"), names(results.require(GreetResult.class).params()));
    }

    @Test
    void resultWithoutPropertiesIsRegistered() {
        assertTrue(results.require(AckResult.class).params().isEmpty());
    }

    @Test
    void onlyAnnotatedResultClassesAreRegistered() {
        assertTrue(results.find(GreetTask.class).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> results.require(GreetTask.class));
        assertEquals(3, results.all().size()); // Greet, Totals, Ack
    }

    // ---- tasks ----

    @Test
    void taskNameDefaultsToSimpleClassNameOrUsesValue() {
        assertEquals("greet", tasks.require(GreetTask.class).type());
        assertEquals("PingTask", tasks.require(PingTask.class).type());
    }

    @Test
    void taskResultClassComesFromGenericParameter() {
        assertEquals(GreetResult.class, tasks.require(GreetTask.class).resultClass());
        assertEquals(Void.class, tasks.require(PingTask.class).resultClass());
        assertEquals(Void.class, tasks.require(LabelTask.class).resultClass()); // via generic superclass
    }

    @Test
    void taskPropertiesIncludeSuperclassAndCustomNames() {
        assertEquals(List.of("name", "count"), names(tasks.require(GreetTask.class).params()));
        assertTrue(names(tasks.require(LabelTask.class).params()).containsAll(List.of("owner", "label")));
        assertEquals(2, tasks.require(LabelTask.class).params().size());
    }

    @Test
    void unknownTaskClass() {
        assertTrue(tasks.find(GreetResult.class).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> tasks.require(GreetResult.class));
    }

    @Test
    void allListsEveryTask() {
        assertEquals(4, tasks.all().size()); // Greet, Ping, Ack, Label
    }

    // ---- application tasks ----

    @Test
    void applicationTasksAreRegisteredWithTheirContracts() {
        assertTask(SendVerificationCode.class, "banker.send_verification_code",
                VerificationCodeDeliveryResult.class, List.of("nickname", "code"));
        assertTask(CollectPayment.class, "banker.collect_payment",
                PaymentCollectedResult.class, List.of("payerNickname", "amount", "timeoutSeconds"));
        assertTask(SendPayment.class, "banker.send_payment",
                PaymentSentResult.class, List.of("recipientNickname", "amount"));
        assertTask(LoadCourierParcel.class, "supervisor.load_courier_parcel",
                CourierParcelLoadResult.class,
                List.of("warehouseId", "parcelPoint", "items"));
        assertTask(UnloadCourierParcel.class, "supervisor.unload_courier_parcel",
                CourierParcelUnloadResult.class,
                List.of("warehouseId", "parcelPoint"));
        assertTask(RunInventory.class, "supervisor.run_inventory",
                InventoryReport.class, List.of("warehouseId", "expectedItems"));
        assertTask(DeliverOrder.class, "courier.deliver_order",
                DeliveryResult.class,
                List.of("orderId", "pickupPoint", "pickupContainer", "dropoffPoint", "dropoffContainer"));
    }

    @Test
    void applicationResultNamesAreRegistered() {
        assertEquals("banker.send_payment.result", appResults.require(PaymentSentResult.class).type());
        assertEquals("courier.deliver_order.result", appResults.require(DeliveryResult.class).type());
        assertEquals("supervisor.run_inventory.result", appResults.require(InventoryReport.class).type());
    }

    private void assertTask(Class<?> taskClass, String type, Class<?> resultClass, List<String> properties) {
        var meta = appTasks.require(taskClass);
        assertEquals(type, meta.type());
        assertEquals(resultClass, meta.resultClass());
        assertEquals(properties, names(meta.params()));
    }
}
