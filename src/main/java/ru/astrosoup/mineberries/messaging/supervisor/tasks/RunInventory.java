package ru.astrosoup.mineberries.messaging.supervisor.tasks;

import java.util.List;

import lombok.Getter;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Task;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;
import ru.astrosoup.mineberries.messaging.common.domain.ItemAmount;
import ru.astrosoup.mineberries.messaging.supervisor.results.InventoryReport;

/**
 * Scans warehouse containers and compares the physical contents with the
 * accounting snapshot.
 *
 * <p>The expected snapshot may be empty for an empty warehouse. Both the
 * expected and the reported quantities are aggregated per item across all
 * containers. The result contains the full physical snapshot and the
 * discrepancies.
 */
@Task("supervisor.run_inventory")
@Getter
public class RunInventory extends AbstractTask<InventoryReport> {
    @Property private final long warehouseId;
    @Property private final List<ItemAmount> expectedItems;

    public RunInventory(long warehouseId, List<ItemAmount> expectedItems) {
        this.warehouseId = DomainPreconditions.positive(warehouseId, "warehouseId");
        this.expectedItems = DomainPreconditions.copy(expectedItems, "expectedItems");
    }
}
