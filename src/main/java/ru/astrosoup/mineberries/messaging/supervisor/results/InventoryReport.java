package ru.astrosoup.mineberries.messaging.supervisor.results;

import java.time.Instant;
import java.util.List;

import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Result;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;
import ru.astrosoup.mineberries.messaging.common.domain.ItemAmount;
import ru.astrosoup.mineberries.messaging.supervisor.model.InventoryDiscrepancy;

/**
 * Physical warehouse snapshot and the differences from the accounting data.
 * Quantities are aggregated per item across all scanned containers. An empty
 * discrepancy list means the warehouse is consistent.
 */
@Result("supervisor.run_inventory.result")
public record InventoryReport(
        @Property long warehouseId,
        @Property List<ItemAmount> actualItems,
        @Property List<InventoryDiscrepancy> discrepancies,
        @Property Instant completedAt) {

    public InventoryReport {
        warehouseId = DomainPreconditions.positive(warehouseId, "warehouseId");
        actualItems = DomainPreconditions.copy(actualItems, "actualItems");
        discrepancies = DomainPreconditions.copy(discrepancies, "discrepancies");
        completedAt = DomainPreconditions.nonNull(completedAt, "completedAt");
    }
}
