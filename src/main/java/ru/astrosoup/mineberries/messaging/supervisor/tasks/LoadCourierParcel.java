package ru.astrosoup.mineberries.messaging.supervisor.tasks;

import java.util.List;

import lombok.Getter;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Task;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;
import ru.astrosoup.mineberries.messaging.common.domain.ItemAmount;
import ru.astrosoup.mineberries.messaging.common.domain.WorldPoint;
import ru.astrosoup.mineberries.messaging.supervisor.results.CourierParcelLoadResult;

/**
 * Collects ordered items from warehouse storage and puts them into the courier
 * parcel container so that a courier can take them.
 *
 * <p>A parcel is a dedicated chest or barrel at the warehouse. The supervisor
 * reports both the items it managed to load and any items that were missing.
 */
@Task("supervisor.load_courier_parcel")
@Getter
public class LoadCourierParcel extends AbstractTask<CourierParcelLoadResult> {
    @Property private final long warehouseId;
    @Property private final WorldPoint parcelPoint;
    @Property private final List<ItemAmount> items;

    public LoadCourierParcel(
            long warehouseId,
            WorldPoint parcelPoint,
            List<ItemAmount> items) {
        this.warehouseId = DomainPreconditions.positive(warehouseId, "warehouseId");
        this.parcelPoint = DomainPreconditions.nonNull(parcelPoint, "parcelPoint");
        this.items = DomainPreconditions.nonEmpty(items, "items");
    }
}
