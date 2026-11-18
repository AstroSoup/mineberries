package ru.astrosoup.mineberries.messaging.supervisor.tasks;

import lombok.Getter;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Task;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;
import ru.astrosoup.mineberries.messaging.common.domain.WorldPoint;
import ru.astrosoup.mineberries.messaging.supervisor.results.CourierParcelUnloadResult;

/**
 * Takes whatever is left in a courier parcel container and puts it into
 * warehouse storage.
 *
 * <p>All parcel contents are unloaded; the result reports both the items that
 * were moved and any items that remained because storage was full.
 */
@Task("supervisor.unload_courier_parcel")
@Getter
public class UnloadCourierParcel extends AbstractTask<CourierParcelUnloadResult> {
    @Property private final long warehouseId;
    @Property private final WorldPoint parcelPoint;

    public UnloadCourierParcel(long warehouseId, WorldPoint parcelPoint) {
        this.warehouseId = DomainPreconditions.positive(warehouseId, "warehouseId");
        this.parcelPoint = DomainPreconditions.nonNull(parcelPoint, "parcelPoint");
    }
}
