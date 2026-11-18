package ru.astrosoup.mineberries.messaging.supervisor.results;

import java.time.Instant;
import java.util.List;

import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Result;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;
import ru.astrosoup.mineberries.messaging.common.domain.ItemAmount;
import ru.astrosoup.mineberries.messaging.common.domain.WorldPoint;

/** Result of emptying a courier parcel into warehouse storage. */
@Result("supervisor.unload_courier_parcel.result")
public record CourierParcelUnloadResult(
        @Property WorldPoint parcelPoint,
        @Property List<ItemAmount> unloadedItems,
        @Property List<ItemAmount> remainingItems,
        @Property Instant unloadedAt) {

    public CourierParcelUnloadResult {
        parcelPoint = DomainPreconditions.nonNull(parcelPoint, "parcelPoint");
        unloadedItems = DomainPreconditions.copy(unloadedItems, "unloadedItems");
        remainingItems = DomainPreconditions.copy(remainingItems, "remainingItems");
        unloadedAt = DomainPreconditions.nonNull(unloadedAt, "unloadedAt");
    }
}
