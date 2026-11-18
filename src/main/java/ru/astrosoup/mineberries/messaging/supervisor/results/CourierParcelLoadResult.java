package ru.astrosoup.mineberries.messaging.supervisor.results;

import java.time.Instant;
import java.util.List;

import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Result;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;
import ru.astrosoup.mineberries.messaging.common.domain.ItemAmount;
import ru.astrosoup.mineberries.messaging.common.domain.WorldPoint;

/** Result of filling a courier parcel from warehouse storage. */
@Result("supervisor.load_courier_parcel.result")
public record CourierParcelLoadResult(
        @Property WorldPoint parcelPoint,
        @Property List<ItemAmount> loadedItems,
        @Property List<ItemAmount> missingItems,
        @Property Instant loadedAt) {

    public CourierParcelLoadResult {
        parcelPoint = DomainPreconditions.nonNull(parcelPoint, "parcelPoint");
        loadedItems = DomainPreconditions.copy(loadedItems, "loadedItems");
        missingItems = DomainPreconditions.copy(missingItems, "missingItems");
        loadedAt = DomainPreconditions.nonNull(loadedAt, "loadedAt");
    }
}
