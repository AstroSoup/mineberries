package ru.astrosoup.mineberries.messaging.courier.results;

import java.time.Instant;
import java.util.List;

import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Result;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;
import ru.astrosoup.mineberries.messaging.common.domain.ItemAmount;
import ru.astrosoup.mineberries.messaging.common.domain.WorldPoint;
import ru.astrosoup.mineberries.messaging.courier.model.DeliveryOutcome;

/**
 * Physical result of a courier movement.
 *
 * @param dropoffContainer      container actually used, or {@code null} when
 *                              the cargo could not be dropped off or was lost
 * @param usedFallbackContainer whether the courier had to place its own
 *                              container because the requested one was absent
 *                              or full
 * @param movedItems            cargo actually moved into the destination
 *                              container; this is a physical fact, not a
 *                              completeness check against the order
 */
@Result("courier.deliver_order.result")
public record DeliveryResult(
        @Property long orderId,
        @Property DeliveryOutcome outcome,
        @Property WorldPoint dropoffContainer,
        @Property boolean usedFallbackContainer,
        @Property List<ItemAmount> movedItems,
        @Property Instant finishedAt) {

    public DeliveryResult {
        orderId = DomainPreconditions.positive(orderId, "orderId");
        outcome = DomainPreconditions.nonNull(outcome, "outcome");
        movedItems = DomainPreconditions.copy(movedItems, "movedItems");
        finishedAt = DomainPreconditions.nonNull(finishedAt, "finishedAt");
        if (outcome == DeliveryOutcome.DELIVERED && dropoffContainer == null)
            throw new IllegalArgumentException("A delivered order must name the drop-off container");
    }
}
