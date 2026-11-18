package ru.astrosoup.mineberries.messaging.courier.tasks;

import lombok.Getter;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;
import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.annotations.Task;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;
import ru.astrosoup.mineberries.messaging.common.domain.WorldPoint;
import ru.astrosoup.mineberries.messaging.courier.results.DeliveryResult;

/**
 * Moves the cargo prepared in a pickup container to a drop-off container.
 *
 * <p>The courier is pure transport: it takes everything from
 * {@code pickupContainer} and leaves it in {@code dropoffContainer}. If the
 * requested drop-off container is absent or full, the courier places its own
 * container near {@code dropoffPoint} and reports it in the result.
 *
 * <p>The task deliberately carries no order items, order type, or player
 * nicknames. Cargo preparation, partial orders, and delivery confirmation are
 * business-logic concerns and are not part of this task.
 */
@Task("courier.deliver_order")
@Getter
public class DeliverOrder extends AbstractTask<DeliveryResult> {
    @Property private final long orderId;
    @Property private final WorldPoint pickupPoint;
    @Property private final WorldPoint pickupContainer;
    @Property private final WorldPoint dropoffPoint;
    @Property private final WorldPoint dropoffContainer;

    public DeliverOrder(
            long orderId,
            WorldPoint pickupPoint,
            WorldPoint pickupContainer,
            WorldPoint dropoffPoint,
            WorldPoint dropoffContainer) {
        this.orderId = DomainPreconditions.positive(orderId, "orderId");
        this.pickupPoint = DomainPreconditions.nonNull(pickupPoint, "pickupPoint");
        this.pickupContainer = DomainPreconditions.nonNull(pickupContainer, "pickupContainer");
        this.dropoffPoint = DomainPreconditions.nonNull(dropoffPoint, "dropoffPoint");
        this.dropoffContainer = dropoffContainer;
    }
}
