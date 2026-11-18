package ru.astrosoup.mineberries.messaging.supervisor.model;

import ru.astrosoup.mineberries.messaging.common.annotations.Property;
import ru.astrosoup.mineberries.messaging.common.domain.DomainPreconditions;

/**
 * Difference between the book quantity and the quantity found in a container
 * during inventory
 */
public record InventoryDiscrepancy(
        @Property Long itemId,
        @Property String material,
        @Property int expectedQuantity,
        @Property int actualQuantity) {

    public InventoryDiscrepancy {
        if (itemId != null)
            DomainPreconditions.positive(itemId, "itemId");
        material = DomainPreconditions.text(material, "material");
        DomainPreconditions.nonNegative(expectedQuantity, "expectedQuantity");
        DomainPreconditions.nonNegative(actualQuantity, "actualQuantity");
        if (expectedQuantity == actualQuantity)
            throw new IllegalArgumentException("A discrepancy must have different expected and actual quantities");
    }
}
