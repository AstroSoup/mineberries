package ru.astrosoup.mineberries.messaging.common.domain;

/**
 * A quantity of a Minecraft item.
 *
 * @param itemId   catalog id, or {@code null} for an item found in a container
 *                 that is not registered in the catalog
 * @param material Minecraft material id, for example
 *                 {@code minecraft:oak_planks}
 * @param quantity positive number of items
 */
public record ItemAmount(Long itemId, String material, int quantity) {
    public ItemAmount {
        if (itemId != null)
            DomainPreconditions.positive(itemId, "itemId");
        material = DomainPreconditions.text(material, "material");
        quantity = DomainPreconditions.positive(quantity, "quantity");
    }
}
