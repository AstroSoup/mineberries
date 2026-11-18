package ru.astrosoup.mineberries.messaging.common.domain;

/**
 * Block coordinates in a Minecraft world.
 *
 * <p>This is the bot-facing counterpart of the {@code point} table. The height
 * bounds match the database constraint for ordinary overworld dimensions.
 */
public record WorldPoint(String world, int x, int y, int z) {
    public WorldPoint {
        world = DomainPreconditions.text(world, "world");
        if (y < -64 || y > 320)
            throw new IllegalArgumentException("y must be between -64 and 320: " + y);
    }
}
