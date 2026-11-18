package ru.astrosoup.mineberries.messaging.courier.model;

/**
 * Physical result of a courier movement. The courier does not judge whether the
 * order was complete; business logic does that.
 */
public enum DeliveryOutcome {
    /** Cargo was moved from the pickup container to the destination container. */
    DELIVERED,
    /** Neither the requested container nor a fallback container could be used. */
    DROPOFF_BLOCKED,
    /**
     * The courier lost the cargo in an accident and it cannot be recovered, for
     * example after a death in a place the items are unable to leave.
     */
    CARGO_LOST
}
