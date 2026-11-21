package ru.astrosoup.mineberries.messaging.common;

import java.time.Instant;
import java.util.UUID;

/** Published after an accepted completion whose result timestamp exceeded the buffered deadline. */
public record LateTaskCompletion(UUID taskId, String taskType, Instant deadline, Instant bufferedDeadline,
                                 Instant resultAt) {}
