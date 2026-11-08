package ru.astrosoup.mineberries.messaging.common.dtos;

import java.util.Map;
import java.util.UUID;

public record MessageDto(UUID messageId, String type, Map<String, Object> payload) {}