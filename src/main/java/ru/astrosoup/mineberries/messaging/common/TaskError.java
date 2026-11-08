package ru.astrosoup.mineberries.messaging.common;

public record TaskError(String message, boolean retryable) {}