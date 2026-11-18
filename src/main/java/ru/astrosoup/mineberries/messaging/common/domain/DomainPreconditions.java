package ru.astrosoup.mineberries.messaging.common.domain;

import java.math.BigDecimal;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Validation shared by bot task contracts.
 *
 * <p>Task objects cross the broker boundary, so they validate their own payload
 * before the service submits them. The rules mirror the database constraints in
 * {@code 01-schema.sql} where those constraints describe the domain rather than
 * storage details.
 */
public final class DomainPreconditions {
    private static final Pattern NICKNAME = Pattern.compile("^[A-Za-z0-9_]{3,16}$");
    private static final BigDecimal MAX_AMOUNT = new BigDecimal("9999999999.99");

    private DomainPreconditions() {}

    public static String nickname(String value, String field) {
        nonNull(value, field);
        if (!NICKNAME.matcher(value).matches())
            throw new IllegalArgumentException(field + " must match " + NICKNAME.pattern() + ": " + value);
        return value;
    }

    public static String text(String value, String field) {
        nonNull(value, field);
        if (value.isBlank())
            throw new IllegalArgumentException(field + " must not be blank");
        return value;
    }

    /**
     * Money in the game economy. The schema stores amounts as NUMERIC(12, 2),
     * so fractional cents are rejected here instead of being silently rounded.
     */
    public static BigDecimal amount(BigDecimal value, String field) {
        nonNull(value, field);
        if (value.signum() <= 0)
            throw new IllegalArgumentException(field + " must be positive: " + value);
        if (value.stripTrailingZeros().scale() > 2)
            throw new IllegalArgumentException(field + " must have at most two decimal places: " + value);
        if (value.compareTo(MAX_AMOUNT) > 0)
            throw new IllegalArgumentException(field + " must not exceed " + MAX_AMOUNT + ": " + value);
        return value;
    }

    public static int positive(int value, String field) {
        if (value <= 0)
            throw new IllegalArgumentException(field + " must be positive: " + value);
        return value;
    }

    public static long positive(long value, String field) {
        if (value <= 0)
            throw new IllegalArgumentException(field + " must be positive: " + value);
        return value;
    }

    public static int nonNegative(int value, String field) {
        if (value < 0)
            throw new IllegalArgumentException(field + " must not be negative: " + value);
        return value;
    }

    public static <T> T nonNull(T value, String field) {
        if (value == null)
            throw new IllegalArgumentException(field + " must not be null");
        return value;
    }

    /** Copies a list that may be empty. */
    public static <T> List<T> copy(List<T> value, String field) {
        nonNull(value, field);
        return List.copyOf(value);
    }

    /** Copies a list that must contain at least one element. */
    public static <T> List<T> nonEmpty(List<T> value, String field) {
        var copy = copy(value, field);
        if (copy.isEmpty())
            throw new IllegalArgumentException(field + " must not be empty");
        return copy;
    }
}
