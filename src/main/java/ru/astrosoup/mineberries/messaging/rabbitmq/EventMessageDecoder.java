package ru.astrosoup.mineberries.messaging.rabbitmq;

import com.networknt.schema.OutputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;

import ru.astrosoup.mineberries.messaging.common.dtos.EventDto;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;

/** Validates the wire format before converting it to the application DTO. */
final class EventMessageDecoder {
    private static final String SCHEMA_PREFIX = "https://mineberries.example/schemas/";

    private final Schema schema;
    private final ObjectReader jsonReader;
    private final ObjectReader eventReader;

    EventMessageDecoder(ObjectMapper mapper) {
        var registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
                builder -> builder.schemaIdResolvers(resolvers ->
                        resolvers.mapPrefix(SCHEMA_PREFIX, "classpath:messaging/schemas/")));
        schema = registry.getSchema(SchemaLocation.of(SCHEMA_PREFIX + "event.schema.json"));
        schema.initializeValidators();
        jsonReader = mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS,
                DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        eventReader = mapper.readerFor(EventDto.class)
                .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    }

    EventDto decode(byte[] body) {
        try {
            var json = jsonReader.readTree(body);
            if (!schema.validate(json, OutputFormat.BOOLEAN) || !json.path("version").canConvertToLong()) {
                throw new InvalidEventException();
            }
            return eventReader.readValue(json);
        } catch (JacksonException e) {
            // Parse/mapping errors can contain raw payload values.
            throw new InvalidEventException();
        }
    }

    static final class InvalidEventException extends RuntimeException {}
}
