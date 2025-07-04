/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.cockroachdb.serialization.ChangefeedSchemaParser.ParsedChange;

/**
 * Unit tests for ChangefeedSchemaParser.
 *
 * @author Virag Tripathi
 */
public class ChangefeedSchemaParserTest {

    private ChangefeedSchemaParser parser;

    @Before
    public void setUp() {
        parser = new ChangefeedSchemaParser();
    }

    @Test
    public void shouldParseInsertEvent() throws Exception {
        String keyJson = "{\"id\": 1}";
        String valueJson = "{\"after\": {\"id\": 1, \"name\": \"John Doe\", \"email\": \"john@example.com\"}, \"updated\": {\"name\": \"John Doe\"}}";

        ParsedChange result = parser.parse(keyJson, valueJson);

        assertThat(result).isNotNull();
        assertThat(result.keySchema()).isNotNull();
        assertThat(result.valueSchema()).isNotNull();
        assertThat(result.key()).isNotNull();
        assertThat(result.value()).isNotNull();

        // Verify key structure
        Struct key = (Struct) result.key();
        assertThat(key.getInt32("id")).isEqualTo(1);

        // Verify value structure
        Struct value = (Struct) result.value();
        assertThat(value.getStruct("after")).isNotNull();
        assertThat(value.getStruct("updated")).isNotNull();
    }

    @Test
    public void shouldParseUpdateEvent() throws Exception {
        String keyJson = "{\"id\": 1}";
        String valueJson = "{\"after\": {\"id\": 1, \"name\": \"Jane Doe\", \"email\": \"jane@example.com\"}, \"before\": {\"id\": 1, \"name\": \"John Doe\", \"email\": \"john@example.com\"}, \"updated\": {\"name\": \"Jane Doe\"}, \"diff\": {\"name\": {\"before\": \"John Doe\", \"after\": \"Jane Doe\"}}}";

        ParsedChange result = parser.parse(keyJson, valueJson);

        assertThat(result).isNotNull();
        assertThat(result.key()).isNotNull();
        assertThat(result.value()).isNotNull();

        Struct value = (Struct) result.value();
        assertThat(value.getStruct("after")).isNotNull();
        assertThat(value.getStruct("before")).isNotNull();
        assertThat(value.getStruct("updated")).isNotNull();
        assertThat(value.getStruct("diff")).isNotNull();
    }

    @Test
    public void shouldParseDeleteEvent() throws Exception {
        String keyJson = "{\"id\": 1}";
        String valueJson = "{\"before\": {\"id\": 1, \"name\": \"John Doe\", \"email\": \"john@example.com\"}}";

        ParsedChange result = parser.parse(keyJson, valueJson);

        assertThat(result).isNotNull();
        assertThat(result.key()).isNotNull();
        assertThat(result.value()).isNotNull();

        Struct value = (Struct) result.value();
        assertThat(value.getStruct("before")).isNotNull();
        assertThat(value.getStruct("after")).isNull();
    }

    @Test
    public void shouldParseResolvedTimestamp() throws Exception {
        String keyJson = null;
        String valueJson = "{\"resolved\": \"1640995200.0000000000\"}";

        ParsedChange result = parser.parse(keyJson, valueJson);

        assertThat(result).isNotNull();
        assertThat(result.key()).isNull();
        assertThat(result.value()).isNotNull();

        Struct value = (Struct) result.value();
        assertThat(value.getString("resolved")).isEqualTo("1640995200.0000000000");
    }

    @Test
    public void shouldHandleNullValues() throws Exception {
        String keyJson = "{\"id\": 1}";
        String valueJson = "{\"after\": {\"id\": 1, \"name\": null, \"email\": \"john@example.com\"}}";

        ParsedChange result = parser.parse(keyJson, valueJson);

        assertThat(result).isNotNull();
        Struct value = (Struct) result.value();
        Struct after = value.getStruct("after");
        assertThat(after.getString("name")).isNull();
        assertThat(after.getString("email")).isEqualTo("john@example.com");
    }

    @Test
    public void shouldHandleNestedObjects() throws Exception {
        String keyJson = "{\"id\": 1}";
        String valueJson = "{\"after\": {\"id\": 1, \"address\": {\"street\": \"123 Main St\", \"city\": \"New York\"}}}";

        ParsedChange result = parser.parse(keyJson, valueJson);

        assertThat(result).isNotNull();
        Struct value = (Struct) result.value();
        Struct after = value.getStruct("after");
        Struct address = after.getStruct("address");
        assertThat(address.getString("street")).isEqualTo("123 Main St");
        assertThat(address.getString("city")).isEqualTo("New York");
    }

    @Test
    public void shouldHandleArrays() throws Exception {
        String keyJson = "{\"id\": 1}";
        String valueJson = "{\"after\": {\"id\": 1, \"tags\": [\"tag1\", \"tag2\"]}}";

        ParsedChange result = parser.parse(keyJson, valueJson);

        assertThat(result).isNotNull();
        Struct value = (Struct) result.value();
        Struct after = value.getStruct("after");
        // Arrays are converted to strings in the current implementation
        assertThat(after.getString("tags")).contains("tag1");
        assertThat(after.getString("tags")).contains("tag2");
    }

    @Test
    public void shouldHandleNumericTypes() throws Exception {
        String keyJson = "{\"id\": 1}";
        String valueJson = "{\"after\": {\"id\": 1, \"age\": 30, \"salary\": 50000.50, \"active\": true}}";

        ParsedChange result = parser.parse(keyJson, valueJson);

        assertThat(result).isNotNull();
        Struct value = (Struct) result.value();
        Struct after = value.getStruct("after");
        assertThat(after.getInt32("age")).isEqualTo(30);
        assertThat(after.getFloat64("salary")).isEqualTo(50000.50);
        assertThat(after.getBoolean("active")).isTrue();
    }

    @Test
    public void shouldHandleMalformedJson() {
        String keyJson = "{\"id\": 1}";
        String valueJson = "{\"after\": {\"id\": 1, \"name\": \"John Doe\"}"; // Missing closing brace

        assertThatThrownBy(() -> parser.parse(keyJson, valueJson))
                .isInstanceOf(Exception.class);
    }

    @Test
    public void shouldHandleEmptyJson() throws Exception {
        String keyJson = "{}";
        String valueJson = "{}";

        ParsedChange result = parser.parse(keyJson, valueJson);

        assertThat(result).isNotNull();
        assertThat(result.key()).isNotNull();
        assertThat(result.value()).isNotNull();
    }

    @Test
    public void shouldHandleComplexEnrichedEnvelope() throws Exception {
        String keyJson = "{\"id\": 1}";
        String valueJson = "{\"after\": {\"id\": 1, \"name\": \"John Doe\", \"email\": \"john@example.com\", \"metadata\": {\"created_at\": \"2023-01-01T00:00:00Z\", \"version\": 1}}, \"updated\": {\"name\": \"John Doe\"}, \"diff\": {\"name\": {\"before\": null, \"after\": \"John Doe\"}}, \"resolved\": \"1640995200.0000000000\"}";

        ParsedChange result = parser.parse(keyJson, valueJson);

        assertThat(result).isNotNull();
        Struct value = (Struct) result.value();

        // Check all envelope fields
        assertThat(value.getStruct("after")).isNotNull();
        assertThat(value.getStruct("updated")).isNotNull();
        assertThat(value.getStruct("diff")).isNotNull();
        assertThat(value.getString("resolved")).isEqualTo("1640995200.0000000000");

        // Check nested metadata
        Struct after = value.getStruct("after");
        Struct metadata = after.getStruct("metadata");
        assertThat(metadata.getString("created_at")).isEqualTo("2023-01-01T00:00:00Z");
        assertThat(metadata.getInt32("version")).isEqualTo(1);
    }
}
