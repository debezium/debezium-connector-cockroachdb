/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

/**
 * Unit tests for schema evolution detection.
 *
 * @author Virag Tripathi
 */
public class CockroachDBSchemaEvolutionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Table buildTable(String... columnDefs) {
        TableId tableId = new TableId("testdb", "public", "test_table");
        TableEditor editor = Table.editor().tableId(tableId);
        int pos = 1;
        for (String colDef : columnDefs) {
            String[] parts = colDef.split(":");
            String name = parts[0];
            boolean nullable = parts.length > 1 && "nullable".equals(parts[1]);
            editor.addColumn(Column.editor()
                    .name(name)
                    .type("STRING")
                    .jdbcType(java.sql.Types.VARCHAR)
                    .optional(nullable)
                    .position(pos++)
                    .create());
        }
        editor.setPrimaryKeyNames(List.of(columnDefs[0].split(":")[0]));
        return editor.create();
    }

    @Test
    public void shouldDetectNoChangeWhenFieldsMatch() throws Exception {
        Table table = buildTable("id", "name", "email:nullable");
        JsonNode data = MAPPER.readTree("{\"id\": 1, \"name\": \"Alice\", \"email\": \"a@b.com\"}");
        assertThat(CockroachDBStreamingChangeEventSource.hasSchemaChanged(data, table)).isFalse();
    }

    @Test
    public void shouldDetectNoChangeWhenNullableFieldAbsent() throws Exception {
        Table table = buildTable("id", "name", "email:nullable");
        // email is nullable and absent from JSON -- this is normal, not a schema change
        JsonNode data = MAPPER.readTree("{\"id\": 1, \"name\": \"Alice\"}");
        assertThat(CockroachDBStreamingChangeEventSource.hasSchemaChanged(data, table)).isFalse();
    }

    @Test
    public void shouldDetectNewColumnAdded() throws Exception {
        Table table = buildTable("id", "name");
        // Event has a new "email" field not in the registered schema
        JsonNode data = MAPPER.readTree("{\"id\": 1, \"name\": \"Alice\", \"email\": \"a@b.com\"}");
        assertThat(CockroachDBStreamingChangeEventSource.hasSchemaChanged(data, table)).isTrue();
    }

    @Test
    public void shouldDetectNonNullableColumnDropped() throws Exception {
        Table table = buildTable("id", "name", "email");
        // "email" is non-nullable but absent from event -> column dropped
        JsonNode data = MAPPER.readTree("{\"id\": 1, \"name\": \"Alice\"}");
        assertThat(CockroachDBStreamingChangeEventSource.hasSchemaChanged(data, table)).isTrue();
    }

    @Test
    public void shouldDetectColumnRenamed() throws Exception {
        Table table = buildTable("id", "name", "email:nullable");
        // "email" renamed to "email_address" -- old field gone, new field appears
        JsonNode data = MAPPER.readTree("{\"id\": 1, \"name\": \"Alice\", \"email_address\": \"a@b.com\"}");
        assertThat(CockroachDBStreamingChangeEventSource.hasSchemaChanged(data, table)).isTrue();
    }

    @Test
    public void shouldDetectMultipleColumnsAdded() throws Exception {
        Table table = buildTable("id", "name");
        JsonNode data = MAPPER.readTree("{\"id\": 1, \"name\": \"Alice\", \"email\": \"a@b.com\", \"age\": 30}");
        assertThat(CockroachDBStreamingChangeEventSource.hasSchemaChanged(data, table)).isTrue();
    }

    @Test
    public void shouldNotFalsePositiveOnNullValues() throws Exception {
        Table table = buildTable("id", "name", "email:nullable");
        // All fields present, email is null
        JsonNode data = MAPPER.readTree("{\"id\": 1, \"name\": \"Alice\", \"email\": null}");
        assertThat(CockroachDBStreamingChangeEventSource.hasSchemaChanged(data, table)).isFalse();
    }

    @Test
    public void shouldDetectChangeWithEmptyEvent() throws Exception {
        Table table = buildTable("id", "name");
        // Empty object -- id and name are non-nullable and missing
        JsonNode data = MAPPER.readTree("{}");
        assertThat(CockroachDBStreamingChangeEventSource.hasSchemaChanged(data, table)).isTrue();
    }

    @Test
    public void shouldHandleSingleColumnTable() throws Exception {
        Table table = buildTable("id");
        JsonNode data = MAPPER.readTree("{\"id\": 1}");
        assertThat(CockroachDBStreamingChangeEventSource.hasSchemaChanged(data, table)).isFalse();
    }

    @Test
    public void shouldDetectChangeOnSingleColumnTableWithNewField() throws Exception {
        Table table = buildTable("id");
        JsonNode data = MAPPER.readTree("{\"id\": 1, \"new_col\": \"value\"}");
        assertThat(CockroachDBStreamingChangeEventSource.hasSchemaChanged(data, table)).isTrue();
    }
}
