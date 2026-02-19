/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.data.vector.DoubleVector;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * Unit tests for {@link CockroachDBValueConverterProvider} verifying schema mapping
 * and value conversion for CockroachDB types, including pgvector-compatible
 * vector types.
 *
 * @author Virag Tripathi
 */
public class CockroachDBValueConverterProviderTest {

    private CockroachDBValueConverterProvider provider;

    @BeforeEach
    void setUp() {
        provider = new CockroachDBValueConverterProvider();
    }

    // --- Schema builder tests ---

    @Test
    void booleanColumnReturnsOptionalBool() {
        Column col = column("BOOL");
        SchemaBuilder builder = provider.schemaBuilder(col);
        assertThat(builder.build().type()).isEqualTo(Schema.Type.BOOLEAN);
        assertThat(builder.build().isOptional()).isTrue();
    }

    @Test
    void int8ColumnReturnsOptionalInt64() {
        Column col = column("INT8");
        SchemaBuilder builder = provider.schemaBuilder(col);
        assertThat(builder.build().type()).isEqualTo(Schema.Type.INT64);
    }

    @Test
    void float8ColumnReturnsOptionalFloat64() {
        Column col = column("FLOAT8");
        SchemaBuilder builder = provider.schemaBuilder(col);
        assertThat(builder.build().type()).isEqualTo(Schema.Type.FLOAT64);
    }

    @Test
    void jsonbColumnReturnsJsonSchema() {
        Column col = column("JSONB");
        SchemaBuilder builder = provider.schemaBuilder(col);
        assertThat(builder.build().name()).isEqualTo("io.debezium.data.Json");
    }

    @Test
    void uuidColumnReturnsUuidSchema() {
        Column col = column("UUID");
        SchemaBuilder builder = provider.schemaBuilder(col);
        assertThat(builder.build().name()).isEqualTo("io.debezium.data.Uuid");
    }

    @Test
    void vectorColumnReturnsDoubleVectorSchema() {
        Column col = column("VECTOR");
        SchemaBuilder builder = provider.schemaBuilder(col);
        Schema schema = builder.build();
        assertThat(schema.name()).isEqualTo(DoubleVector.LOGICAL_NAME);
        assertThat(schema.type()).isEqualTo(Schema.Type.ARRAY);
    }

    @Test
    void unknownTypeReturnsOptionalString() {
        Column col = column("SOMEFUTURETYPE");
        SchemaBuilder builder = provider.schemaBuilder(col);
        assertThat(builder.build().type()).isEqualTo(Schema.Type.STRING);
        assertThat(builder.build().isOptional()).isTrue();
    }

    @Test
    void nullColumnReturnsOptionalString() {
        SchemaBuilder builder = provider.schemaBuilder(null);
        assertThat(builder.build().type()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    void nullTypeNameReturnsOptionalString() {
        Column col = Column.editor().name("col").type("unknown").create();
        SchemaBuilder builder = provider.schemaBuilder(col);
        assertThat(builder.build().type()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    void timestamptzColumnReturnsMicroTimestamp() {
        Column col = column("TIMESTAMPTZ");
        SchemaBuilder builder = provider.schemaBuilder(col);
        assertThat(builder.build().name()).isEqualTo("io.debezium.time.MicroTimestamp");
    }

    @Test
    void bytesColumnReturnsBytesSchema() {
        Column col = column("BYTEA");
        SchemaBuilder builder = provider.schemaBuilder(col);
        assertThat(builder.build().type()).isEqualTo(Schema.Type.BYTES);
    }

    @Test
    void arrayTypeFallsBackToString() {
        Column col = column("TEXT[]");
        SchemaBuilder builder = provider.schemaBuilder(col);
        assertThat(builder.build().type()).isEqualTo(Schema.Type.STRING);
    }

    // --- Value converter tests ---

    @Test
    void booleanConverterHandlesTrueValues() {
        Column col = column("BOOL");
        Schema schema = provider.schemaBuilder(col).build();
        Field field = new Field("test", 0, schema);
        ValueConverter converter = provider.converter(col, field);

        assertThat(converter.convert("true")).isEqualTo(true);
        assertThat(converter.convert("t")).isEqualTo(true);
        assertThat(converter.convert("1")).isEqualTo(true);
        assertThat(converter.convert("yes")).isEqualTo(true);
        assertThat(converter.convert("false")).isEqualTo(false);
        assertThat(converter.convert(null)).isNull();
    }

    @Test
    void int64ConverterParsesStrings() {
        Column col = column("INT8");
        Schema schema = provider.schemaBuilder(col).build();
        Field field = new Field("test", 0, schema);
        ValueConverter converter = provider.converter(col, field);

        assertThat(converter.convert("12345")).isEqualTo(12345L);
        assertThat(converter.convert(null)).isNull();
    }

    @Test
    void float64ConverterParsesStrings() {
        Column col = column("FLOAT8");
        Schema schema = provider.schemaBuilder(col).build();
        Field field = new Field("test", 0, schema);
        ValueConverter converter = provider.converter(col, field);

        assertThat(converter.convert("3.14")).isEqualTo(3.14);
        assertThat(converter.convert(null)).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void vectorConverterParsesVectorString() {
        Column col = column("VECTOR");
        Schema schema = provider.schemaBuilder(col).build();
        Field field = new Field("embedding", 0, schema);
        ValueConverter converter = provider.converter(col, field);

        Object result = converter.convert("[1.0,2.5,3.7]");
        assertThat(result).isInstanceOf(List.class);
        List<Double> vector = (List<Double>) result;
        assertThat(vector).hasSize(3);
        assertThat(vector.get(0)).isEqualTo(1.0);
        assertThat(vector.get(1)).isEqualTo(2.5);
        assertThat(vector.get(2)).isEqualTo(3.7);
    }

    @Test
    void vectorConverterHandlesNull() {
        Column col = column("VECTOR");
        Schema schema = provider.schemaBuilder(col).build();
        Field field = new Field("embedding", 0, schema);
        ValueConverter converter = provider.converter(col, field);

        assertThat(converter.convert(null)).isNull();
    }

    @Test
    void jsonbConverterPassesThrough() {
        Column col = column("JSONB");
        Schema schema = provider.schemaBuilder(col).build();
        Field field = new Field("data", 0, schema);
        ValueConverter converter = provider.converter(col, field);

        assertThat(converter.convert("{\"key\":\"value\"}")).isEqualTo("{\"key\":\"value\"}");
        assertThat(converter.convert(null)).isNull();
    }

    @Test
    void nullConverterColumnReturnsStringConverter() {
        ValueConverter converter = provider.converter(null, null);
        assertThat(converter.convert("hello")).isEqualTo("hello");
        assertThat(converter.convert(null)).isNull();
    }

    @Test
    void defaultConverterReturnsToString() {
        Column col = column("SOMEFUTURETYPE");
        Schema schema = provider.schemaBuilder(col).build();
        Field field = new Field("test", 0, schema);
        ValueConverter converter = provider.converter(col, field);

        assertThat(converter.convert(42)).isEqualTo("42");
        assertThat(converter.convert(null)).isNull();
    }

    private static Column column(String typeName) {
        return Column.editor()
                .name("test_col")
                .type(typeName)
                .jdbcType(java.sql.Types.OTHER)
                .create();
    }
}
