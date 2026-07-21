/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Regression test for the required-field conversion failure with {@code NOT NULL} JSONB and
 * VECTOR columns.
 *
 * <p>Changefeed events can predate the registered table schema, for example when the
 * intermediate topic holds a backlog written before a column was added. Every value field must
 * therefore be optional regardless of column nullability; a required field with no default fails
 * {@link JsonConverter} with "Conversion error: null value for field that is required and has no
 * default value" as soon as the value is absent.</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBNotNullColumnConversionTest {

    private TableSchemaBuilder tableSchemaBuilder;
    private TopicNamingStrategy<TableId> topicNamingStrategy;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.password", "")
                .with("database.dbname", "defaultdb")
                .with("topic.prefix", "test")
                .build();
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        tableSchemaBuilder = new TableSchemaBuilder(
                new CockroachDBValueConverterProvider(),
                new CockroachDBDefaultValueConverter(),
                connectorConfig.schemaNameAdjuster(),
                new CustomConverterRegistry(Collections.emptyList()),
                connectorConfig.getSourceInfoStructMaker().schema(),
                connectorConfig.getFieldNamer(),
                false,
                connectorConfig.getEventConvertingFailureHandlingMode());
    }

    @Test
    void notNullJsonbAndVectorFieldsAreOptional() {
        TableSchema tableSchema = schemaFor(tableWithNotNullJsonbAndVector());
        assertThat(tableSchema.valueSchema().field("doc").schema().isOptional()).isTrue();
        assertThat(tableSchema.valueSchema().field("emb").schema().isOptional()).isTrue();
    }

    @Test
    void eventLackingNotNullJsonbAndVectorValuesConvertsWithSchemasEnabled() throws Exception {
        TableSchema tableSchema = schemaFor(tableWithNotNullJsonbAndVector());

        // An event written before doc and emb existed carries only id; the emitter supplies null
        // for the absent columns.
        Struct value = tableSchema.valueFromColumnData(new Object[]{ 42L, null, null });

        try (JsonConverter jsonConverter = new JsonConverter()) {
            Map<String, Object> converterConfig = new HashMap<>();
            converterConfig.put("schemas.enable", "true");
            converterConfig.put("converter.type", "value");
            jsonConverter.configure(converterConfig);
            byte[] serialized = jsonConverter.fromConnectData("test.public.diag", tableSchema.valueSchema(), value);
            assertThat(serialized).isNotEmpty();
        }
    }

    private Table tableWithNotNullJsonbAndVector() {
        return Table.editor()
                .tableId(new TableId("defaultdb", "public", "diag"))
                .addColumn(Column.editor()
                        .name("id")
                        .type("INT8")
                        .jdbcType(Types.BIGINT)
                        .optional(false)
                        .create())
                .addColumn(Column.editor()
                        .name("doc")
                        .type("JSONB")
                        .jdbcType(Types.OTHER)
                        .optional(false)
                        .create())
                .addColumn(Column.editor()
                        .name("emb")
                        .type("VECTOR")
                        .jdbcType(Types.OTHER)
                        .optional(false)
                        .create())
                .setPrimaryKeyNames("id")
                .create();
    }

    private TableSchema schemaFor(Table table) {
        TableSchema tableSchema = tableSchemaBuilder.create(topicNamingStrategy, table, null, null, null);
        assertThat(tableSchema.valueSchema().type()).isEqualTo(Schema.Type.STRUCT);
        return tableSchema;
    }
}
