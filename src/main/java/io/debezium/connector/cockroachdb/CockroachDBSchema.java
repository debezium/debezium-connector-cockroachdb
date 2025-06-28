/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0,
 * available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Schema manager for the CockroachDB connector.
 * Extends Debezium's RelationalDatabaseSchema to manage table schemas.
 *
 * Compatible with Debezium versions that use extended TableSchemaBuilder constructors.
 *
 * @author Virag Tripathi
 */
public class CockroachDBSchema extends RelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBSchema.class);

    public CockroachDBSchema(
                             CockroachDBConnectorConfig config,
                             TopicNamingStrategy<TableId> topicNamingStrategy) {

        super(
                config,
                topicNamingStrategy,
                config.getTableFilters().dataCollectionFilter(),
                config.getColumnFilter(),
                new TableSchemaBuilder(
                        new CockroachDBValueConverterProvider(), // custom or stub
                        config.schemaNameAdjuster(),
                        new CustomConverterRegistry(Collections.emptyList()),
                        new CockroachDBSourceInfoStructMaker().schema(), // source struct schema
                        column -> column.name(),
                        false // multiPartitionMode
                ),
                false, // tableIdCaseInsensitive
                null // KeyMapper: pass null if no custom mapping logic
        );
    }

    public void initialize() {
        LOGGER.info("Initializing CockroachDBSchema (currently no-op)");
    }

    @Override
    public void close() {
        LOGGER.info("Closing CockroachDBSchema");
        super.close();
    }
}
