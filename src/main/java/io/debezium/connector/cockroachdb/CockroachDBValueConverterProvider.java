/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;

/**
 * Stub implementation of a value converter provider for CockroachDB.
 * This can be expanded later to support type-specific conversions.
 *
 * @author Virag Tripathi
 */
public class CockroachDBValueConverterProvider implements ValueConverterProvider {
    @Override
    public SchemaBuilder schemaBuilder(Column columnDefinition) {
        // Return null for now — implement if needed
        return null;
    }

    @Override
    public ValueConverter converter(Column columnDefinition, Field fieldDefn) {
        // Return null for now — implement if needed
        return null;
    }
}
