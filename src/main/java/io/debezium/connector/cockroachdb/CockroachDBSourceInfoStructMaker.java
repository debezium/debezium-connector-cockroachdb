/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

/**
 * Struct maker for CockroachDB source metadata (`source` block).
 *
 * This class extends the common Debezium source info struct with CockroachDB-specific fields.
 * The base class already provides common fields like:
 * - db (database name)
 * - ts_ms (timestamp)
 * - ts_ns (nanosecond timestamp, since Debezium 2.5+)
 * - snapshot (snapshot indicator)
 *
 * This class adds CockroachDB-specific fields:
 * <ul>
 *     <li>cluster - logical cluster name (CockroachDB specific)</li>
 *     <li>resolved_ts - resolved timestamp from changefeed (for consistency tracking)</li>
 *     <li>ts_hlc - Hybrid Logical Clock timestamp (CockroachDB's internal timestamp)</li>
 * </ul>
 *
 * NOTE: Do NOT add the "ts_ns" field here, as it is already included by the base class.
 * Adding it again will cause a SchemaBuilderException due to field name duplication.
 *
 * @author Virag Tripathi
 */
public class CockroachDBSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    // CockroachDB-specific field names
    // Note: "db" and "ts_ns" fields are already provided by the base class, so we don't duplicate them
    private static final String FIELD_CLUSTER = "cluster";
    private static final String FIELD_RESOLVED_TS = "resolved_ts";
    private static final String FIELD_HLC = "ts_hlc";

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);

        // Build the schema by extending the common schema with CockroachDB-specific fields
        this.schema = commonSchemaBuilder()
                .name("io.debezium.connector.cockroachdb.Source")
                // Note: "db" and "ts_ns" fields are already included by commonSchemaBuilder()
                .field(FIELD_CLUSTER, Schema.STRING_SCHEMA) // CockroachDB cluster name
                .field(FIELD_RESOLVED_TS, Schema.STRING_SCHEMA) // Resolved timestamp for consistency
                .field(FIELD_HLC, Schema.OPTIONAL_STRING_SCHEMA) // Hybrid Logical Clock timestamp
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        // Start with the common struct (includes db, ts_ms, ts_ns, snapshot, etc.)
        Struct result = super.commonStruct(sourceInfo);

        // Add CockroachDB-specific fields
        result.put(FIELD_CLUSTER, sourceInfo.cluster());
        result.put(FIELD_RESOLVED_TS, sourceInfo.resolvedTimestamp());

        // Add optional fields only if they have values
        if (sourceInfo.hlc() != null) {
            result.put(FIELD_HLC, sourceInfo.hlc());
        }
        // Do NOT add ts_ns here; it is already handled by the base struct

        return result;
    }
}
