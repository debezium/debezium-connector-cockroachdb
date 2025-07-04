/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.util.Map;
import java.util.Objects;

import io.debezium.pipeline.spi.Partition;

/**
 * Represents a logical partition of a CockroachDB changefeed stream.
 * For now, this is a stub that always returns a static key.
 * <p>
 * Future versions may support partitioning by tenant, table, etc.
 *
 * @author Virag Tripathi
 */
public class CockroachDBPartition implements Partition {

    private static final String PARTITION_KEY = "cockroachdb";

    @Override
    public Map<String, String> getSourcePartition() {
        return Map.of("server", PARTITION_KEY);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CockroachDBPartition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(PARTITION_KEY);
    }
}
