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
 * Uses {@code topic.prefix} as the partition key so that multiple connector
 * instances targeting different databases maintain independent offset tracking.
 *
 * @author Virag Tripathi
 */
public class CockroachDBPartition implements Partition {

    private final String serverName;

    public CockroachDBPartition(String serverName) {
        this.serverName = Objects.requireNonNull(serverName, "serverName must not be null");
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Map.of("server", serverName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CockroachDBPartition)) {
            return false;
        }
        CockroachDBPartition that = (CockroachDBPartition) o;
        return Objects.equals(serverName, that.serverName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverName);
    }
}
