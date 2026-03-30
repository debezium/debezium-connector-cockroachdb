/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition.Provider;

/**
 * Provider for CockroachDB partitions.
 * Currently returns a single partition keyed by the connector's logical name (topic.prefix).
 *
 * @author Virag Tripathi
 */
public class CockroachDBPartitionProvider implements Provider<CockroachDBPartition> {

    private final String serverName;

    public CockroachDBPartitionProvider(String serverName) {
        this.serverName = Objects.requireNonNull(serverName, "serverName must not be null");
    }

    @Override
    public Set<CockroachDBPartition> getPartitions() {
        return Set.of(new CockroachDBPartition(serverName));
    }
}
