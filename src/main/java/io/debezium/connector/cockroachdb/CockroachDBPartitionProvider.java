/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.util.Set;

import io.debezium.pipeline.spi.Partition.Provider;

/**
 * Provider for CockroachDB partitions.
 * Currently returns a single partition for the entire database.
 *
 * @author Virag Tripathi
 */
public class CockroachDBPartitionProvider implements Provider<CockroachDBPartition> {

    @Override
    public Set<CockroachDBPartition> getPartitions() {
        return Set.of(new CockroachDBPartition());
    }
}
