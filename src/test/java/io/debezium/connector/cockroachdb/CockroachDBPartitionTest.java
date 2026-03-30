/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CockroachDBPartition} and {@link CockroachDBPartitionProvider}.
 *
 * @author Virag Tripathi
 */
public class CockroachDBPartitionTest {

    @Test
    public void shouldReturnServerNameInSourcePartition() {
        CockroachDBPartition partition = new CockroachDBPartition("myserver");
        Map<String, String> sourcePartition = partition.getSourcePartition();

        assertThat(sourcePartition).containsEntry("server", "myserver");
        assertThat(sourcePartition).hasSize(1);
    }

    @Test
    public void shouldUseTopicPrefixAsPartitionKey() {
        CockroachDBPartition partition = new CockroachDBPartition("crdb-prod");
        assertThat(partition.getSourcePartition().get("server")).isEqualTo("crdb-prod");
    }

    @Test
    public void shouldRejectNullServerName() {
        assertThatThrownBy(() -> new CockroachDBPartition(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void shouldBeEqualForSameServerName() {
        CockroachDBPartition p1 = new CockroachDBPartition("myserver");
        CockroachDBPartition p2 = new CockroachDBPartition("myserver");

        assertThat(p1).isEqualTo(p2);
        assertThat(p1.hashCode()).isEqualTo(p2.hashCode());
    }

    @Test
    public void shouldNotBeEqualForDifferentServerName() {
        CockroachDBPartition p1 = new CockroachDBPartition("server-a");
        CockroachDBPartition p2 = new CockroachDBPartition("server-b");

        assertThat(p1).isNotEqualTo(p2);
    }

    @Test
    public void shouldProviderReturnSinglePartition() {
        CockroachDBPartitionProvider provider = new CockroachDBPartitionProvider("myserver");
        Set<CockroachDBPartition> partitions = provider.getPartitions();

        assertThat(partitions).hasSize(1);
        CockroachDBPartition partition = partitions.iterator().next();
        assertThat(partition.getSourcePartition().get("server")).isEqualTo("myserver");
    }
}
