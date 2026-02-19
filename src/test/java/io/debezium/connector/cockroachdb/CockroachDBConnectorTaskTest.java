/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for CockroachDB connector task.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorTaskTest {

    @Test
    public void shouldHaveCorrectVersion() {
        CockroachDBConnectorTask task = new CockroachDBConnectorTask();
        String version = task.version();

        assertThat(version).isNotNull();
        assertThat(version).isNotEmpty();
    }

    @Test
    public void shouldExtendBaseSourceTask() {
        CockroachDBConnectorTask task = new CockroachDBConnectorTask();
        assertThat(task).isInstanceOf(io.debezium.connector.common.BaseSourceTask.class);
    }
}
