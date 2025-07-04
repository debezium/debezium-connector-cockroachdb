/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for CockroachDBConnectorTask.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorTaskTest {

    private CockroachDBConnectorTask task;
    private Map<String, String> config;

    @Before
    public void setUp() {
        task = new CockroachDBConnectorTask();
        config = new HashMap<>();
        config.put("database.hostname", "localhost");
        config.put("database.port", "26257");
        config.put("database.user", "root");
        config.put("database.password", "");
        config.put("database.dbname", "testdb");
        config.put("database.server.name", "test-server");
        config.put("topic.prefix", "test");
    }

    @Test
    public void shouldHaveCorrectVersion() {
        String version = task.version();
        assertThat(version).isNotNull();
        assertThat(version).isNotEmpty();
    }

    @Test
    public void shouldStopSuccessfully() {
        // Test that stop doesn't throw exceptions
        task.stop();
        assertThat(task).isNotNull();
    }
}