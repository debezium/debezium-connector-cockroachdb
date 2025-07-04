/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.pipeline.txmetadata.TransactionContext;

/**
 * Unit tests for CockroachDBOffsetContext.
 *
 * @author Virag Tripathi
 */
public class CockroachDBOffsetContextTest {

    private CockroachDBOffsetContext offsetContext;
    private CockroachDBConnectorConfig config;

    @Before
    public void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        config = new CockroachDBConnectorConfig(Configuration.from(props));
        offsetContext = new CockroachDBOffsetContext(config);
    }

    @Test
    public void shouldCreateOffsetContext() {
        assertThat(offsetContext).isNotNull();
    }

    @Test
    public void shouldHandleConfiguration() {
        assertThat(config).isNotNull();
        assertThat(config.getHostname()).isEqualTo("localhost");
        assertThat(config.getDatabaseName()).isEqualTo("testdb");
    }

    @Test
    public void shouldHandleCursorOperations() {
        String testCursor = "test-cursor-123";
        offsetContext.setCursor(testCursor);
        assertThat(offsetContext.getCursor()).isEqualTo(testCursor);
    }

    @Test
    public void shouldHandleTimestampOperations() {
        Instant testTimestamp = Instant.now();
        offsetContext.setTimestamp(testTimestamp);
        assertThat(offsetContext.getTimestamp()).isEqualTo(testTimestamp);
    }

    @Test
    public void shouldHandleKafkaOffsetOperations() {
        Long testOffset = 12345L;
        offsetContext.setKafkaOffset(testOffset);
        assertThat(offsetContext.getKafkaOffset()).isEqualTo(testOffset);
    }

    @Test
    public void shouldHandleTransactionOperations() {
        TransactionContext transactionContext = new TransactionContext();
        offsetContext.setTransaction(transactionContext);
        assertThat(offsetContext.getTransactionContext()).isEqualTo(transactionContext);

        offsetContext.endTransaction();
        assertThat(offsetContext.getTransactionContext()).isNull();
    }

    @Test
    public void shouldHandleConstructorWithCursorAndTimestamp() {
        String testCursor = "test-cursor";
        Instant testTimestamp = Instant.now();
        Long testKafkaOffset = 67890L;

        CockroachDBOffsetContext context = new CockroachDBOffsetContext(config, testCursor, testTimestamp, testKafkaOffset);
        assertThat(context).isNotNull();
        assertThat(context.getCursor()).isEqualTo(testCursor);
        assertThat(context.getTimestamp()).isEqualTo(testTimestamp);
        assertThat(context.getKafkaOffset()).isEqualTo(testKafkaOffset);
    }
}