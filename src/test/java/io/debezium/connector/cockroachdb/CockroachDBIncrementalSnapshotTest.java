/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.relational.TableId;

/**
 * Unit tests for CockroachDB incremental snapshot support.
 *
 * @author Virag Tripathi
 */
public class CockroachDBIncrementalSnapshotTest {

    private CockroachDBConnectorConfig createConfig(Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("topic.prefix", "test");
        props.put("database.server.name", "test");
        props.putAll(overrides);
        return new CockroachDBConnectorConfig(Configuration.from(props));
    }

    @Test
    public void shouldReturnEmptyWhenNoSignalCollectionConfigured() {
        CockroachDBConnectorConfig config = createConfig(Map.of());
        assertThat(config.getSignalingDataCollectionIds()).isEmpty();
    }

    @Test
    public void shouldParseSignalDataCollection() {
        CockroachDBConnectorConfig config = createConfig(Map.of(
                "signal.data.collection", "testdb.public.debezium_signal"));
        assertThat(config.getSignalingDataCollectionIds()).isNotEmpty();
        assertThat(config.getSignalingDataCollectionIds().get(0)).isEqualTo("testdb.public.debezium_signal");
    }

    @Test
    public void shouldCreateIncrementalSnapshotContextInOffsetContext() {
        CockroachDBConnectorConfig config = createConfig(Map.of());
        CockroachDBOffsetContext offsetContext = new CockroachDBOffsetContext(config);
        assertThat(offsetContext.getIncrementalSnapshotContext()).isNotNull();
    }

    @Test
    public void shouldLoadIncrementalSnapshotContextFromOffset() {
        CockroachDBConnectorConfig config = createConfig(Map.of());
        CockroachDBOffsetContext.Loader loader = new CockroachDBOffsetContext.Loader(config);

        Map<String, Object> offset = new HashMap<>();
        offset.put("offset.cursor", "test-cursor");
        offset.put("offset.timestamp", "1234567890000");
        offset.put("snapshot_completed", "true");

        CockroachDBOffsetContext context = loader.load(offset);
        assertThat(context.getIncrementalSnapshotContext()).isNotNull();
        assertThat(context.getCursor()).isEqualTo("test-cursor");
    }

    @Test
    public void shouldPreserveIncrementalSnapshotContextAcrossConstructors() {
        CockroachDBConnectorConfig config = createConfig(Map.of());
        IncrementalSnapshotContext<TableId> snapContext = new SignalBasedIncrementalSnapshotContext<>(false);

        CockroachDBOffsetContext context = new CockroachDBOffsetContext(
                config, "cursor", java.time.Instant.now(), null, snapContext);
        assertThat(context.getIncrementalSnapshotContext()).isSameAs(snapContext);
    }

    @Test
    public void shouldHaveNullsSortLastForCockroachDB() throws Exception {
        CockroachDBConnectorConfig config = createConfig(Map.of());
        io.debezium.connector.cockroachdb.connection.CockroachDBConnection conn = new io.debezium.connector.cockroachdb.connection.CockroachDBConnection(config);
        Optional<Boolean> nullsSortLast = conn.nullsSortLast();
        assertThat(nullsSortLast).isPresent();
        assertThat(nullsSortLast.get()).isTrue();
    }
}
