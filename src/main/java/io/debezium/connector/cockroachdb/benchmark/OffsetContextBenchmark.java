/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.benchmark;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.debezium.config.Configuration;
import io.debezium.connector.cockroachdb.CockroachDBConnectorConfig;
import io.debezium.connector.cockroachdb.CockroachDBOffsetContext;

/**
 * JMH benchmark for {@link CockroachDBOffsetContext} serialization and deserialization.
 * Measures the cost of loading offset context from stored offsets, including
 * incremental snapshot context restoration.
 *
 * @author Virag Tripathi
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class OffsetContextBenchmark {

    private static final int OP_COUNT = 100;

    private CockroachDBOffsetContext.Loader loader;
    private Map<String, Object>[] storedOffsets;
    private CockroachDBOffsetContext context;

    @SuppressWarnings("unchecked")
    @Setup(Level.Trial)
    public void setup() {
        Configuration config = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", "26257")
                .with("database.user", "root")
                .with("database.dbname", "defaultdb")
                .with("topic.prefix", "bench")
                .with("database.server.name", "bench")
                .build();

        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);
        loader = new CockroachDBOffsetContext.Loader(connectorConfig);
        context = new CockroachDBOffsetContext(connectorConfig);

        storedOffsets = new Map[OP_COUNT];
        for (int i = 0; i < OP_COUNT; i++) {
            Map<String, Object> offset = new HashMap<>();
            offset.put(CockroachDBOffsetContext.CURSOR, "170931234567890" + i + ".0000000000");
            offset.put(CockroachDBOffsetContext.TIMESTAMP, String.valueOf(System.currentTimeMillis() - i * 1000));
            offset.put("snapshot_completed", "true");
            storedOffsets[i] = offset;
        }
    }

    @Benchmark
    @OperationsPerInvocation(OP_COUNT)
    public void loadOffsetContext(Blackhole bh) {
        for (int i = 0; i < OP_COUNT; i++) {
            bh.consume(loader.load(storedOffsets[i]));
        }
    }

    @Benchmark
    @OperationsPerInvocation(OP_COUNT)
    public void serializeOffsetContext(Blackhole bh) {
        for (int i = 0; i < OP_COUNT; i++) {
            context.setCursor("170931234567890" + i + ".0000000000");
            bh.consume(context.getOffset());
        }
    }
}
