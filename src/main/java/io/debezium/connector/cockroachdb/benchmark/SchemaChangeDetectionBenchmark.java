/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.benchmark;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.cockroachdb.CockroachDBStreamingChangeEventSource;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * JMH benchmark for schema change detection logic.
 * Measures the cost of comparing incoming event fields against the registered table schema,
 * which runs on every changefeed event during streaming.
 *
 * @author Virag Tripathi
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class SchemaChangeDetectionBenchmark {

    private static final int OP_COUNT = 100;

    @Param({ "5", "15", "30" })
    private int columnCount;

    private JsonNode[] matchingNodes;
    private JsonNode[] mismatchNodes;
    private Table table;
    private Method hasSchemaChangedMethod;

    private Object previousLogLevel;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        Logger logger = LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);
        try {
            Class<?> logbackLoggerClass = Class.forName("ch.qos.logback.classic.Logger");
            Class<?> logbackLevelClass = Class.forName("ch.qos.logback.classic.Level");
            if (logbackLoggerClass.isInstance(logger)) {
                previousLogLevel = logbackLoggerClass.getMethod("getLevel").invoke(logger);
                Object offLevel = logbackLevelClass.getField("OFF").get(null);
                logbackLoggerClass.getMethod("setLevel", logbackLevelClass).invoke(logger, offLevel);
            }
            else {
                previousLogLevel = null;
            }
        }
        catch (ReflectiveOperationException e) {
            previousLogLevel = null;
        }

        ObjectMapper mapper = new ObjectMapper();

        hasSchemaChangedMethod = CockroachDBStreamingChangeEventSource.class
                .getDeclaredMethod("hasSchemaChanged", JsonNode.class, Table.class);
        hasSchemaChangedMethod.setAccessible(true);

        TableId tableId = new TableId("testdb", "public", "bench_table");
        io.debezium.relational.TableEditor editor = Table.editor()
                .tableId(tableId);
        for (int c = 0; c < columnCount; c++) {
            editor.addColumn(Column.editor()
                    .name("col_" + c)
                    .type("STRING")
                    .jdbcType(java.sql.Types.VARCHAR)
                    .optional(c > 0)
                    .create());
        }
        editor.setPrimaryKeyNames("col_0");
        table = editor.create();

        matchingNodes = new JsonNode[OP_COUNT];
        mismatchNodes = new JsonNode[OP_COUNT];
        for (int i = 0; i < OP_COUNT; i++) {
            StringBuilder matching = new StringBuilder("{");
            StringBuilder mismatch = new StringBuilder("{");
            for (int c = 0; c < columnCount; c++) {
                if (c > 0) {
                    matching.append(",");
                    mismatch.append(",");
                }
                matching.append(String.format("\"col_%d\":\"val_%d_%d\"", c, i, c));
                mismatch.append(String.format("\"col_%d\":\"val_%d_%d\"", c, i, c));
            }
            matching.append("}");
            mismatch.append(",\"new_column\":\"surprise\"}");
            matchingNodes[i] = mapper.readTree(matching.toString());
            mismatchNodes[i] = mapper.readTree(mismatch.toString());
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (previousLogLevel != null) {
            try {
                Logger logger = LoggerFactory.getLogger(CockroachDBStreamingChangeEventSource.class);
                Class<?> logbackLoggerClass = Class.forName("ch.qos.logback.classic.Logger");
                Class<?> logbackLevelClass = Class.forName("ch.qos.logback.classic.Level");
                logbackLoggerClass.getMethod("setLevel", logbackLevelClass).invoke(logger, previousLogLevel);
            }
            catch (ReflectiveOperationException e) {
                // best-effort restore
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(OP_COUNT)
    public void schemaUnchanged(Blackhole bh) throws Exception {
        for (int i = 0; i < OP_COUNT; i++) {
            bh.consume(hasSchemaChangedMethod.invoke(null, matchingNodes[i], table));
        }
    }

    @Benchmark
    @OperationsPerInvocation(OP_COUNT)
    public void schemaChanged(Blackhole bh) throws Exception {
        for (int i = 0; i < OP_COUNT; i++) {
            bh.consume(hasSchemaChangedMethod.invoke(null, mismatchNodes[i], table));
        }
    }
}
