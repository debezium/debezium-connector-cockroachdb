/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.benchmark;

import java.util.List;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.cockroachdb.CockroachDBChangeRecordEmitter;
import io.debezium.connector.cockroachdb.serialization.ChangefeedSchemaParser;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * JMH benchmark for CockroachDB enriched changefeed JSON parsing.
 * Measures both the legacy ChangefeedSchemaParser path and the real production hot path
 * (JSON deserialization + column value extraction via CockroachDBChangeRecordEmitter).
 *
 * @author Virag Tripathi
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class ChangefeedJsonParsingBenchmark {

    private static final int BATCH_SIZE = 100;

    @Param({ "small", "medium", "large" })
    private String payloadSize;

    private ObjectMapper mapper;
    private String[] keyJsons;
    private String[] valueJsons;
    private JsonNode[] parsedAfterNodes;
    private Table smallTable;
    private Table mediumTable;
    private List<Column> columns;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        mapper = new ObjectMapper();
        keyJsons = new String[BATCH_SIZE];
        valueJsons = new String[BATCH_SIZE];
        parsedAfterNodes = new JsonNode[BATCH_SIZE];

        for (int i = 0; i < BATCH_SIZE; i++) {
            keyJsons[i] = generateKeyJson(i);
            valueJsons[i] = generateValueJson(i, payloadSize);
            JsonNode root = mapper.readTree(valueJsons[i]);
            parsedAfterNodes[i] = root.path("after");
        }

        smallTable = Table.editor().tableId(new TableId("testdb", "public", "items"))
                .addColumn(Column.editor().name("id").type("INT8").jdbcType(java.sql.Types.BIGINT).create())
                .addColumn(Column.editor().name("name").type("STRING").jdbcType(java.sql.Types.VARCHAR).create())
                .addColumn(Column.editor().name("price").type("DECIMAL").jdbcType(java.sql.Types.NUMERIC).create())
                .setPrimaryKeyNames("id")
                .create();

        mediumTable = Table.editor().tableId(new TableId("testdb", "public", "products"))
                .addColumn(Column.editor().name("id").type("INT8").jdbcType(java.sql.Types.BIGINT).create())
                .addColumn(Column.editor().name("name").type("STRING").jdbcType(java.sql.Types.VARCHAR).create())
                .addColumn(Column.editor().name("price").type("DECIMAL").jdbcType(java.sql.Types.NUMERIC).create())
                .addColumn(Column.editor().name("description").type("STRING").jdbcType(java.sql.Types.VARCHAR).create())
                .addColumn(Column.editor().name("category").type("STRING").jdbcType(java.sql.Types.VARCHAR).create())
                .addColumn(Column.editor().name("stock").type("INT8").jdbcType(java.sql.Types.BIGINT).create())
                .addColumn(Column.editor().name("active").type("BOOL").jdbcType(java.sql.Types.BOOLEAN).create())
                .addColumn(Column.editor().name("rating").type("FLOAT8").jdbcType(java.sql.Types.DOUBLE).create())
                .setPrimaryKeyNames("id")
                .create();

        Table tableForBenchmark = "medium".equals(payloadSize) || "large".equals(payloadSize) ? mediumTable : smallTable;
        columns = tableForBenchmark.columns();
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void parseEnrichedChangefeedEvent(Blackhole bh) throws Exception {
        for (int i = 0; i < BATCH_SIZE; i++) {
            bh.consume(ChangefeedSchemaParser.parse(keyJsons[i], valueJsons[i]));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void jsonDeserializationOnly(Blackhole bh) throws Exception {
        for (int i = 0; i < BATCH_SIZE; i++) {
            bh.consume(mapper.readTree(valueJsons[i]));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void resolvedTimestampParsing(Blackhole bh) throws Exception {
        String resolved = "{\"resolved\":\"1709312345678901234.0000000000\"}";
        for (int i = 0; i < BATCH_SIZE; i++) {
            bh.consume(ChangefeedSchemaParser.parse(null, resolved));
        }
    }

    /**
     * Measures the real production hot path: JSON parse + column value extraction.
     * This reflects the per-event cost in processChangefeedEvent(): Jackson readTree
     * followed by column extraction via the static utility method.
     */
    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void jsonParseAndColumnExtraction(Blackhole bh) throws Exception {
        for (int i = 0; i < BATCH_SIZE; i++) {
            JsonNode root = mapper.readTree(valueJsons[i]);
            JsonNode afterNode = root.path("after");
            bh.consume(CockroachDBChangeRecordEmitter.extractColumnValues(afterNode, columns));
        }
    }

    /**
     * Measures only the column value extraction cost (JSON already parsed).
     * Isolates the extractColumnValues() overhead from JSON deserialization.
     */
    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void columnExtractionOnly(Blackhole bh) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            bh.consume(CockroachDBChangeRecordEmitter.extractColumnValues(parsedAfterNodes[i], columns));
        }
    }

    private static String generateKeyJson(int id) {
        return "[" + id + "]";
    }

    private static String generateValueJson(int id, String size) {
        return switch (size) {
            case "small" -> String.format(
                    "{\"after\":{\"id\":%d,\"name\":\"item-%d\",\"price\":\"19.99\"},"
                            + "\"op\":\"c\",\"ts_ns\":%d}",
                    id, id, System.nanoTime());
            case "medium" -> String.format(
                    "{\"after\":{\"id\":%d,\"name\":\"item-%d\",\"price\":\"19.99\","
                            + "\"description\":\"A medium-length description for benchmarking purposes that contains several words\","
                            + "\"category\":\"electronics\",\"stock\":%d,\"active\":true,\"rating\":4.5},"
                            + "\"before\":{\"id\":%d,\"name\":\"item-%d\",\"price\":\"14.99\","
                            + "\"description\":\"Previous description\",\"category\":\"electronics\","
                            + "\"stock\":%d,\"active\":true,\"rating\":4.0},"
                            + "\"op\":\"u\",\"ts_ns\":%d,"
                            + "\"source\":{\"version\":\"3.5.0\",\"connector\":\"cockroachdb\","
                            + "\"name\":\"test\",\"ts_ms\":%d,\"db\":\"testdb\","
                            + "\"schema\":\"public\",\"table_name\":\"products\"}}",
                    id, id, id * 10, id, id, id * 10 - 5, System.nanoTime(), System.currentTimeMillis());
            case "large" -> String.format(
                    "{\"after\":{\"id\":%d,\"name\":\"item-%d\",\"price\":\"19.99\","
                            + "\"description\":\"" + "x".repeat(500) + "\","
                            + "\"category\":\"electronics\",\"stock\":%d,\"active\":true,\"rating\":4.5,"
                            + "\"metadata\":\"{\\\"tags\\\":[\\\"sale\\\",\\\"featured\\\"],\\\"weight\\\":2.5}\","
                            + "\"created_at\":\"2025-01-15T10:30:00Z\",\"updated_at\":\"2025-06-20T14:22:00Z\","
                            + "\"sku\":\"SKU-%06d\",\"barcode\":\"1234567890%04d\","
                            + "\"manufacturer\":\"Acme Corp\",\"warranty_months\":24,"
                            + "\"dimensions\":\"{\\\"l\\\":10,\\\"w\\\":5,\\\"h\\\":3}\","
                            + "\"color\":\"blue\",\"material\":\"aluminum\",\"country_of_origin\":\"US\"},"
                            + "\"before\":{\"id\":%d,\"name\":\"item-%d\",\"price\":\"14.99\","
                            + "\"description\":\"" + "y".repeat(500) + "\","
                            + "\"category\":\"electronics\",\"stock\":%d,\"active\":true,\"rating\":4.0,"
                            + "\"metadata\":\"{\\\"tags\\\":[\\\"sale\\\"],\\\"weight\\\":2.5}\","
                            + "\"created_at\":\"2025-01-15T10:30:00Z\",\"updated_at\":\"2025-03-10T09:15:00Z\","
                            + "\"sku\":\"SKU-%06d\",\"barcode\":\"1234567890%04d\","
                            + "\"manufacturer\":\"Acme Corp\",\"warranty_months\":24,"
                            + "\"dimensions\":\"{\\\"l\\\":10,\\\"w\\\":5,\\\"h\\\":3}\","
                            + "\"color\":\"blue\",\"material\":\"aluminum\",\"country_of_origin\":\"US\"},"
                            + "\"op\":\"u\",\"ts_ns\":%d,"
                            + "\"source\":{\"version\":\"3.5.0\",\"connector\":\"cockroachdb\","
                            + "\"name\":\"test\",\"ts_ms\":%d,\"db\":\"testdb\","
                            + "\"schema\":\"public\",\"table_name\":\"products\"}}",
                    id, id, id * 10, id, id, id, id, id * 10 - 5, id, id,
                    System.nanoTime(), System.currentTimeMillis());
            default -> throw new IllegalArgumentException("Unknown payload size: " + size);
        };
    }
}
