/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.relational.Column;

/**
 * Unit tests for {@link CockroachDBChangeRecordEmitter#extractColumnValues(JsonNode, List)}, the
 * mapping from an enriched changefeed row to Java values aligned with the column schema types.
 * Temporal value/normalization rules are covered by {@link CockroachDBTemporalConversionsTest}.
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangeRecordEmitterTest {

    @Test
    public void shouldExtractTemporalColumnValuesWithCorrectJavaTypes() throws Exception {
        // Real CockroachDB enriched changefeed output for each temporal type (captured from v25.4.11).
        String json = "{\"d\":\"2026-06-08\",\"id\":1,\"tm\":\"11:01:45.883\",\"tmtz\":\"11:01:45.883+02\","
                + "\"ts\":\"2026-06-08T11:01:45.883\",\"tstz\":\"2026-06-08T09:01:45.883Z\"}";
        JsonNode node = new ObjectMapper().readTree(json);
        List<Column> columns = List.of(
                Column.editor().name("ts").type("TIMESTAMP").create(),
                Column.editor().name("tstz").type("TIMESTAMPTZ").create(),
                Column.editor().name("tm").type("TIME").create(),
                Column.editor().name("tmtz").type("TIMETZ").create(),
                Column.editor().name("d").type("DATE").create());

        Object[] values = CockroachDBChangeRecordEmitter.extractColumnValues(node, columns);

        // TIMESTAMP (without tz) -> MicroTimestamp (Long micros); must not be null (the original bug).
        assertThat(values[0]).isInstanceOf(Long.class);
        // TIMESTAMPTZ -> ZonedTimestamp (String); a Z value already satisfies ISO_OFFSET_DATE_TIME.
        assertThat(values[1]).isEqualTo("2026-06-08T09:01:45.883Z");
        // TIME -> MicroTime (Long micros since midnight).
        assertThat(values[2]).isEqualTo(39_705_883_000L);
        // TIMETZ -> ZonedTime (String); the CockroachDB hour-only "+02" offset is normalized to
        // "+02:00" so it parses with the ZonedTime (ISO_OFFSET_TIME) formatter downstream.
        assertThat(values[3]).isEqualTo("11:01:45.883+02:00");
        // DATE -> Date (Integer days since epoch).
        assertThat(values[4]).isInstanceOf(Integer.class);
    }

    @Test
    public void shouldReturnNullForNullJsonColumnValues() throws Exception {
        String json = "{\"id\":1,\"tstz\":null,\"tmtz\":null}";
        JsonNode node = new ObjectMapper().readTree(json);
        List<Column> columns = List.of(
                Column.editor().name("tstz").type("TIMESTAMPTZ").create(),
                Column.editor().name("tmtz").type("TIMETZ").create());

        Object[] values = CockroachDBChangeRecordEmitter.extractColumnValues(node, columns);

        assertThat(values[0]).isNull();
        assertThat(values[1]).isNull();
    }
}
