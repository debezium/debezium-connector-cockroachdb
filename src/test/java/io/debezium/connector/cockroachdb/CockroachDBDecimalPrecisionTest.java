/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.connector.cockroachdb.serialization.ChangefeedJsonMapper;
import io.debezium.relational.Column;

/**
 * Regression test for DECIMAL precision loss.
 *
 * <p>Changefeed JSON carries DECIMAL values as JSON numbers with full precision. Parsing them
 * into a Java double silently rounds anything above roughly 15 to 17 significant digits, so the
 * emitted string no longer matches the source value. The payloads below are exactly what a
 * CockroachDB v25.4 changefeed emits for DECIMAL(28,18), DECIMAL(16,6), and DECIMAL(14,4)
 * columns; the connector must emit the same digits it received.</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBDecimalPrecisionTest {

    private static final String CHANGEFEED_AFTER = "{"
            + "\"id\": 1, "
            + "\"trade_dt_qty\": 9999999999.999999999000000000, "
            + "\"cost_basis\": 9999999999.999999, "
            + "\"seg_memo_qty\": 9999.9999"
            + "}";

    @Test
    void decimalValuesRetainSourcePrecision() throws Exception {
        JsonNode after = ChangefeedJsonMapper.create().readTree(CHANGEFEED_AFTER);
        List<Column> columns = List.of(
                column("id", "INT8", Types.BIGINT),
                column("trade_dt_qty", "DECIMAL", Types.NUMERIC),
                column("cost_basis", "DECIMAL", Types.NUMERIC),
                column("seg_memo_qty", "DECIMAL", Types.NUMERIC));

        Object[] values = CockroachDBChangeRecordEmitter.extractColumnValues(after, columns);

        assertThat(values[0]).isEqualTo(1L);
        assertThat(values[1]).isEqualTo("9999999999.999999999000000000");
        assertThat(values[2]).isEqualTo("9999999999.999999");
        assertThat(values[3]).isEqualTo("9999.9999");
    }

    @Test
    void decAliasRetainsSourcePrecision() throws Exception {
        JsonNode node = ChangefeedJsonMapper.create().readTree("{\"v\": 9999999999.999999999}");
        Object[] values = CockroachDBChangeRecordEmitter.extractColumnValues(
                node, List.of(column("v", "DEC", Types.NUMERIC)));
        assertThat(values[0]).isEqualTo("9999999999.999999999");
    }

    @Test
    void decimalValuesAvoidScientificNotation() throws Exception {
        JsonNode node = ChangefeedJsonMapper.create().readTree(
                "{\"tiny\": 0.000000000000000001, \"large\": 12345678901234567890.123456789}");
        Object[] values = CockroachDBChangeRecordEmitter.extractColumnValues(
                node, List.of(
                        column("tiny", "DECIMAL", Types.NUMERIC),
                        column("large", "NUMERIC", Types.NUMERIC)));
        assertThat(values[0]).isEqualTo("0.000000000000000001");
        assertThat(values[1]).isEqualTo("12345678901234567890.123456789");
    }

    private static Column column(String name, String typeName, int jdbcType) {
        return Column.editor()
                .name(name)
                .type(typeName)
                .jdbcType(jdbcType)
                .optional(true)
                .create();
    }
}
