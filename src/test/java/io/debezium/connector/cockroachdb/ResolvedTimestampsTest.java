/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

/**
 * Tests which resolved timestamp becomes the stored cursor when the captured tables report their own.
 */
class ResolvedTimestampsTest {

    private static final TableId ORDERS = new TableId("testdb", "public", "orders");
    private static final TableId ITEMS = new TableId("testdb", "public", "items");
    private static final TableId STOCK = new TableId("testdb", "public", "stock");

    @Test
    void tellsTheResolvedTimestampThatEveryTableHasReached() {
        final ResolvedTimestamps resolved = new ResolvedTimestamps(List.of(ORDERS, ITEMS));

        // Nothing is known yet about the changefeed that covers items.
        assertThat(resolved.advance(ORDERS, "1700000000000000010.0000000000")).isNull();
        assertThat(resolved.advance(ITEMS, "1700000000000000005.0000000000")).isEqualTo("1700000000000000005.0000000000");
        // The changefeed of orders moves on and the one of items does not: the cursor stays with the slower one.
        assertThat(resolved.advance(ORDERS, "1700000000000000020.0000000000")).isEqualTo("1700000000000000005.0000000000");
        assertThat(resolved.advance(ORDERS, "1700000000000000025.0000000000")).isEqualTo("1700000000000000005.0000000000");
        assertThat(resolved.advance(ITEMS, "1700000000000000030.0000000000")).isEqualTo("1700000000000000025.0000000000");
    }

    @Test
    void followsOneChangefeedThatReportsToEveryTableInTurn() {
        // One changefeed for three tables sends each resolved timestamp to the topic of each table.
        final ResolvedTimestamps resolved = new ResolvedTimestamps(List.of(ORDERS, ITEMS, STOCK));
        resolved.advance(ORDERS, "1700000000000000010.0000000000");
        resolved.advance(ITEMS, "1700000000000000010.0000000000");
        assertThat(resolved.advance(STOCK, "1700000000000000010.0000000000")).isEqualTo("1700000000000000010.0000000000");

        // The cursor moves when the last table has delivered the next one.
        assertThat(resolved.advance(ITEMS, "1700000000000000020.0000000000")).isEqualTo("1700000000000000010.0000000000");
        assertThat(resolved.advance(ORDERS, "1700000000000000020.0000000000")).isEqualTo("1700000000000000010.0000000000");
        assertThat(resolved.advance(STOCK, "1700000000000000020.0000000000")).isEqualTo("1700000000000000020.0000000000");
        // And again, with the tables in another order and one of them a round ahead.
        assertThat(resolved.advance(STOCK, "1700000000000000030.0000000000")).isEqualTo("1700000000000000020.0000000000");
        assertThat(resolved.advance(STOCK, "1700000000000000040.0000000000")).isEqualTo("1700000000000000020.0000000000");
        assertThat(resolved.advance(ORDERS, "1700000000000000030.0000000000")).isEqualTo("1700000000000000020.0000000000");
        assertThat(resolved.advance(ITEMS, "1700000000000000030.0000000000")).isEqualTo("1700000000000000030.0000000000");
        assertThat(resolved.advance(ORDERS, "1700000000000000040.0000000000")).isEqualTo("1700000000000000030.0000000000");
        assertThat(resolved.advance(ITEMS, "1700000000000000040.0000000000")).isEqualTo("1700000000000000040.0000000000");
    }

    @Test
    void keepsTheLatestResolvedTimestampOfATableWhenAnOlderOneArrives() {
        final ResolvedTimestamps resolved = new ResolvedTimestamps(List.of(ORDERS, ITEMS));
        resolved.advance(ORDERS, "1700000000000000020.0000000000");
        resolved.advance(ITEMS, "1700000000000000030.0000000000");

        // A changefeed sends every resolved timestamp to each partition of its topics, and it can send one again.
        assertThat(resolved.advance(ORDERS, "1700000000000000010.0000000000")).isEqualTo("1700000000000000020.0000000000");
        assertThat(resolved.advance(ORDERS, "1700000000000000020.0000000000")).isEqualTo("1700000000000000020.0000000000");
        assertThat(resolved.advance(ORDERS, "1700000000000000040.0000000000")).isEqualTo("1700000000000000030.0000000000");
    }

    @Test
    void takesTheResolvedTimestampOfASingleChangefeedAsItIs() {
        // One table: every resolved timestamp is the cursor.
        assertThat(new ResolvedTimestamps(List.of(ORDERS)).advance(ORDERS, "1700000000000000010.0000000000"))
                .isEqualTo("1700000000000000010.0000000000");
        // The resolved timestamps of a sinkless changefeed carry no table. It is one changefeed for all tables.
        assertThat(new ResolvedTimestamps(List.of(ORDERS, ITEMS)).advance(null, "1700000000000000040.0000000000"))
                .isEqualTo("1700000000000000040.0000000000");
    }
}
