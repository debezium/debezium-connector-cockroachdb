/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.debezium.relational.TableId;

/**
 * Keeps the latest resolved timestamp of each captured table, and tells the resolved timestamp that all
 * of them have reached.
 * <p>A resolved timestamp says that its changefeed has emitted every change up to it. The tables can be
 * covered by more than one changefeed, and a changefeed sends its resolved timestamps to the topics of
 * its own tables. The stored cursor is where a later start creates the changefeed of a table that has
 * none, unless the offsets hold a later point for the table. It must not be ahead of any of the
 * changefeeds: when a changefeed fell behind or stopped, the changes between its own resolved timestamp
 * and the cursor would never be emitted.
 *
 * @author Virag Tripathi
 */
final class ResolvedTimestamps {

    private record Resolved(BigDecimal value, String text) {
    }

    private final Set<TableId> tables;
    private final Map<TableId, Resolved> latest = new HashMap<>();

    /** The lowest of the latest timestamps, once every table has one. */
    private Resolved lowest;
    /** How many tables are at the lowest timestamp. The lowest timestamp changes when the last of them moves on. */
    private int tablesAtLowest;

    /**
     * @param tables the tables that the connector captures
     */
    ResolvedTimestamps(Collection<TableId> tables) {
        this.tables = new HashSet<>(tables);
    }

    /**
     * Records a resolved timestamp that the topic of a table delivered.
     *
     * @param table the table, or {@code null} for a changefeed that sends its resolved timestamps without
     *            one, which is a single changefeed for all tables
     * @param resolvedTs the resolved timestamp
     * @return the resolved timestamp that every captured table has reached, or {@code null} as long as one
     *         of them has not delivered a resolved timestamp
     */
    String advance(TableId table, String resolvedTs) {
        final BigDecimal value = parse(resolvedTs);
        if (table == null || value == null || !tables.contains(table)) {
            return resolvedTs;
        }
        final Resolved known = latest.get(table);
        // A changefeed sends every resolved timestamp to each partition of its topics, and it can send one again.
        if (known == null || value.compareTo(known.value()) > 0) {
            latest.put(table, new Resolved(value, resolvedTs));
            if (latest.size() == tables.size()) {
                final boolean wasAtLowest = known != null && lowest != null && known.value().compareTo(lowest.value()) == 0;
                if (lowest == null || wasAtLowest && --tablesAtLowest == 0) {
                    findLowest();
                }
            }
        }
        return lowest == null ? null : lowest.text();
    }

    /**
     * Returns the latest resolved timestamp of every table that has delivered one: how far the changefeed
     * of the table has come, by what the connector has read from its topic.
     */
    Map<TableId, BigDecimal> latest() {
        final Map<TableId, BigDecimal> values = new HashMap<>();
        latest.forEach((table, resolved) -> values.put(table, resolved.value()));
        return values;
    }

    private void findLowest() {
        lowest = null;
        tablesAtLowest = 0;
        for (Resolved resolved : latest.values()) {
            final int order = lowest == null ? -1 : resolved.value().compareTo(lowest.value());
            if (order < 0) {
                lowest = resolved;
                tablesAtLowest = 1;
            }
            else if (order == 0) {
                tablesAtLowest++;
            }
        }
    }

    private static BigDecimal parse(String timestamp) {
        if (timestamp == null) {
            return null;
        }
        try {
            return new BigDecimal(timestamp.trim());
        }
        catch (NumberFormatException e) {
            return null;
        }
    }
}
