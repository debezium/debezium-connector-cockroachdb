/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the changefeed reuse option checks. CockroachDB fixes changefeed options at
 * creation time, so a reused changefeed must be validated against the connector configuration;
 * silently reusing a changefeed without the {@code diff} option while
 * {@code cockroachdb.changefeed.include.diff} is enabled leaves update events without a before
 * image.
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangefeedReuseTest {

    private static final String WITH_DIFF = "CREATE CHANGEFEED FOR TABLE reprodb.public.acct INTO 'kafka://kafka:9092?topic_prefix=repro.' "
            + "WITH OPTIONS (diff, enriched_properties = 'source,schema', envelope = 'enriched', resolved = '3s')";
    private static final String WITHOUT_DIFF = "CREATE CHANGEFEED FOR TABLE reprodb.public.acct INTO 'kafka://kafka:9092?topic_prefix=repro.' "
            + "WITH OPTIONS (enriched_properties = 'source,schema', envelope = 'enriched', resolved = '3s')";

    @Test
    void detectsDiffOptionInJobDescription() {
        assertThat(CockroachDBStreamingChangeEventSource.changefeedHasDiffOption(WITH_DIFF)).isTrue();
    }

    @Test
    void detectsMissingDiffOptionInJobDescription() {
        assertThat(CockroachDBStreamingChangeEventSource.changefeedHasDiffOption(WITHOUT_DIFF)).isFalse();
    }

    @Test
    void diffCheckDoesNotMatchTableNamesContainingDiff() {
        String description = "CREATE CHANGEFEED FOR TABLE demodb.public.diff_audit INTO 'kafka://kafka:9092?topic_prefix=crdb.' "
                + "WITH OPTIONS (envelope = 'enriched', resolved = '3s')";
        assertThat(CockroachDBStreamingChangeEventSource.changefeedHasDiffOption(description)).isFalse();
    }

    @Test
    void diffCheckHandlesNullAndMalformedDescriptions() {
        assertThat(CockroachDBStreamingChangeEventSource.changefeedHasDiffOption(null)).isFalse();
        assertThat(CockroachDBStreamingChangeEventSource.changefeedHasDiffOption("not a changefeed description")).isFalse();
    }
}
