/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.cockroachdb.CockroachDBStreamingChangeEventSource.JobStatus;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Clock;

/**
 * Unit tests for how a {@code snapshot.mode=initial_only} run in kafka mode waits for its changefeed
 * jobs and mirrors consumer positions (debezium/dbz#2966).
 */
public class CockroachDBInitialOnlyCompletionTest {

    private static final CockroachDBConnectorConfig CONFIG = new CockroachDBConnectorConfig(Configuration.create()
            .with("database.hostname", "localhost")
            .with("database.port", "26257")
            .with("database.user", "root")
            .with("database.dbname", "testdb")
            .with("topic.prefix", "crdb")
            .with("cockroachdb.changefeed.sink.type", "kafka")
            .with("cockroachdb.changefeed.sink.uri", "kafka://kafka:9092")
            .with("snapshot.mode", "initial_only")
            .build());

    /** A source whose job status query returns the queued answers, or throws when the answer is null. */
    private static final class ScriptedSource extends CockroachDBStreamingChangeEventSource {
        private final Deque<Map<Long, JobStatus>> answers = new ArrayDeque<>();
        private final AtomicInteger reconnects = new AtomicInteger();
        private boolean reconnectFails;

        ScriptedSource() {
            super(CONFIG, null, null, Clock.system(), null);
        }

        ScriptedSource answer(Map<Long, JobStatus> statuses) {
            answers.add(statuses);
            return this;
        }

        ScriptedSource failQuery() {
            answers.add(Map.of());
            return this;
        }

        ScriptedSource failReconnect() {
            reconnectFails = true;
            return this;
        }

        @Override
        Map<Long, JobStatus> queryChangefeedJobStatuses(List<Long> jobIds) throws SQLException {
            Map<Long, JobStatus> next = answers.poll();
            if (next == null || next.isEmpty()) {
                throw new SQLException("An I/O error occurred while sending to the backend.", "08006");
            }
            return next;
        }

        @Override
        void reconnectSchemaRefreshConnection() throws SQLException {
            reconnects.incrementAndGet();
            if (reconnectFails) {
                throw new SQLException("Connection to localhost:26257 refused.", "08001");
            }
        }
    }

    private static JobStatus status(String status) {
        return new JobStatus(status, "");
    }

    @Test
    void succeedsOnlyWhenEveryChangefeedJobSucceeded() {
        ScriptedSource source = new ScriptedSource()
                .answer(Map.of(1L, status("succeeded"), 2L, status("running")))
                .answer(Map.of(1L, status("succeeded"), 2L, status("succeeded")));

        assertThat(source.changefeedJobsSucceeded(List.of(1L, 2L))).isFalse();
        assertThat(source.changefeedJobsSucceeded(List.of(1L, 2L))).isTrue();
    }

    @Test
    void failsTheRunWhenAChangefeedJobFails() {
        ScriptedSource source = new ScriptedSource()
                .answer(Map.of(7L, new JobStatus("failed", "kafka: client has run out of available brokers")));

        assertThatThrownBy(() -> source.changefeedJobsSucceeded(List.of(7L)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("7")
                .hasMessageContaining("failed")
                .hasMessageContaining("kafka: client has run out of available brokers");
    }

    @Test
    void logsTheChangefeedJobsItIsWaitingFor() {
        LogInterceptor logs = new LogInterceptor(CockroachDBStreamingChangeEventSource.class);
        ScriptedSource source = new ScriptedSource().answer(Map.of(3L, status("running")));

        assertThat(source.changefeedJobsSucceeded(List.of(3L))).isFalse();
        assertThat(logs.containsMessage("3=running")).isTrue();
    }

    @Test
    void warnsWhenAChangefeedJobIsPaused() {
        // A paused job never finishes on its own, so the operator has to see it.
        LogInterceptor logs = new LogInterceptor(CockroachDBStreamingChangeEventSource.class);
        ScriptedSource source = new ScriptedSource().answer(Map.of(4L, status("paused")));

        assertThat(source.changefeedJobsSucceeded(List.of(4L))).isFalse();
        assertThat(logs.containsWarnMessage("RESUME JOB 4")).isTrue();
    }

    @Test
    void retriesTheStatusQueryOnceAfterReconnecting() {
        // For example a node restart behind a load balancer: the new connection reaches another node.
        ScriptedSource source = new ScriptedSource()
                .failQuery()
                .answer(Map.of(5L, status("succeeded")));

        assertThat(source.changefeedJobsSucceeded(List.of(5L))).isTrue();
        assertThat(source.reconnects.get()).isEqualTo(1);
    }

    @Test
    void failsTheTaskWhenTheStatusQueryFailsAgainAfterReconnecting() {
        // Retrying every second would hide a permanent failure. With the SQLException as the cause, the
        // error handler treats the failure as retriable, so the runtime restarts the task with its backoff.
        ScriptedSource source = new ScriptedSource()
                .failQuery()
                .failQuery();

        assertThatThrownBy(() -> source.changefeedJobsSucceeded(List.of(5L)))
                .isInstanceOf(DebeziumException.class)
                .hasCauseInstanceOf(SQLException.class);
        assertThat(source.reconnects.get()).isEqualTo(1);
    }

    @Test
    void failsTheTaskWhenReconnectingFails() {
        ScriptedSource source = new ScriptedSource()
                .failQuery()
                .failReconnect();

        assertThatThrownBy(() -> source.changefeedJobsSucceeded(List.of(5L)))
                .isInstanceOf(DebeziumException.class)
                .hasCauseInstanceOf(SQLException.class);
    }

    @Test
    void mirrorsOnlyPositionsStoredInTheDebeziumOffset() {
        // In initial_only mode the latest record is held back, so its position is not stored yet and
        // must not be mirrored to the consumer group either.
        CockroachDBOffsetContext offsetContext = new CockroachDBOffsetContext(CONFIG);
        offsetContext.setConsumerOffset(CockroachDBStreamingChangeEventSource.kafkaConsumerOffsetKey("orders", 0), 5L);
        TopicPartition dispatched = new TopicPartition("orders", 0);
        TopicPartition heldBackOnly = new TopicPartition("orders", 1);

        Map<TopicPartition, OffsetAndMetadata> positions = CockroachDBStreamingChangeEventSource.dispatchedPositions(
                List.of(dispatched, heldBackOnly), offsetContext);

        assertThat(positions).containsOnlyKeys(dispatched);
        assertThat(positions.get(dispatched).offset()).isEqualTo(6L);
    }
}
