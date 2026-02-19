[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-connector-cockroachdb/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-connector-cockroachdb)
[![Build Status](https://github.com/debezium/debezium-connector-cockroachdb/workflows/CI/badge.svg)](https://github.com/debezium/debezium-connector-cockroachdb/actions)
[![Community](https://img.shields.io/badge/Community-Zulip-blue.svg)](https://debezium.zulipchat.com/#narrow/channel/510960-community-cockroachdb)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Connector for CockroachDB

A [Debezium](https://debezium.io/) connector for capturing changes from [CockroachDB](https://www.cockroachlabs.com/) databases.

## Overview

The Debezium CockroachDB connector processes row-level changes from CockroachDB databases that have been captured and streamed to Apache Kafka topics by CockroachDB's native [changefeed mechanism](https://www.cockroachlabs.com/docs/stable/change-data-capture-overview).

The connector works in a two-stage process:

**CockroachDB Changefeed Stage:** CockroachDB's native changefeed mechanism captures row-level changes from the database and streams them directly to configured sinks (Kafka, webhook, cloud storage, etc.) in real-time.

**Debezium Processing Stage:** The Debezium connector consumes these changefeed events from Kafka topics and processes them through Debezium's event processing pipeline, converting them into standardized Debezium change events with enriched metadata.

This architecture leverages CockroachDB's reliable changefeed delivery mechanism while providing the benefits of Debezium's event processing capabilities, including schema evolution, event transformation, and integration with the broader Debezium ecosystem.

**Status**: This connector is currently in incubation phase and is being developed and tested.

## Prerequisites

* CockroachDB v25.2+ with [rangefeed enabled](https://www.cockroachlabs.com/docs/stable/create-and-configure-changefeeds.html#enable-rangefeeds) (enriched envelope support introduced in v25.2; tested with v26.1.0)
* CockroachDB v24.2+ for [pgvector-compatible VECTOR type](https://www.cockroachlabs.com/docs/stable/vector) support
* Kafka Connect
* JDK 21+
* Maven 3.9.8 or later

## Building

```bash
./mvnw clean package -Passembly
```

## Configuration

Example connector configuration:

```json
{
  "name": "cockroachdb-connector",
  "config": {
    "connector.class": "io.debezium.connector.cockroachdb.CockroachDBConnector",
    "database.hostname": "cockroachdb",
    "database.port": "26257",
    "database.user": "testuser",
    "database.password": "",
    "database.dbname": "testdb",
    "database.server.name": "cockroachdb",
    "table.include.list": "public.products",
    "cockroachdb.changefeed.envelope": "enriched",
    "cockroachdb.changefeed.enriched.properties": "source,schema",
    "cockroachdb.changefeed.sink.type": "kafka",
    "cockroachdb.changefeed.sink.uri": "kafka://kafka-test:9092",
    "cockroachdb.changefeed.sink.topic.prefix": "",
    "cockroachdb.changefeed.sink.options": "",
    "cockroachdb.changefeed.resolved.interval": "10s",
    "cockroachdb.changefeed.include.updated": true,
    "cockroachdb.changefeed.include.diff": true,
    "cockroachdb.changefeed.cursor": "now",
    "cockroachdb.changefeed.batch.size": 1000,
    "cockroachdb.changefeed.poll.interval.ms": 100,
    "connection.timeout.ms": 30000,
    "connection.retry.delay.ms": 1000,
    "connection.max.retries": 3
  }
}
```

### Configuration Options

#### Database Connection

| Option                 | Default | Description                         |
|------------------------|---------|-------------------------------------|
| `database.hostname`    | -       | CockroachDB host                    |
| `database.port`        | 26257   | CockroachDB port                    |
| `database.user`        | -       | Database user                       |
| `database.password`    | -       | Database password                   |
| `database.dbname`      | -       | Database name                       |
| `database.server.name` | -       | Unique server name for topic prefix |

#### Table Selection

| Option               | Default | Description                               |
|----------------------|---------|-------------------------------------------|
| `table.include.list` | -       | Comma-separated list of tables to monitor |

#### Changefeed Configuration

| Option                                       | Default  | Description                                   |
|----------------------------------------------|----------|-----------------------------------------------|
| `cockroachdb.changefeed.envelope`            | enriched | Envelope type: enriched, wrapped, bare        |
| `cockroachdb.changefeed.enriched.properties` | source   | Comma-separated enriched properties           |
| `cockroachdb.changefeed.sink.type`           | kafka    | Sink type (kafka, webhook, pubsub, etc.)      |
| `cockroachdb.changefeed.sink.uri`            | -        | Sink URI (required). e.g. `kafka://host:port` |
| `cockroachdb.changefeed.sink.topic.prefix`   | ""       | Optional prefix for sink topic names          |
| `cockroachdb.changefeed.sink.options`        | ""       | Additional sink options in key=value format   |
| `cockroachdb.changefeed.resolved.interval`   | 10s      | Resolved timestamp interval                   |
| `cockroachdb.changefeed.include.updated`     | false    | Include updated column information            |
| `cockroachdb.changefeed.include.diff`        | false    | Include before/after diff information         |
| `cockroachdb.changefeed.cursor`              | now      | Start cursor position                         |
| `cockroachdb.changefeed.batch.size`          | 1000     | Batch size for changefeed processing          |
| `cockroachdb.changefeed.poll.interval.ms`    | 100      | Poll interval in milliseconds                 |

#### Kafka Consumer Configuration (Advanced)

| Option                                                 | Default      | Description                                      |
|--------------------------------------------------------|--------------|--------------------------------------------------|
| `cockroachdb.changefeed.kafka.consumer.group.prefix`   | cockroachdb  | Prefix for Kafka consumer group ID               |
| `cockroachdb.changefeed.kafka.poll.timeout.ms`         | 1000         | Kafka consumer poll timeout in milliseconds      |
| `cockroachdb.changefeed.kafka.auto.offset.reset`       | earliest     | Kafka consumer auto offset reset policy          |

#### Connection Settings

| Option                                  | Default | Description                                 |
|-----------------------------------------|---------|---------------------------------------------|
| `connection.timeout.ms`                 | 30000   | Connection timeout in milliseconds          |
| `connection.retry.delay.ms`             | 1000    | Delay between connection retries in ms      |
| `connection.max.retries`                | 3       | Maximum number of connection retry attempts |
| `connection.validation.timeout.seconds` | 5       | Timeout for validating JDBC connections     |
| `cockroachdb.skip.permission.check`     | false   | Skip changefeed permission check at startup |

## Supported Data Types

The connector maps CockroachDB column types to Kafka Connect schema types:

| CockroachDB Type                      | Kafka Connect Type                | Notes                                   |
|---------------------------------------|-----------------------------------|-----------------------------------------|
| `BOOL`                                | `BOOLEAN`                         |                                         |
| `INT2`, `SMALLINT`                    | `INT16`                           |                                         |
| `INT4`, `INT`, `INTEGER`              | `INT32`                           |                                         |
| `INT8`, `BIGINT`, `SERIAL`            | `INT64`                           |                                         |
| `FLOAT4`, `REAL`                      | `FLOAT32`                         |                                         |
| `FLOAT8`, `DOUBLE PRECISION`          | `FLOAT64`                         |                                         |
| `NUMERIC`, `DECIMAL`                  | `STRING`                          | Preserves precision                     |
| `VARCHAR`, `TEXT`, `STRING`           | `STRING`                          |                                         |
| `BYTEA`, `BYTES`                      | `BYTES`                           |                                         |
| `DATE`                                | `io.debezium.time.Date`           | Days since epoch                        |
| `TIMESTAMP`, `TIMESTAMPTZ`            | `io.debezium.time.MicroTimestamp` | Microseconds since epoch                |
| `JSON`, `JSONB`                       | `io.debezium.data.Json`           |                                         |
| `UUID`                                | `io.debezium.data.Uuid`           |                                         |
| `VECTOR`                              | `io.debezium.data.DoubleVector`   | pgvector-compatible (CockroachDB 24.2+) |
| `GEOGRAPHY`, `GEOMETRY`               | `STRING`                          | Spatial types                           |
| `INET`                                | `STRING`                          |                                         |
| `INTERVAL`                            | `STRING`                          |                                         |
| `ENUM`                                | `STRING`                          |                                         |
| Array types (`INT[]`, `TEXT[]`, etc.) | `STRING`                          | JSON array representation               |

## Event Format

Events are produced in Debezium's enriched envelope format. For details on the changefeed message format, see the [CockroachDB changefeed messages documentation](https://www.cockroachlabs.com/docs/stable/changefeed-messages).

```json
{
  "before": null,
  "after": {
    "id": "...",
    "name": "...",
    "...": "..."
  },
  "source": {
    "changefeed_sink": "kafka",
    "cluster_id": "...",
    "database_name": "testdb",
    "table_name": "products",
    "...": "..."
  },
  "op": "c",
  "ts_ns": 1751407136710963868
}
```

## SSL/TLS with CockroachDB Cloud

For CockroachDB Cloud (Serverless or Dedicated), use `verify-full` SSL mode:

```json
{
  "database.hostname": "my-cluster-1234.abc.cockroachlabs.cloud",
  "database.port": "26257",
  "database.sslmode": "verify-full",
  "database.user": "myuser",
  "database.password": "mypassword",
  "database.dbname": "defaultdb"
}
```

CockroachDB Cloud clusters use publicly trusted certificates, so no `sslrootcert` is needed.

## Testing

Run all unit tests:

```bash
./mvnw clean test
```

Run integration tests (requires Docker for Testcontainers):

```bash
./mvnw clean test -Dtest="*IT"
```

Run against a specific CockroachDB version (default is v26.1.0):

```bash
./mvnw clean test -Dtest="*IT" -Dcockroachdb.version=v25.2.3
```

Run CockroachDB Cloud connectivity tests (requires a Cloud instance):

```bash
CRDB_CLOUD_URL="postgresql://user:pass@host:26257/defaultdb?sslmode=verify-full" \
  ./mvnw test -Dtest=CockroachDBCloudConnectionIT
```

The Cloud IT is guarded by `@EnabledIfEnvironmentVariable` and will be skipped in CI when the env var is absent.

For docker-compose based testing with a specific version:

```bash
COCKROACHDB_VERSION=v25.2.3 docker-compose -f src/test/scripts/docker-compose.yml up
```

## Known Limitations

- **No snapshot support**: The connector skips the snapshot phase and starts from the current changefeed position. Initial table load (`AS OF SYSTEM TIME`) is planned.
- **Sequential table processing**: Tables are processed one at a time during changefeed creation. Changefeed event consumption is topic-based.
- **No schema change detection**: DDL changes (ALTER TABLE) are not automatically detected. Restart the connector after schema changes.
- **No incremental snapshots**: Signal-based incremental snapshots are not yet supported.
- **No heartbeat support**: Heartbeat events are not emitted.
- **Kafka-only sink**: Only Kafka sinks are supported. Webhook, Pub/Sub, and cloud storage sinks are planned.

## Troubleshooting

- **Permission Errors**: Ensure [CHANGEFEED and SELECT privileges](https://www.cockroachlabs.com/docs/stable/grant#supported-privileges) are granted on all monitored tables.
- **Rangefeed Disabled**: Enable with `SET CLUSTER SETTING kv.rangefeed.enabled = true;`
- **No Events**: Check connector logs and [changefeed job status](https://www.cockroachlabs.com/docs/stable/monitor-and-debug-changefeeds.html#monitor-a-changefeed).
- **Configuration Issues**: Verify all required [changefeed parameters](https://www.cockroachlabs.com/docs/stable/create-and-configure-changefeeds#parameters) are properly configured.
