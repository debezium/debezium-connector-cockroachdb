[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/io.debezium/debezium-connector-cockroachdb.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:io.debezium%20AND%20a:debezium-connector-cockroachdb)
[![Build Status](https://github.com/debezium/debezium-connector-cockroachdb/workflows/CI/badge.svg)](https://github.com/debezium/debezium-connector-cockroachdb/actions)
[![Community](https://img.shields.io/badge/Community-Zulip-blue.svg)](https://debezium.zulipchat.com/#narrow/channel/510960-community-cockroachdb)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Connector for CockroachDB

A [Debezium](https://debezium.io/) connector for capturing changes from [CockroachDB](https://www.cockroachlabs.com/) databases.

## Overview

The Debezium CockroachDB connector processes row-level changes from CockroachDB databases that have been captured and streamed to Apache Kafka topics by CockroachDB's native [changefeed mechanism](https://www.cockroachlabs.com/docs/stable/change-data-capture-overview).

The connector uses a two-stage Kafka architecture:

1. **CockroachDB changefeed -> Intermediate Kafka**: By default the connector creates a single CockroachDB changefeed covering all configured tables (`CREATE CHANGEFEED FOR table1, table2, ...`) with the `enriched` envelope format. CockroachDB automatically routes events to per-table Kafka topics in the intermediate cluster. The enriched format includes both schema metadata and the full before/after row state.

2. **Intermediate Kafka -> Debezium -> Output Kafka**: The connector subscribes to all per-table Kafka topics in a single KafkaConsumer, routes each event to the correct table based on the topic name, transforms the enriched changefeed events into the standard Debezium envelope format (with `before`, `after`, `source`, and `op` fields), and produces them to the final output Kafka topics.

Consolidating tables into one changefeed keeps the connector to a single changefeed job, which helps stay within CockroachDB's [recommended limit](https://www.cockroachlabs.com/docs/stable/create-and-configure-changefeeds#recommendations) on the number of changefeeds per cluster (around 80 at the time of writing). However, CockroachDB also [advises against](https://www.cockroachlabs.com/docs/stable/changefeed-best-practices) putting very many tables in a single changefeed, because their performance becomes coupled. For large table counts, set `cockroachdb.changefeed.max.tables.per.changefeed` to split the tables across several changefeeds, or run multiple connector instances each capturing a related subset of tables.

**Status**: This connector is currently in incubation phase and is being developed and tested.

## Prerequisites

* CockroachDB v25.2+ with [rangefeed enabled](https://www.cockroachlabs.com/docs/stable/create-and-configure-changefeeds.html#enable-rangefeeds) (enriched envelope support introduced in v25.2)
* CockroachDB v24.2+ for [pgvector-compatible VECTOR type](https://www.cockroachlabs.com/docs/stable/vector) support
* Kafka Connect
* Java 17 or later to run the connector. It is compiled to Java 17 to match common Kafka Connect deployments (see [debezium/dbz#1922](https://issues.redhat.com/browse/DBZ-1922)).
* To build from source: JDK 21 (required by the Debezium build) and Maven 3.9.8 or later

### Required CockroachDB Privileges

The database user must have one of the following to create changefeeds:

* `CHANGEFEED` privilege on monitored tables, **or**
* `ALL` privilege on monitored tables, **or**
* Membership in the `admin` role

Additionally, the `kv.rangefeed.enabled` cluster setting must be `true` (the connector checks this at startup).
Grant `VIEWCLUSTERSETTING` if the user is not `admin` so the connector can verify the setting:

```sql
GRANT CHANGEFEED ON TABLE mydb.public.* TO myuser;
GRANT VIEWCLUSTERSETTING TO myuser;
SET CLUSTER SETTING kv.rangefeed.enabled = true;
```

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
    "topic.prefix": "cockroachdb",
    "table.include.list": "public.orders,public.customers",
    "cockroachdb.changefeed.enriched.properties": "source",
    "cockroachdb.changefeed.sink.type": "kafka",
    "cockroachdb.changefeed.sink.uri": "kafka://kafka-test:9092",
    "cockroachdb.changefeed.sink.options": "",
    "cockroachdb.changefeed.resolved.interval": "10s",
    "cockroachdb.changefeed.include.updated": true,
    "cockroachdb.changefeed.include.diff": true,
    "snapshot.mode": "initial",
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

| Option                | Default | Description                                                                                            |
|-----------------------|---------|--------------------------------------------------------------------------------------------------------|
| `schema.include.list` | -       | Comma-separated list of schemas (regex) to include. When set, only tables in matching schemas are captured. |
| `schema.exclude.list` | -       | Comma-separated list of schemas (regex) to exclude. Mutually exclusive with `schema.include.list`.     |
| `table.include.list`  | -       | Comma-separated list of fully-qualified tables (`schema.table`) to monitor. May span multiple schemas. |
| `table.exclude.list`  | -       | Comma-separated list of fully-qualified tables to exclude. Mutually exclusive with `table.include.list`. |

#### Changefeed Configuration

| Option                                       | Default  | Description                                   |
|----------------------------------------------|----------|-----------------------------------------------|
| `cockroachdb.changefeed.enriched.properties` | source   | Comma-separated enriched properties (passthrough to CockroachDB) |
| `cockroachdb.changefeed.sink.type`           | kafka    | Sink type (kafka, webhook, pubsub, etc.)      |
| `cockroachdb.changefeed.sink.uri`            | -        | Sink URI (required). e.g. `kafka://host:port`. Do not set `topic_name`/`topic_prefix` here |
| `cockroachdb.changefeed.sink.topic.prefix`   | ""       | Prefix for intermediate topic names, used verbatim: topics are `<prefix><database>.<schema>.<table>`. Include your own separator (e.g. `crdb.`). If empty, defaults to `<topic.prefix>.` |
| `cockroachdb.changefeed.max.tables.per.changefeed` | 0  | Max tables per changefeed. `0` = all in one. Set positive to split large table sets across multiple changefeeds and avoid per-table coupling |
| `cockroachdb.changefeed.sink.options`        | ""       | Additional sink options in key=value format   |
| `cockroachdb.changefeed.resolved.interval`   | 10s      | Resolved timestamp interval                   |
| `cockroachdb.changefeed.include.updated`     | false    | Include updated column information            |
| `cockroachdb.changefeed.include.diff`        | false    | Include before/after diff information         |
| `cockroachdb.changefeed.cursor`              | now      | Start cursor position                         |
| `cockroachdb.changefeed.batch.size`          | 1000     | Batch size for changefeed processing          |
| `cockroachdb.changefeed.poll.interval.ms`    | 100      | Poll interval in milliseconds                 |

#### Snapshot Configuration

The connector uses CockroachDB's native changefeed [`initial_scan`](https://www.cockroachlabs.com/docs/stable/create-changefeed#initial-scan) option to backfill existing rows instead of a separate JDBC-based snapshot phase. During the initial scan, events are marked with `op=r` (read). Once the scan completes and streaming begins, events use the standard `op=c/u/d` operation types.

| Option          | Default   | Description                                                                                                      |
|-----------------|-----------|------------------------------------------------------------------------------------------------------------------|
| `snapshot.mode` | `initial` | Controls whether existing rows are backfilled on startup. See the mapping table below for all supported modes.   |

**Snapshot mode to CockroachDB `initial_scan` mapping:**

| Snapshot Mode       | initial_scan | Behavior                                                                                             |
|---------------------|--------------|------------------------------------------------------------------------------------------------------|
| `initial` (default) | `yes` / `no` | On first start (no prior offset), backfills all rows. On restart, resumes from stored cursor.        |
| `always`            | `yes`        | Always backfills all existing rows, even on restart.                                                 |
| `initial_only`      | `only`       | Backfills all existing rows, then stops the connector. Useful for one-time data migration.           |
| `no_data` / `never` | `no`         | Skips the initial scan. Only ongoing changes are captured.                                           |
| `when_needed`       | `yes` / `no` | Like `initial`, but also re-snapshots if the stored offset is no longer valid (e.g. GC TTL expired). |

#### Incremental Snapshots

The connector supports Debezium's [signal-based incremental snapshots](https://debezium.io/documentation/reference/stable/configuration/signalling.html). This allows you to re-snapshot existing table data on demand -- without stopping the connector or missing any in-flight changes.

**Setup:**

1. Create a signaling table in CockroachDB:

```sql
CREATE TABLE debezium_signal (
    id STRING PRIMARY KEY,
    type STRING NOT NULL,
    data STRING
);
```

2. Configure the connector to monitor the signaling table:

```json
{
  "signal.data.collection": "mydb.public.debezium_signal",
  "table.include.list": "public.my_table,public.debezium_signal"
}
```

The signaling table **must** be included in `table.include.list` so the changefeed delivers signal events to the connector.

**Triggering a snapshot:**

Insert a row into the signaling table to trigger an incremental snapshot of one or more tables:

```sql
INSERT INTO debezium_signal (id, type, data) VALUES
    ('snap-1', 'execute-snapshot',
     '{"data-collections": ["mydb.public.my_table"]}');
```

The connector will re-read all rows from the specified table(s) and emit them as `op=r` (read) events, while continuing to capture any concurrent DML changes without interruption.

**Use cases:**
- Re-populate a downstream consumer that lost data
- Backfill a newly added sink or topic
- Verify source-target consistency by re-snapshotting and comparing

#### Changefeed Sink TLS / mTLS

For changefeed sinks that require TLS (or mutual TLS), point the connector at the PEM files on disk. The connector reads each file, base64-encodes the contents, URL-encodes the result, and appends the parameters to the sink URI in the form expected by the sink type (for Kafka: `ca_cert=...`, `client_cert=...`, `client_key=...`, `tls_enabled=true`). File-based values overwrite any same-named query parameter that was already present inline in `cockroachdb.changefeed.sink.uri`.

| Option                                            | Default | Description                                                                                                                                       |
|---------------------------------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `cockroachdb.changefeed.sink.tls.ca.cert.file`    | -       | Path to a PEM-encoded CA certificate file. Used to verify the sink broker's certificate.                                                          |
| `cockroachdb.changefeed.sink.tls.client.cert.file`| -       | Path to a PEM-encoded client certificate file. Required for mutual TLS.                                                                            |
| `cockroachdb.changefeed.sink.tls.client.key.file` | -       | Path to a PEM-encoded client private key file. Required for mutual TLS. Setting any of these three options implies `tls_enabled=true` on the URI. |

Example for an mTLS-secured Kafka cluster:

```json
{
  "cockroachdb.changefeed.sink.type": "kafka",
  "cockroachdb.changefeed.sink.uri": "kafka://kafka.example.com:9093",
  "cockroachdb.changefeed.sink.tls.ca.cert.file": "/etc/kafka/secrets/ca.pem",
  "cockroachdb.changefeed.sink.tls.client.cert.file": "/etc/kafka/secrets/client.pem",
  "cockroachdb.changefeed.sink.tls.client.key.file": "/etc/kafka/secrets/client-key.pem"
}
```

Currently only the Kafka sink consumes these TLS parameters; other sink types pass through unchanged.

#### Kafka Consumer Configuration (Advanced)

| Option                                               | Default     | Description                                                                                                                                                                    |
|------------------------------------------------------|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cockroachdb.changefeed.kafka.bootstrap.servers`     | -           | Consumer bootstrap servers. When not set, derived from `sink.uri`. Use when CockroachDB connects to Kafka via internal DNS but the connector JVM requires an external address. |
| `cockroachdb.changefeed.kafka.consumer.group.prefix` | cockroachdb-connector | Kafka consumer group ID                                                                                                                                               |
| `cockroachdb.changefeed.kafka.poll.timeout.ms`       | 100         | Kafka consumer poll timeout in milliseconds                                                                                                                                    |
| `cockroachdb.changefeed.kafka.auto.offset.reset`     | earliest    | Kafka consumer auto offset reset policy                                                                                                                                        |
| `cockroachdb.changefeed.kafka.consumer.override.*`   | -           | Passed verbatim to the changefeed consumer (Kafka client properties), e.g. `...override.security.protocol=SSL`. Use for SASL, custom trust/key stores, or to override auto-derived SSL. When `sink.tls.*` is set, the consumer's SSL is auto-derived from those PEM files, so this is only needed for extra or different settings. |

#### Connection Settings

| Option                                  | Default | Description                                 |
|-----------------------------------------|---------|---------------------------------------------|
| `connection.timeout.ms`                 | 30000   | Connection timeout in milliseconds          |
| `connection.retry.delay.ms`             | 1000    | Delay between connection retries in ms      |
| `connection.max.retries`                | 3       | Maximum number of connection retry attempts |
| `connection.validation.timeout.seconds` | 5       | Timeout for validating JDBC connections     |


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

## Heartbeat Support

CockroachDB changefeed [resolved timestamps](https://www.cockroachlabs.com/docs/stable/changefeed-messages#resolved-messages) serve as natural heartbeats. When the connector receives a resolved timestamp it updates the stored offset cursor and dispatches a Debezium heartbeat event, ensuring offsets advance even during idle periods with no data changes.

To emit heartbeat records to the `__debezium-heartbeat.<topic.prefix>` Kafka topic, set:

```json
{
  "heartbeat.interval.ms": "10000"
}
```

The `cockroachdb.changefeed.resolved.interval` property (default `10s`) controls how frequently CockroachDB emits resolved timestamps.

## Schema Evolution Detection

The connector automatically detects DDL changes (`ALTER TABLE ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`) without requiring a restart. When an incoming changefeed event contains fields that don't match the registered table schema, the connector:

1. Detects the mismatch by comparing event field names against registered column names
2. Re-queries `information_schema` to get the updated table definition
3. Refreshes the internal schema and continues processing with the new schema

CockroachDB changefeeds handle schema changes natively by performing a backfill (re-emitting all rows with the new schema), so no events are lost during the transition.

## Testing

Run all unit tests:

```bash
./mvnw clean test
```

Run integration tests (requires Docker for Testcontainers):

```bash
./mvnw clean test -Dtest="*IT"
```

Run against a specific CockroachDB version (default is v25.4.10):

```bash
./mvnw clean test -Dtest="*IT" -Dcockroachdb.version=v25.4.10
```

Run CockroachDB Cloud connectivity tests (requires a Cloud instance):

```bash
CRDB_CLOUD_URL="postgresql://user:pass@host:26257/defaultdb?sslmode=verify-full" \
  ./mvnw test -Dtest=CockroachDBCloudConnectionIT
```

The Cloud IT is guarded by `@EnabledIfEnvironmentVariable` and will be skipped in CI when the env var is absent.

For docker-compose based testing with a specific version:

```bash
COCKROACHDB_VERSION=v25.4.10 docker-compose -f src/test/scripts/docker-compose.yml up
```

## Known Limitations

- **Kafka-only sink**: Only Kafka sinks are supported. Webhook, Pub/Sub, and cloud storage sinks are planned.

## Troubleshooting

- **Permission Errors**: Ensure [CHANGEFEED and SELECT privileges](https://www.cockroachlabs.com/docs/stable/grant#supported-privileges) are granted on all monitored tables.
- **Rangefeed Disabled**: Enable with `SET CLUSTER SETTING kv.rangefeed.enabled = true;`
- **No Events**: Check connector logs and [changefeed job status](https://www.cockroachlabs.com/docs/stable/monitor-and-debug-changefeeds.html#monitor-a-changefeed).
- **Configuration Issues**: Verify all required [changefeed parameters](https://www.cockroachlabs.com/docs/stable/create-and-configure-changefeeds#parameters) are properly configured.
