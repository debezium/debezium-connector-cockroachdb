[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-connector-cockroachdb/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-connector-cockroachdb)
[![Build Status](https://github.com/debezium/debezium-connector-cockroachdb/workflows/CI/badge.svg)](https://github.com/debezium/debezium-connector-cockroachdb/actions)
[![Community](https://img.shields.io/badge/Community-Zulip-blue.svg)](https://debezium.zulipchat.com/#narrow/channel/510960-community-cockroachdb)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Connector for CockroachDB

A [Debezium](https://debezium.io/) connector for capturing changes from [CockroachDB](https://www.cockroachlabs.com/) databases.

## Overview

The Debezium CockroachDB connector captures row-level changes from CockroachDB databases and streams them to Apache Kafka topics using Debezium's event processing pipeline. The connector leverages CockroachDB's native [changefeed mechanism](https://www.cockroachlabs.com/docs/v25.2/change-data-capture-overview) for reliable change capture.

**Status**: This connector is currently in incubation phase and is being developed and tested.

## Prerequisites

* CockroachDB v25.2+ with [rangefeed enabled](https://www.cockroachlabs.com/docs/v25.2/create-and-configure-changefeeds.html#enable-rangefeeds) (enriched envelope support introduced in v25.2)
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
    "connection.retry.delay.ms": 100,
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

| Option                                       | Default                | Description                                 |
|----------------------------------------------|------------------------|---------------------------------------------|
| `cockroachdb.changefeed.envelope`            | enriched               | Envelope type: enriched, wrapped, bare      |
| `cockroachdb.changefeed.enriched.properties` | source                 | Comma-separated enriched properties         |
| `cockroachdb.changefeed.sink.type`           | kafka                  | Sink type (kafka, webhook, pubsub, etc.)    |
| `cockroachdb.changefeed.sink.uri`            | kafka://localhost:9092 | Sink URI (format depends on sink type)      |
| `cockroachdb.changefeed.sink.topic.prefix`   | ""                     | Optional prefix for sink topic names        |
| `cockroachdb.changefeed.sink.options`        | ""                     | Additional sink options in key=value format |
| `cockroachdb.changefeed.resolved.interval`   | 10s                    | Resolved timestamp interval                 |
| `cockroachdb.changefeed.include.updated`     | false                  | Include updated column information          |
| `cockroachdb.changefeed.include.diff`        | false                  | Include before/after diff information       |
| `cockroachdb.changefeed.cursor`              | now                    | Start cursor position                       |
| `cockroachdb.changefeed.batch.size`          | 1000                   | Batch size for changefeed processing        |
| `cockroachdb.changefeed.poll.interval.ms`    | 100                    | Poll interval in milliseconds               |

#### Connection Settings

| Option                      | Default | Description                                 |
|-----------------------------|---------|---------------------------------------------|
| `connection.timeout.ms`     | 30000   | Connection timeout in milliseconds          |
| `connection.retry.delay.ms` | 100     | Delay between connection retries in ms      |
| `connection.max.retries`    | 3       | Maximum number of connection retry attempts |

## Event Format

Events are produced in Debezium's enriched envelope format. For details on the changefeed message format, see the [CockroachDB changefeed messages documentation](https://www.cockroachlabs.com/docs/v25.2/changefeed-messages).

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

## Testing

Run all unit and integration tests:

```bash
./mvnw clean test
```

To run only integration tests:

```bash
./mvnw clean test -Dtest="*IT"
```

## Troubleshooting

- **Permission Errors**: Ensure [CHANGEFEED and SELECT privileges](https://www.cockroachlabs.com/docs/v25.2/grant#supported-privileges) are granted on all monitored tables.
- **Rangefeed Disabled**: Enable with `SET CLUSTER SETTING kv.rangefeed.enabled = true;`
- **No Events**: Check connector logs and [changefeed job status](https://www.cockroachlabs.com/docs/v25.2/monitor-and-debug-changefeeds.html#monitor-a-changefeed).
- **Configuration Issues**: Verify all required [changefeed parameters](https://www.cockroachlabs.com/docs/v25.2/create-and-configure-changefeeds#parameters) are properly configured.
