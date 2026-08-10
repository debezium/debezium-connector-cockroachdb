[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/io.debezium/debezium-connector-cockroachdb.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:io.debezium%20AND%20a:debezium-connector-cockroachdb)
[![Build Status](https://github.com/debezium/debezium-connector-cockroachdb/workflows/CI/badge.svg)](https://github.com/debezium/debezium-connector-cockroachdb/actions)
[![Community](https://img.shields.io/badge/Community-Zulip-blue.svg)](https://debezium.zulipchat.com/#narrow/channel/510960-community-cockroachdb)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Connector for CockroachDB

A [Debezium](https://debezium.io/) connector for capturing changes from
[CockroachDB](https://www.cockroachlabs.com/) databases, built on CockroachDB's native
[changefeed](https://www.cockroachlabs.com/docs/stable/change-data-capture-overview)
infrastructure. Row-level changes are emitted as standard Debezium change events, so any
Debezium-compatible consumer or sink connector works unchanged.

This connector is currently in an **incubating** state; details are subject to change.

## Documentation

The connector documentation lives on the Debezium site and is the authoritative reference
for configuration properties, delivery modes, data type mappings, snapshots, monitoring,
and troubleshooting:

- [Debezium connector for CockroachDB](https://debezium.io/documentation/reference/stable/connectors/cockroachdb.html)
- [Debezium JDBC sink connector](https://debezium.io/documentation/reference/stable/connectors/jdbc.html) (for writing into CockroachDB)

Runnable end-to-end examples (CockroachDB to CockroachDB, Oracle, Iceberg, sinkless, mTLS,
embedded engine, and workload-based verification) live in the
[examples repository](https://github.com/viragtripathi/debezium-cockroachdb-examples).

## Table discovery

The connector resolves `table.include.list` and `table.exclude.list` patterns when the task starts
and freezes the discovered tables into the running CockroachDB changefeed. A table created later is
not captured automatically, even when its name matches an include-list regular expression. Restart
the connector to rediscover and add it. An `execute-snapshot` signal that requests a table outside
the running capture set logs a warning; restart the connector before snapshotting that table so its
subsequent changes are also streamed. This behavior is the same for `kafka` and `sinkless` delivery.

## Quick start

Install the plugin from Maven Central
([`io.debezium:debezium-connector-cockroachdb`](https://search.maven.org/search?q=g:io.debezium%20AND%20a:debezium-connector-cockroachdb))
into your Kafka Connect `plugin.path`, enable rangefeeds on the source cluster, and
register a connector:

```sql
SET CLUSTER SETTING kv.rangefeed.enabled = true;
```

```json
{
  "name": "cockroachdb-connector",
  "config": {
    "connector.class": "io.debezium.connector.cockroachdb.CockroachDBConnector",
    "database.hostname": "cockroachdb",
    "database.port": "26257",
    "database.user": "cdc_user",
    "database.password": "",
    "database.dbname": "mydb",
    "topic.prefix": "crdb",
    "table.include.list": "public.orders",
    "cockroachdb.changefeed.sink.type": "kafka",
    "cockroachdb.changefeed.sink.uri": "kafka://kafka:9092",
    "cockroachdb.changefeed.kafka.bootstrap.servers": "kafka:9092"
  }
}
```

The `kafka` delivery mode uses an intermediate Kafka cluster that CockroachDB pushes
changefeed events to; the `sinkless` mode streams directly over a SQL connection with no
intermediate Kafka. See the
[connector documentation](https://debezium.io/documentation/reference/stable/connectors/cockroachdb.html)
for the trade-offs.

## Building

Requirements: JDK 17+, Docker (for integration tests).

```bash
./mvnw clean install
```

Build the connector plugin archive:

```bash
./mvnw clean package -Passembly
```

## Testing

```bash
# Unit tests
./mvnw test

# Integration tests (Testcontainers; requires Docker)
./mvnw verify -DskipUTs -P-assembly

# Coverage report (informational): target/site/jacoco/index.html
./mvnw verify jacoco:report
```

The integration tests pin the CockroachDB version via the `cockroachdb.version` system
property, defaulting to the version set in `pom.xml`.

## Contributing

The Debezium community welcomes anyone who wants to help out in any way, whether that
includes reporting problems, helping with documentation, or contributing code changes to
fix bugs, add tests, or implement new features. See
[CONTRIBUTING.md](https://github.com/debezium/debezium/blob/main/CONTRIBUTING.md) for details.

Issues are tracked at [github.com/debezium/dbz](https://github.com/debezium/dbz/issues)
with the label `component/cockroachdb-connector`. Community chat happens in the
[CockroachDB channel on Debezium Zulip](https://debezium.zulipchat.com/#narrow/channel/510960-community-cockroachdb).
