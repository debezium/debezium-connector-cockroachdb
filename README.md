# Debezium Connector for CockroachDB

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-connector-cockroachdb/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-connector-cockroachdb)
[![Build Status](https://github.com/debezium/debezium-connector-cockroachdb/workflows/CI/badge.svg)](https://github.com/debezium/debezium-connector-cockroachdb/actions)
[![Community](https://img.shields.io/badge/Community-Zulip-blue.svg)](https://debezium.zulipchat.com/#narrow/channel/510960-community-cockroachdb)

An incubating Debezium CDC connector for CockroachDB database; Please log issues in our tracker at https://issues.redhat.com/projects/DBZ/

A [Debezium](https://debezium.io/) connector for capturing changes from [CockroachDB](https://www.cockroachlabs.com/) databases.

## Overview

The Debezium CockroachDB connector captures row-level changes from CockroachDB databases and streams them to Apache Kafka topics using Debezium's event processing pipeline. The connector leverages CockroachDB's native [changefeed mechanism](https://www.cockroachlabs.com/docs/stable/create-changefeed) for reliable change capture.

## Prerequisites

- CockroachDB v25.2+ with [rangefeed enabled](https://www.cockroachlabs.com/docs/stable/architecture/overview#rangefeeds) (enriched envelope support introduced in v25.2)
- Kafka Connect (tested with Debezium Connect 3.0.0.Final)
- Java 21+
- Maven 3.8+

## Quickstart

### 1. Build the Connector

```bash
./mvnw clean package -Ptest-uber-jar -DskipTests
mkdir -p target/plugin/debezium-connector-cockroachdb
cp target/debezium-connector-cockroachdb-3.2.0-SNAPSHOT-test-uber-jar-with-dependencies.jar target/plugin/debezium-connector-cockroachdb/
```

### 2. Start Infrastructure

```bash
docker compose up -d
```

### 3. Create Test Table and Grant Privileges

```sql
CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name STRING NOT NULL,
    description STRING,
    price DECIMAL(10,2),
    category STRING,
    in_stock BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW()
);
GRANT CHANGEFEED, SELECT ON TABLE products TO <user>;
```

### 4. Deploy the Connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @scripts/configs/cockroachdb-source.json
```

### 5. Consume Change Events

```bash
docker exec -it kafka-test kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cockroachdb.public.products \
  --from-beginning
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
| `cockroachdb.changefeed.sink.uri`            | kafka://localhost:9092 | Sink URI (format depends on sink type)     |
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

Events are produced in Debezium's enriched envelope format. For details on the changefeed message format, see the [CockroachDB changefeed messages documentation](https://www.cockroachlabs.com/docs/stable/changefeed-messages).

```json
{
  "before": null,
  "after": {
    "id": "...",
    "name": "...",
    ...
  },
  "source": {
    "changefeed_sink": "kafka",
    "cluster_id": "...",
    "database_name": "testdb",
    "table_name": "products",
    ...
  },
  "op": "c",
  "ts_ns": 1751407136710963868
}
```

## Architecture

The connector creates [sink changefeeds](https://www.cockroachlabs.com/docs/stable/changefeed-sinks) in CockroachDB that write to configured sinks (Kafka, webhook, pubsub, cloud storage, etc.). The connector then consumes events from these sinks and processes them through Debezium's event pipeline.

## Testing

Run all tests:
```bash
./mvnw clean test
```

Run end-to-end test:
```bash
./scripts/run-tests.sh --cleanup-start --cleanup-exit test-end-to-end-sink.sh
```

## Troubleshooting

- **Permission Errors**: Ensure [CHANGEFEED and SELECT privileges](https://www.cockroachlabs.com/docs/stable/grant#supported-privileges) are granted on all monitored tables.
- **Rangefeed Disabled**: Enable with `SET CLUSTER SETTING kv.rangefeed.enabled = true;`
- **No Events**: Check connector logs and [changefeed job status](https://www.cockroachlabs.com/docs/stable/show-changefeed-jobs).
- **Configuration Issues**: Verify all required [changefeed parameters](https://www.cockroachlabs.com/docs/stable/create-changefeed#parameters) are properly configured.

## Community

- [Debezium Community](https://debezium.io/community/)
- [Zulip Chat](https://debezium.zulipchat.com/)
- [Mailing List](https://groups.google.com/forum/#!forum/debezium)

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
