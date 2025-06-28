# Debezium Connector for CockroachDB

A [Debezium](https://debezium.io/) connector for [CockroachDB](https://www.cockroachlabs.com/) that captures change events using CockroachDB's native changefeed feature with enriched envelope support.

## Overview

This connector leverages CockroachDB's changefeed functionality to capture real-time changes from your database and stream them to Kafka in Debezium's standard format. It uses the enriched envelope feature introduced in CockroachDB v25.2 to provide comprehensive metadata about change events.

## Features

- **Real-time Change Capture**: Uses CockroachDB's native changefeed feature for efficient change streaming
- **Enriched Envelope Support**: Leverages CockroachDB v25.2+ enriched envelope for comprehensive metadata
- **Snapshot Support**: Initial snapshots using CockroachDB's AS OF SYSTEM TIME feature
- **Retry Logic**: Built-in retry mechanism for transient errors (serialization failures, connection issues)
- **SSL Support**: Full SSL/TLS configuration support
- **Schema Evolution**: Handles schema changes gracefully
- **Multiple Sink Support**: Compatible with all CockroachDB changefeed sinks (Kafka, cloud storage, etc.)

## Prerequisites

- CockroachDB v25.2 or later (for enriched envelope support)
- Java 17 or later
- Apache Kafka (for sink configuration)
- Debezium 3.2.0 or later

## Configuration

### Basic Configuration

```properties
# Connector configuration
connector.class=io.debezium.connector.cockroachdb.CockroachDBConnector

# Database connection
database.hostname=localhost
database.port=26257
database.user=root
database.password=
database.dbname=defaultdb

# Logical name for the connector
database.server.name=cockroachdb-server

# Topic naming
topic.prefix=cockroachdb

# Snapshot configuration
snapshot.mode=initial
snapshot.isolation.mode=serializable
snapshot.locking.mode=none

# Changefeed configuration
database.history.kafka.bootstrap.servers=localhost:9092
database.history.kafka.topic=dbhistory.cockroachdb
```

### Advanced Configuration

```properties
# SSL Configuration
database.sslmode=prefer
database.sslrootcert=/path/to/ca.crt
database.sslcert=/path/to/client.crt
database.sslkey=/path/to/client.key
database.sslpassword=password

# Connection settings
database.tcpKeepAlive=true
database.on.connect.statements=SET timezone='UTC'

# Performance tuning
poll.interval.ms=1000
max.queue.size=8192
max.batch.size=2048
max.queue.size.in.bytes=1073741824

# Error handling
errors.max.retries=3
errors.retry.delay.ms=1000
```

## Enriched Envelope Features

The connector uses CockroachDB's enriched envelope feature to provide comprehensive metadata:

### Source Information
- Database and schema names
- Table information
- Cluster metadata
- Timestamp information (HLC, nanosecond precision)
- Node information

### Change Event Metadata
- Operation type (INSERT, UPDATE, DELETE)
- Primary key information
- Before/after values (with diff option)
- Transaction boundaries
- MVCC timestamps

### Example Enriched Envelope Output

```json
{
  "after": {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com"
  },
  "before": {
    "id": 1,
    "name": "John Smith",
    "email": "john@example.com"
  },
  "source": {
    "version": "3.2.0-SNAPSHOT",
    "connector": "cockroachdb",
    "name": "cockroachdb-server",
    "ts_ms": 1640995200000,
    "snapshot": false,
    "db": "defaultdb",
    "schema": "public",
    "table": "users",
    "cluster_id": "cockroachdb-cluster",
    "node_id": 1,
    "mvcc_timestamp": "1640995200.0000000000",
    "ts_hlc": "1640995200.0000000000",
    "ts_ns": 1640995200000000000
  },
  "op": "u",
  "ts_ms": 1640995200000
}
```

## Supported Sinks

The connector supports all CockroachDB changefeed sinks:

- **Kafka**: Direct streaming to Kafka topics
- **Cloud Storage**: AWS S3, Google Cloud Storage, Azure Blob Storage
- **Webhook**: HTTP endpoints
- **Pub/Sub**: Google Cloud Pub/Sub, AWS SNS/SQS

## Snapshot Modes

- `initial`: Perform snapshot on first start, then stream changes
- `always`: Perform snapshot on every start
- `initial_only`: Perform snapshot only, then stop
- `never`: Skip snapshot, stream changes only
- `when_needed`: Perform snapshot when needed based on configuration

## Error Handling

The connector includes robust error handling for CockroachDB-specific scenarios:

### Transient Errors (Auto-retry)
- Serialization failures (SQL state 40001)
- Deadlock detection (SQL state 40P01)
- Connection failures (SQL state 08000)
- Network timeouts

### Permanent Errors (Fail-fast)
- Authentication failures
- Permission denied
- Invalid configuration

## Development

### Building

```bash
mvn clean package
```

### Testing

```bash
# Run unit tests
mvn test

# Run integration tests (requires Docker)
mvn verify

# Skip integration tests
mvn verify -DskipITs=true
```

### Docker Support

The project includes Docker configuration for testing with CockroachDB:

```bash
# Start CockroachDB container
mvn docker:start

# Run tests
mvn verify

# Stop CockroachDB container
mvn docker:stop
```

## Architecture

### Core Components

1. **CockroachDBConnector**: Main connector class
2. **CockroachDBConnectorTask**: Task implementation for change streaming
3. **CockroachDBStreamingChangeEventSource**: Streaming implementation using changefeeds
4. **CockroachDBConnection**: JDBC connection management with retry logic
5. **CockroachDBErrorHandler**: Error handling and retry logic
6. **ChangefeedSchemaParser**: Parsing enriched envelope messages

### Data Flow

1. Connector establishes JDBC connection to CockroachDB
2. Creates changefeed with enriched envelope configuration
3. Streams change events from changefeed
4. Parses enriched envelope messages
5. Converts to Debezium format
6. Dispatches to Kafka topics

## Limitations

- Requires CockroachDB v25.2+ for enriched envelope support
- Single-node changefeed support (no multi-node coordination yet)
- Limited schema evolution support
- No incremental snapshot support yet

## Roadmap

- [ ] Multi-node changefeed coordination
- [ ] Incremental snapshot support
- [ ] Enhanced schema evolution handling
- [ ] Performance optimizations
- [ ] Additional sink support
- [ ] Metrics and monitoring

## Contributing

Contributions are welcome! Please see the [contributing guidelines](CONTRIBUTING.md) for details.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- [Debezium Documentation](https://debezium.io/documentation/)
- [CockroachDB Documentation](https://www.cockroachlabs.com/docs/)
- [GitHub Issues](https://github.com/debezium/debezium-connector-cockroachdb/issues)
