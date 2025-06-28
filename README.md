# Debezium Connector for CockroachDB

A [Debezium](https://debezium.io/) connector for capturing change events from [CockroachDB](https://www.cockroachlabs.com/) databases using CockroachDB's enriched envelope changefeed feature (v25.2+).

## Overview

This connector leverages CockroachDB's native changefeed functionality to capture database changes in real-time and stream them to Kafka topics. It supports CockroachDB's enriched envelope format, providing comprehensive change event information including before/after states, operation types, and metadata.

## Features

- **Real-time Change Capture**: Uses CockroachDB's changefeed feature for efficient change detection
- **Enriched Envelope Support**: Leverages CockroachDB v25.2+ enriched envelope for comprehensive change data
- **Multiple Sink Support**: Compatible with all CockroachDB changefeed-compatible sinks (Kafka, Pub/Sub, etc.)
- **Snapshot Support**: Configurable snapshot modes for initial data loading
- **SSL/TLS Support**: Secure connections with comprehensive SSL configuration options
- **Error Handling**: Robust error handling with transient error detection and retry logic
- **Schema Evolution**: Automatic schema detection and evolution support

## Prerequisites

- **CockroachDB v25.2+** with changefeed feature enabled
- **Kafka Connect** (or compatible sink)
- **Java 17+**
- **Maven 3.9.8+**

## Quick Start

### 1. Build the Connector

```bash
git clone <repository-url>
cd debezium-connector-cockroachdb
mvn clean package
```

### 2. Start CockroachDB

```bash
# Using Docker
docker run -d --name cockroachdb \
  -p 26257:26257 -p 8080:8080 \
  cockroachdb/cockroach:latest \
  start-single-node --insecure

# Or using CockroachDB Cloud/local installation
```

### 3. Enable Changefeeds

```sql
-- Connect to CockroachDB and enable changefeeds
SET CLUSTER SETTING kv.rangefeed.enabled = true;
```

### 4. Create Sample Data

```sql
-- Create a database and table
CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

CREATE TABLE users (
    id INT PRIMARY KEY,
    name STRING,
    email STRING,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert some sample data
INSERT INTO users (id, name, email) VALUES 
    (1, 'John Doe', 'john@example.com'),
    (2, 'Jane Smith', 'jane@example.com');
```

### 5. Create a Changefeed

```sql
-- Create a changefeed with enriched envelope
CREATE CHANGEFEED FOR TABLE users 
INTO 'kafka://localhost:9092' 
WITH updated, resolved = '10s', envelope = 'wrapped';
```

### 6. Configure the Connector

Create a connector configuration file `cockroachdb-connector.json`:

```json
{
  "name": "cockroachdb-connector",
  "config": {
    "connector.class": "io.debezium.connector.cockroachdb.CockroachDBConnector",
    "database.hostname": "localhost",
    "database.port": "26257",
    "database.user": "root",
    "database.password": "",
    "database.dbname": "testdb",
    "database.server.name": "cockroachdb-server",
    "topic.prefix": "cockroachdb",
    "snapshot.mode": "initial",
    "snapshot.isolation.mode": "read_committed",
    "snapshot.locking.mode": "shared",
    "database.sslmode": "disable",
    "status.update.interval.ms": "10000",
    "unavailable.value.placeholder": "hex:FF"
  }
}
```

### 7. Deploy the Connector

```bash
# Using Kafka Connect REST API
curl -X POST -H "Content-Type: application/json" \
  --data @cockroachdb-connector.json \
  http://localhost:8083/connectors

# Or using Confluent Control Center
```

### 8. Monitor the Connector

```bash
# Check connector status
curl -X GET http://localhost:8083/connectors/cockroachdb-connector/status

# View connector configuration
curl -X GET http://localhost:8083/connectors/cockroachdb-connector/config
```

### 9. Test the Connector

```sql
-- Make some changes to test the connector
INSERT INTO users (id, name, email) VALUES (3, 'Bob Wilson', 'bob@example.com');
UPDATE users SET email = 'john.doe@example.com' WHERE id = 1;
DELETE FROM users WHERE id = 2;
```

### 10. View Change Events

```bash
# Consume messages from the Kafka topic
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic cockroachdb.testdb.users \
  --from-beginning
```

## Configuration

### Required Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `connector.class` | The connector class | `io.debezium.connector.cockroachdb.CockroachDBConnector` |
| `database.hostname` | CockroachDB server hostname | - |
| `database.port` | CockroachDB server port | `26257` |
| `database.user` | Database user | - |
| `database.password` | Database password | - |
| `database.dbname` | Database name | - |
| `database.server.name` | Logical name for the database server | - |
| `topic.prefix` | Prefix for Kafka topics | - |

### Optional Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `snapshot.mode` | Snapshot mode (`initial`, `never`, `when_needed`) | `initial` |
| `snapshot.isolation.mode` | Snapshot isolation level | `read_committed` |
| `snapshot.locking.mode` | Snapshot locking mode | `shared` |
| `database.sslmode` | SSL mode (`disable`, `require`, `verify-ca`, `verify-full`) | `disable` |
| `database.sslrootcert` | SSL root certificate path | - |
| `database.sslcert` | SSL client certificate path | - |
| `database.sslkey` | SSL client key path | - |
| `database.sslpassword` | SSL client key password | - |
| `database.tcpKeepAlive` | Enable TCP keep-alive | `false` |
| `database.on.connect.statements` | SQL statements to execute on connection | - |
| `read.only` | Use read-only connection | `false` |
| `status.update.interval.ms` | Status update interval | `10000` |
| `unavailable.value.placeholder` | Placeholder for unavailable values | `hex:FF` |

### SSL Configuration Examples

#### Basic SSL (require)
```json
{
  "database.sslmode": "require"
}
```

#### SSL with Certificate Verification
```json
{
  "database.sslmode": "verify-ca",
  "database.sslrootcert": "/path/to/ca.crt",
  "database.sslcert": "/path/to/client.crt",
  "database.sslkey": "/path/to/client.key",
  "database.sslpassword": "keypassword"
}
```

## Change Event Format

The connector produces change events in the following format:

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "field": "before",
        "type": {
          "type": "struct",
          "fields": [
            {"field": "id", "type": "int32"},
            {"field": "name", "type": "string"},
            {"field": "email", "type": "string"}
          ]
        }
      },
      {
        "field": "after",
        "type": {
          "type": "struct",
          "fields": [
            {"field": "id", "type": "int32"},
            {"field": "name", "type": "string"},
            {"field": "email", "type": "string"}
          ]
        }
      },
      {"field": "source", "type": "struct"},
      {"field": "op", "type": "string"},
      {"field": "ts_ms", "type": "int64"}
    ]
  },
  "payload": {
    "before": null,
    "after": {
      "id": 1,
      "name": "John Doe",
      "email": "john@example.com"
    },
    "source": {
      "version": "3.2.0-SNAPSHOT",
      "connector": "cockroachdb",
      "name": "cockroachdb-server",
      "ts_ms": 1640995200000,
      "snapshot": "false",
      "db": "testdb",
      "schema": "public",
      "table": "users"
    },
    "op": "c",
    "ts_ms": 1640995200000
  }
}
```

## Operation Types

- `c` - Create (INSERT)
- `u` - Update (UPDATE)
- `d` - Delete (DELETE)
- `r` - Read (SNAPSHOT)

## Troubleshooting

### Common Issues

1. **Connection Failures**
   - Verify CockroachDB is running and accessible
   - Check network connectivity and firewall settings
   - Validate SSL configuration if using secure connections

2. **Changefeed Not Working**
   - Ensure changefeeds are enabled: `SET CLUSTER SETTING kv.rangefeed.enabled = true;`
   - Verify CockroachDB version is 25.2+
   - Check changefeed permissions

3. **Schema Evolution Issues**
   - Monitor connector logs for schema change events
   - Verify downstream systems can handle schema changes

### Logging

Enable debug logging for troubleshooting:

```properties
# In logback.xml or application.properties
loggers=io.debezium.connector.cockroachdb=DEBUG
```

### Health Checks

```bash
# Check connector health
curl -X GET http://localhost:8083/connectors/cockroachdb-connector/status

# View connector metrics
curl -X GET http://localhost:8083/connectors/cockroachdb-connector/metrics
```

## Development

### Building from Source

```bash
git clone <repository-url>
cd debezium-connector-cockroachdb
mvn clean package
```

### Running Tests

```bash
# Unit tests
mvn test

# Integration tests
mvn verify
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/debezium/debezium-connector-cockroachdb/issues)
- **Documentation**: [Debezium Documentation](https://debezium.io/documentation/)
- **Community**: [Debezium Community](https://debezium.io/community/)

## Version History

- **3.3.0** - Initial MVP release with enriched envelope support
- **3.2.0** - Development version
