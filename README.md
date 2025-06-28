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

- **CockroachDB v25.2+** with changefeed feature enabled (enriched envelope support requires v25.2+)
- **Kafka Connect** (or compatible sink)
- **Java 17+**
- **Maven 3.9.8+**

**Important:** The enriched envelope feature, which provides full Debezium compatibility including operation types (`op` field), is only available in CockroachDB v25.2 and later. Earlier versions will not support the recommended changefeed configuration.

## Quick Start

### 1. Build the Connector

```bash
git clone <repository-url>
cd debezium-connector-cockroachdb
./mvnw clean package
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
-- Create a changefeed with enriched envelope for Debezium compatibility
CREATE CHANGEFEED FOR TABLE users 
INTO 'kafka://localhost:9092' 
WITH envelope = 'enriched',
     enriched_properties = 'source,schema',
     diff,
     updated,
     resolved = '10s';
```

**Changefeed Options Explained:**
- `envelope = 'enriched'`: Provides the richest metadata including operation types (`op` field)
- `enriched_properties = 'source,schema'`: Includes source metadata and schema information
- `diff`: Includes the previous state of the row in the `before` field
- `updated`: Includes the commit timestamp in the `updated` field
- `resolved = '10s'`: Emits resolved timestamps every 10 seconds for consistency

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
    "snapshot.isolation.mode": "serializable",
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
| `snapshot.isolation.mode` | Snapshot isolation level (`serializable`, `read_committed`) | `serializable` |
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

**Note:** CockroachDB only supports two isolation levels: SERIALIZABLE (default) and READ COMMITTED. SERIALIZABLE is the strongest ANSI transaction isolation level and is recommended for most use cases as it provides the strongest consistency guarantees. READ COMMITTED is available for applications that need higher concurrency with minimal transaction retries.

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

The connector produces change events in CockroachDB's enriched envelope format:

```json
{
  "after": {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com",
    "created_at": "2025-01-15T10:30:00Z"
  },
  "before": null,
  "key": {
    "id": 1
  },
  "updated": "2025-01-15T10:30:00.123456Z",
  "op": "c",
  "ts_ns": 1642234567890123456,
  "source": {
    "changefeed_sink": "kafka",
    "cluster_id": "12345678-1234-1234-1234-123456789abc",
    "cluster_name": "my-cluster",
    "database_name": "testdb",
    "db_version": "v25.2.1",
    "job_id": 123456,
    "mvcc_timestamp": "1642234567.890123456",
    "node_id": 1,
    "node_name": "cockroach-0",
    "origin": "cockroachdb",
    "primary_keys": ["id"],
    "schema_name": "public",
    "source_node_locality": "cloud=gce,region=us-east1,zone=us-east1-b",
    "table_name": "users",
    "ts_hlc": "1642234567.890123456",
    "ts_ns": "1642234567890123456"
  },
  "schema": {
    "after": {
      "fields": [
        {"field": "id", "type": "int", "optional": false},
        {"field": "name", "type": "string", "optional": false},
        {"field": "email", "type": "string", "optional": true},
        {"field": "created_at", "type": "timestamp", "optional": true}
      ],
      "name": "after_schema"
    },
    "before": {
      "fields": [
        {"field": "id", "type": "int", "optional": false},
        {"field": "name", "type": "string", "optional": false},
        {"field": "email", "type": "string", "optional": true},
        {"field": "created_at", "type": "timestamp", "optional": true}
      ],
      "name": "before_schema"
    }
  }
}
```

**Envelope Fields Explained:**

- `after`: The new state of the row after the change
- `before`: The previous state of the row (null for inserts, populated for updates/deletes when `diff` is enabled)
- `key`: The primary key of the changed row
- `updated`: Timestamp when the change was committed
- `op`: Operation type (`c` for INSERT, `u` for UPDATE, `d` for DELETE)
- `ts_ns`: Processing timestamp in nanoseconds
- `source`: Metadata about the source of the change event
- `schema`: Schema information for the `after` and `before` fields (when `enriched_properties = 'schema'` is enabled)

## Operation Types

The `op` field indicates the type of database operation that triggered the change event. This field is only available when using `envelope = 'enriched'`:

- `c` - Create (INSERT): A new row was inserted
- `u` - Update (UPDATE): An existing row was modified
- `d` - Delete (DELETE): A row was removed

**Note:** The `op` field is not available in other envelope types like `wrapped` or `bare`. For full Debezium compatibility, always use `envelope = 'enriched'`.

## Envelope Types

CockroachDB supports different envelope types for changefeed messages. The connector is designed to work with the `enriched` envelope for maximum compatibility with Debezium:

### Enriched Envelope (Recommended)
```sql
CREATE CHANGEFEED FOR TABLE users 
INTO 'kafka://localhost:9092' 
WITH envelope = 'enriched',
     enriched_properties = 'source,schema',
     diff,
     updated;
```

**Features:**
- Full metadata including operation types (`op` field)
- Source information (cluster, database, table details)
- Schema information for data interpretation
- Before/after data for change tracking
- Timestamps for ordering and consistency

### Wrapped Envelope
```sql
CREATE CHANGEFEED FOR TABLE users 
INTO 'kafka://localhost:9092' 
WITH envelope = 'wrapped';
```

**Features:**
- Basic change data with primary key
- Limited metadata
- No operation type information
- Not recommended for Debezium integration

### Bare Envelope
```sql
CREATE CHANGEFEED FOR TABLE users 
INTO 'kafka://localhost:9092' 
WITH envelope = 'bare';
```

**Features:**
- Raw row data at the top level
- Metadata nested under `__crdb__` field
- Minimal overhead
- Not suitable for Debezium integration

## Troubleshooting

### Common Issues

1. **Connection Failures**
   - Verify CockroachDB is running and accessible
   - Check network connectivity and firewall settings
   - Validate SSL configuration if using secure connections

2. **Changefeed Not Working**
   - Ensure changefeeds are enabled: `SET CLUSTER SETTING kv.rangefeed.enabled = true;`
   - Verify CockroachDB version is 25.2+ (required for enriched envelope)
   - Check that the changefeed is using `envelope = 'enriched'` for full Debezium compatibility
   - Verify the changefeed job is running: `SHOW CHANGEFEED JOBS;`

3. **Missing Operation Types**
   - Ensure `envelope = 'enriched'` is set in the changefeed configuration
   - The `op` field is only available with enriched envelopes

4. **Missing Before/After Data**
   - Add `diff` option to the changefeed to include the `before` field
   - The `before` field will be null for inserts and populated for updates/deletes

5. **Missing Source Metadata**
   - Add `enriched_properties = 'source,schema'` to include source information
   - This provides cluster, database, and table metadata

6. **Timestamp Issues**
   - Use `updated` option to include commit timestamps
   - For ordering, use `ts_hlc` from the source field rather than top-level `ts_ns`

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
./mvnw clean package
```

### Running Tests

```bash
# Unit tests (no Docker required)
./mvnw test

# Integration tests (requires Docker for CockroachDB, Kafka, and Zookeeper)
./mvnw verify

# Skip integration tests (useful for CI or when Docker is not available)
./mvnw verify -DskipITs=true
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