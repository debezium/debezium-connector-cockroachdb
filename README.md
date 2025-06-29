# debezium-connector-cockroachdb

An incubating Debezium CDC connector for CockroachDB database; Please log issues in our tracker at https://issues.redhat.com/projects/DBZ/

[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-connector-cockroachdb/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.debezium%22%20a%3A%22debezium-connector-cockroachdb%22)
[![User chat](https://img.shields.io/badge/chat-users-brightgreen.svg)](https://debezium.zulipchat.com/#narrow/stream/302529-users)
[![Developer chat](https://img.shields.io/badge/chat-devs-brightgreen.svg)](https://debezium.zulipchat.com/#narrow/stream/302533-dev)
[![CockroachDB Community](https://img.shields.io/badge/chat-cockroachdb%20community-brightgreen.svg)](https://debezium.zulipchat.com/#narrow/channel/510960-community-cockroachdb)
[![Google Group](https://img.shields.io/:mailing%20list-debezium-brightgreen.svg)](https://groups.google.com/forum/#!forum/debezium)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-debezium-brightgreen.svg)](http://stackoverflow.com/questions/tagged/debezium)

---

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
- **Docker** (for testing)

**Important:** The enriched envelope feature, which provides full Debezium compatibility including operation types (`op` field), is only available in CockroachDB v25.2 and later. Earlier versions will not support the recommended changefeed configuration.

## Quick Start

### 1. Build the Connector

```bash
git clone <repository-url>
cd debezium-connector-cockroachdb
./mvnw clean package
```

**Expected Output:**
```
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  45.123 s
[INFO] Finished at: 2025-06-29T...
[INFO] ------------------------------------------------------------------------
```

### 2. Manual Testing Setup (Recommended for Development)

This section provides a complete manual testing setup that demonstrates the full workflow and helps with debugging.

#### 2.1 Create Docker Network

```bash
# Create a dedicated network for the containers
docker network create debezium-test
```

**Expected Output:**
```
debezium-test
```

#### 2.2 Start CockroachDB

```bash
# Start CockroachDB with proper networking
docker run -d --name cockroachdb \
  --network debezium-test \
  -p 26257:26257 -p 8080:8080 \
  cockroachdb/cockroach:v25.2.1 \
  start-single-node --insecure
```

**Expected Output:**
```
2981b6bb975c
```

**Verify it's running:**
```bash
docker ps | grep cockroach
```

**Expected Output:**
```
2981b6bb975c  cockroachdb/cockroach:v25.2.1              start-single-node...  2 minutes ago  Up 2 minutes  0.0.0.0:26257->26257/tcp, 0.0.0.0:8080->8080/tcp  cockroachdb
```

#### 2.3 Start Zookeeper

```bash
# Start Zookeeper
docker run -d --name zookeeper-test \
  --network debezium-test \
  -p 2181:2181 \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  -e ZOOKEEPER_TICK_TIME=2000 \
  confluentinc/cp-zookeeper:7.4.0

# Wait for Zookeeper to start (check logs if needed)
docker logs zookeeper-test
```

**Expected Output:**
```
35dc0b4f6a09
```

**Check Zookeeper logs:**
```bash
docker logs zookeeper-test | tail -5
```

**Expected Output:**
```
[2025-06-29 01:40:33,440] INFO zookeeper.snapshotSizeFactor = 0.33 (org.apache.zookeeper.server.ZKDatabase)
[2025-06-29 01:40:33,440] INFO zookeeper.commitLogCount=500 (org.apache.zookeeper.server.ZKDatabase)
[2025-06-29 01:40:33,444] INFO zookeeper.snapshot.compression.method = CHECKED (org.apache.zookeeper.server.persistence.SnapStream)
[2025-06-29 01:40:33,444] INFO Snapshotting: 0x0 to /var/lib/zookeeper/data/version-2/snapshot.0 (org.apache.zookeeper.server.persistence.FileTxnSnapLog)
[2025-06-29 01:40:33,445] INFO Snapshot loaded in 5 ms, highest zxid is 0x0, digest is 1371985504 (org.apache.zookeeper.server.ZooKeeperServer)
```

#### 2.4 Start Kafka

```bash
# Start Kafka with proper listener configuration
docker run -d --name kafka-test \
  --network debezium-test \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper-test:2181 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka-test:9092,PLAINTEXT_HOST://localhost:9092 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:7.4.0

# Wait for Kafka to start (check logs if needed)
docker logs kafka-test
```

**Expected Output:**
```
4a132dd933c1
```

**Check Kafka logs:**
```bash
docker logs kafka-test | tail -10
```

**Expected Output:**
```
[2025-06-29 01:56:58,057] TRACE [Controller id=1] Checking need to trigger auto leader balancing (kafka.controller.KafkaController)
[2025-06-29 01:56:58,060] DEBUG [Controller id=1] Topics not in preferred replica for broker 1 HashMap() (kafka.controller.KafkaController)
[2025-06-29 01:56:58,060] TRACE [Controller id=1] Leader imbalance ratio for broker 1 is 0.0 (kafka.controller.KafkaController)
```

#### 2.5 Verify Container Connectivity

```bash
# Check if all containers are running
docker ps
```

**Expected Output:**
```
CONTAINER ID  IMAGE                                      COMMAND               CREATED         STATUS         PORTS                                             NAMES
2981b6bb975c  cockroachdb/cockroach:v25.2.1              start-single-node...  5 minutes ago   Up 5 minutes   0.0.0.0:26257->26257/tcp, 0.0.0.0:8080->8080/tcp  cockroachdb
35dc0b4f6a09  confluentinc/cp-zookeeper:7.4.0            /etc/confluent/do...  3 minutes ago   Up 3 minutes   0.0.0.0:2181->2181/tcp                            zookeeper-test
4a132dd933c1  confluentinc/cp-kafka:7.4.0                /etc/confluent/do...  2 minutes ago   Up 2 minutes   0.0.0.0:9092->9092/tcp                            kafka-test
```

```bash
# Test Kafka connectivity from host
nc -zv localhost 9092
```

**Expected Output:**
```
Connection to localhost port 9092 [tcp/XmlIpcRegSvc] succeeded!
```

```bash
# Test Zookeeper connectivity from host
nc -zv localhost 2181
```

**Expected Output:**
```
Connection to localhost port 2181 [tcp/eforward] succeeded!
```

```bash
# Test CockroachDB connectivity from host
nc -zv localhost 26257
```

**Expected Output:**
```
Connection to localhost port 26257 [tcp/*] succeeded!
```

#### 2.6 Create Test Database and Table

```bash
# Connect to CockroachDB and create test data
docker exec -it cockroachdb cockroach sql --insecure -e "
CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

CREATE TABLE users (
    id INT PRIMARY KEY,
    name STRING,
    email STRING,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO users (id, name, email) VALUES 
    (1, 'John Doe', 'john@example.com'),
    (2, 'Jane Smith', 'jane@example.com');
"
```

**Expected Output:**
```
SET
CREATE DATABASE
SET
CREATE TABLE
INSERT 0 2
```

#### 2.7 Create Changefeed

```bash
# Create changefeed with enriched envelope
docker exec -it cockroachdb cockroach sql --insecure -e "
USE testdb;
CREATE CHANGEFEED FOR TABLE users 
INTO 'kafka://kafka-test:9092' 
WITH envelope = 'enriched',
     enriched_properties = 'source,schema',
     diff,
     updated,
     resolved = '10s';
"
```

**Expected Output:**
```
SET
NOTICE: auto-committing transaction before processing DDL due to autocommit_before_ddl setting
        job_id
-----------------------
  1084922957081444353
(1 row)

NOTICE: changefeed will emit to topic users
NOTICE: resolved (10s) messages will not be emitted more frequently than the default min_checkpoint_frequency (30s), but may be emitted less frequently
```

**Important:** Note the use of `kafka-test:9092` instead of `localhost:9092`. This is crucial for Docker networking - containers must use container names, not localhost.

#### 2.8 Verify Changefeed Status

```bash
# Check changefeed job status
docker exec -it cockroachdb cockroach sql --insecure -e "SHOW CHANGEFEED JOBS;"
```

**Expected Output:**
```
        job_id        |                                                                                  description                                                                                  | user_name | status  |              running_status              |            created            |            started            | finished |           modified            |      high_water_timestamp      | readable_high_water_timestamptz | error |        sink_uri         |   full_table_names    | topics | format
----------------------+------------------------------------------------------------------------------------+-----------+---------+------------------------------------------+-------------------------------+-------------------------------+----------+-------------------------------+--------------------------------+--------------------------------+-------+-------------------------+-----------------------+--------+---------
  1084922957081444353 | CREATE CHANGEFEED FOR TABLE users INTO 'kafka://kafka-test:9092' WITH OPTIONS (diff, enriched_properties = 'source,schema', envelope = 'enriched', resolved = '10s', updated) | root      | running | running: resolved=1751162611.014522846,0 | 2025-06-29 02:03:31.023411+00 | 2025-06-29 02:03:31.023411+00 | NULL     | 2025-06-29 02:03:31.07006+00  | 1751162611014522846.0000000000 | 2025-06-29 02:03:31.014523+00   |       | kafka://kafka-test:9092 | {testdb.public.users} | users  | json
(1 row)
```

Look for:
- Status: `running`
- Running status: `running: resolved=...`
- No error messages

#### 2.9 Test Change Capture

```bash
# Insert a new row to test real-time capture
docker exec -it cockroachdb cockroach sql --insecure -e "
USE testdb;
INSERT INTO users (id, name, email) VALUES (3, 'Test User', 'test@example.com');
"
```

**Expected Output:**
```
SET
INSERT 0 1
```

#### 2.10 Consume Change Events

```bash
# Consume messages from Kafka topic
docker exec -it kafka-test kafka-console-consumer \
  --bootstrap-server kafka-test:9092 \
  --topic users \
  --from-beginning \
  --max-messages 5
```

**Expected Output:**
```
{"payload": {"after": {"created_at": "2025-06-29T01:35:09.8297", "email": "jane@example.com", "id": 2, "name": "Jane Smith"}, "before": null, "op": "c", "source": {"changefeed_sink": "kafka", "cluster_id": "c5b33262-b2b6-4d59-b644-4d40c8ca53d0", "cluster_name": "", "database_name": "testdb", "db_version": "v25.2.1", "job_id": "1084920637155278849", "node_id": "1", "node_name": "127.0.0.1", "origin": "cockroachdb", "primary_keys": ["id"], "schema_name": "public", "source_node_locality": "", "table_name": "users", "ts_hlc": "1751161903005030052.0000000000", "ts_ns": 1751162467008308178}, "ts_ns": 1751162467008308178}, "schema": {"fields": [{"field": "before", "fields": [{"field": "id", "optional": false, "type": "int64"}, {"field": "name", "optional": true, "type": "string"}, {"field": "email", "optional": true, "type": "string"}, {"field": "created_at", "name": "timestamp", "optional": true, "type": "string"}], "name": "users.before.value", "optional": true, "type": "struct"}, {"field": "after", "fields": [{"field": "id", "optional": false, "type": "int64"}, {"field": "name", "optional": true, "type": "string"}, {"field": "email", "optional": true, "type": "string"}, {"field": "created_at", "name": "timestamp", "optional": true, "type": "string"}], "name": "users.after.value", "optional": false, "type": "struct"}, {"field": "source", "fields": [{"field": "node_name", "optional": false, "type": "string"}, {"field": "ts_hlc", "optional": true, "type": "string"}, {"field": "table_name", "optional": false, "type": "string"}, {"field": "primary_keys", "items": {"optional": false, "type": "string"}, "optional": false, "type": "array"}, {"field": "changefeed_sink", "optional": false, "type": "string"}, {"field": "source_node_locality", "optional": false, "type": "string"}, {"field": "mvcc_timestamp", "optional": true, "type": "string"}, {"field": "ts_ns", "optional": true, "type": "int64"}, {"field": "schema_name", "optional": false, "type": "string"}, {"field": "origin", "optional": false, "type": "string"}, {"field": "job_id", "optional": false, "type": "string"}, {"field": "cluster_id", "optional": false, "type": "string"}, {"field": "database_name", "optional": false, "type": "string"}, {"field": "db_version", "optional": false, "type": "string"}, {"field": "cluster_name", "optional": false, "type": "string"}, {"field": "node_id", "optional": false, "type": "string"}], "name": "cockroachdb.source", "optional": true, "type": "struct"}, {"field": "ts_ns", "optional": false, "type": "int64"}, {"field": "op", "optional": false, "type": "string"}], "name": "cockroachdb.envelope", "optional": false, "type": "struct"}}
{"payload": {"after": {"created_at": "2025-06-29T01:35:09.8297", "email": "john@example.com", "id": 1, "name": "John Doe"}, "before": null, "op": "c", "source": {"changefeed_sink": "kafka", "cluster_id": "c5b33262-b2b6-4d59-b644-4d40c8ca53d0", "cluster_name": "", "database_name": "testdb", "db_version": "v25.2.1", "job_id": "1084920637155278849", "node_id": "1", "node_name": "127.0.0.1", "origin": "cockroachdb", "primary_keys": ["id"], "schema_name": "public", "source_node_locality": "", "table_name": "users", "ts_hlc": "1751161903005030052.0000000000", "ts_ns": 1751161903005030052}, "ts_ns": 1751162467008309470}, "schema": {"fields": [{"field": "before", "fields": [{"field": "id", "optional": false, "type": "int64"}, {"field": "name", "optional": true, "type": "string"}, {"field": "email", "optional": true, "type": "string"}, {"field": "created_at", "name": "timestamp", "optional": true, "type": "string"}], "name": "users.before.value", "optional": true, "type": "struct"}, {"field": "after", "fields": [{"field": "id", "optional": false, "type": "int64"}, {"field": "name", "optional": true, "type": "string"}, {"field": "email", "optional": true, "type": "string"}, {"field": "created_at", "name": "timestamp", "optional": true, "type": "string"}], "name": "users.after.value", "optional": false, "type": "struct"}, {"field": "source", "fields": [{"field": "node_name", "optional": false, "type": "string"}, {"field": "ts_hlc", "optional": true, "type": "string"}, {"field": "table_name", "optional": false, "type": "string"}, {"field": "primary_keys", "items": {"optional": false, "type": "string"}, "optional": false, "type": "array"}, {"field": "changefeed_sink", "optional": false, "type": "string"}, {"field": "source_node_locality", "optional": false, "type": "string"}, {"field": "mvcc_timestamp", "optional": true, "type": "string"}, {"field": "ts_ns", "optional": true, "type": "int64"}, {"field": "schema_name", "optional": false, "type": "string"}, {"field": "origin", "optional": false, "type": "string"}, {"field": "job_id", "optional": false, "type": "string"}, {"field": "cluster_id", "optional": false, "type": "string"}, {"field": "database_name", "optional": false, "type": "string"}, {"field": "db_version", "optional": false, "type": "string"}, {"field": "cluster_name", "optional": false, "type": "string"}, {"field": "node_id", "optional": false, "type": "string"}], "name": "cockroachdb.source", "optional": true, "type": "struct"}, {"field": "ts_ns", "optional": false, "type": "int64"}, {"field": "op", "optional": false, "type": "string"}], "name": "cockroachdb.envelope", "optional": false, "type": "struct"}}
{"resolved":"1751161903005030052.0000000000"}
Processed a total of 3 messages
```

You should see change events in this format:
```json
{
  "payload": {
    "after": {
      "id": 3,
      "name": "Test User",
      "email": "test@example.com",
      "created_at": "2025-06-29T..."
    },
    "before": null,
    "op": "c",
    "source": {
      "changefeed_sink": "kafka",
      "cluster_id": "...",
      "database_name": "testdb",
      "table_name": "users",
      "ts_hlc": "...",
      "ts_ns": ...
    },
    "ts_ns": ...
  },
  "schema": {...}
}
```

### 3. Troubleshooting

#### 3.1 Changefeed Connection Issues

**Problem:** `ERROR: unable to dial: dial tcp [::1]:9092: connect: connection refused`

**Solution:** Use container name instead of localhost:
```sql
-- Wrong
CREATE CHANGEFEED FOR TABLE users INTO 'kafka://localhost:9092' ...

-- Correct
CREATE CHANGEFEED FOR TABLE users INTO 'kafka://kafka-test:9092' ...
```

#### 3.2 Kafka Consumer Connection Issues

**Problem:** Consumer can't connect to Kafka

**Solution:** Use internal container address:
```bash
# Wrong
docker exec -it kafka-test kafka-console-consumer --bootstrap-server localhost:9092 ...

# Correct
docker exec -it kafka-test kafka-console-consumer --bootstrap-server kafka-test:9092 ...
```

#### 3.3 Changefeed in Error State

**Problem:** Changefeed shows "transient error" status

**Solution:** Check connectivity and restart:
```bash
# Cancel the job
docker exec -it cockroachdb cockroach sql --insecure -e "CANCEL JOB <job_id>;"

# Recreate with proper configuration
docker exec -it cockroachdb cockroach sql --insecure -e "
USE testdb;
CREATE CHANGEFEED FOR TABLE users 
INTO 'kafka://kafka-test:9092' 
WITH envelope = 'enriched',
     enriched_properties = 'source,schema',
     diff,
     updated,
     resolved = '10s';
"
```

#### 3.4 Container Networking Issues

**Problem:** Containers can't communicate

**Solution:** Ensure all containers are on the same network:
```bash
# Check network
docker network ls

# Check container network
docker inspect cockroachdb | grep -A 10 "NetworkMode"

# Add container to network if needed
docker network connect debezium-test <container_name>
```

### 4. Cleanup

```bash
# Stop and remove containers
docker stop cockroachdb zookeeper-test kafka-test
docker rm cockroachdb zookeeper-test kafka-test

# Remove network
docker network rm debezium-test
```

**Expected Output:**
```
cockroachdb
zookeeper-test
kafka-test
cockroachdb
zookeeper-test
kafka-test
debezium-test
```

## Integration Testing

The project includes comprehensive integration tests that use the same Docker setup:

```bash
# Run integration tests
./mvnw verify

# Run only unit tests (no Docker required)
./mvnw test

# Run only integration tests
./mvnw verify -DskipTests -DskipUnitTests
```

**Note:** Integration tests require Docker and may take several minutes to complete as they start the full infrastructure.

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

#### Changefeed Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `changefeed.envelope` | Changefeed envelope type (`wrapped`, `enriched`, `bare`) | `enriched` |
| `changefeed.resolved.interval` | How often to emit resolved timestamps (e.g., "10s", "1m") | `10s` |
| `changefeed.include.updated` | Include updated column values (enriched envelope only) | `true` |
| `changefeed.include.diff` | Include diff information showing what changed (enriched envelope only) | `true` |
| `changefeed.enriched.properties` | Comma-separated enriched properties (`source,schema`) | `source,schema` |
| `changefeed.topic.prefix` | Optional prefix for changefeed topic names | - |
| `changefeed.cursor` | Starting cursor position (`now` or timestamp) | `now` |
| `changefeed.batch.size` | Number of rows to fetch per batch | `1000` |
| `changefeed.poll.interval.ms` | Polling interval for changefeed events in milliseconds | `100` |

#### Connection Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `connection.timeout.ms` | Connection timeout in milliseconds | `30000` |
| `connection.retry.delay.ms` | Delay between connection retry attempts in milliseconds | `100` |
| `connection.max.retries` | Maximum number of connection retry attempts | `3` |

**Note:** CockroachDB only supports two isolation levels: SERIALIZABLE (default) and READ COMMITTED. SERIALIZABLE is the strongest ANSI transaction isolation level and is recommended for most use cases as it provides the strongest consistency guarantees. READ COMMITTED is available for applications that need higher concurrency with minimal transaction retries.

### Configuration Examples

#### Basic Configuration
```json
{
  "connector.class": "io.debezium.connector.cockroachdb.CockroachDBConnector",
  "database.hostname": "localhost",
  "database.port": "26257",
  "database.user": "root",
  "database.password": "",
  "database.dbname": "testdb",
  "database.server.name": "cockroachdb-server",
  "topic.prefix": "cockroachdb"
}
```

#### Advanced Changefeed Configuration
```json
{
  "connector.class": "io.debezium.connector.cockroachdb.CockroachDBConnector",
  "database.hostname": "localhost",
  "database.port": "26257",
  "database.user": "root",
  "database.password": "",
  "database.dbname": "testdb",
  "database.server.name": "cockroachdb-server",
  
  // Changefeed configuration
  "changefeed.envelope": "enriched",
  "changefeed.resolved.interval": "5s",
  "changefeed.include.updated": true,
  "changefeed.include.diff": true,
  "changefeed.enriched.properties": "source,schema",
  "changefeed.batch.size": 500,
  "changefeed.poll.interval.ms": 50,
  
  // Connection configuration
  "connection.timeout.ms": 60000,
  "connection.retry.delay.ms": 200,
  "connection.max.retries": 5
}
```

#### High-Performance Configuration
```json
{
  "connector.class": "io.debezium.connector.cockroachdb.CockroachDBConnector",
  "database.hostname": "localhost",
  "database.port": "26257",
  "database.user": "root",
  "database.password": "",
  "database.dbname": "testdb",
  "database.server.name": "cockroachdb-server",
  
  // Optimized for high throughput
  "changefeed.batch.size": 2000,
  "changefeed.poll.interval.ms": 25,
  "changefeed.resolved.interval": "30s",
  
  // Aggressive connection retry for unstable networks
  "connection.timeout.ms": 30000,
  "connection.retry.delay.ms": 50,
  "connection.max.retries": 10
}
```

#### Development Configuration
```json
{
  "connector.class": "io.debezium.connector.cockroachdb.CockroachDBConnector",
  "database.hostname": "localhost",
  "database.port": "26257",
  "database.user": "root",
  "database.password": "",
  "database.dbname": "testdb",
  "database.server.name": "cockroachdb-server",
  
  // Faster feedback for development
  "changefeed.resolved.interval": "1s",
  "changefeed.poll.interval.ms": 100,
  "changefeed.batch.size": 100,
  
  // Quick failure for development
  "connection.timeout.ms": 5000,
  "connection.retry.delay.ms": 100,
  "connection.max.retries": 2
}
```

### Configuration Best Practices

#### Changefeed Configuration
- **Envelope Type**: Use `enriched` for full Debezium compatibility with operation types
- **Resolved Interval**: Use shorter intervals (5-10s) for real-time applications, longer intervals (30s+) for batch processing
- **Batch Size**: Increase for high-throughput scenarios, decrease for low-latency requirements
- **Poll Interval**: Lower values provide faster event processing but higher CPU usage

#### Connection Configuration
- **Timeout**: Increase for high-latency networks or large databases
- **Retry Delay**: Use exponential backoff (current implementation multiplies by attempt number)
- **Max Retries**: Higher values for unstable networks, lower values for quick failure detection

#### Performance Tuning
- **High Throughput**: Increase batch size, decrease poll interval, use longer resolved intervals
- **Low Latency**: Decrease batch size, decrease poll interval, use shorter resolved intervals
- **Network Issues**: Increase connection timeout, retry delay, and max retries
- **Resource Constraints**: Decrease batch size and poll frequency

### Changefeed Configuration

The connector requires CockroachDB changefeeds to be configured with the enriched envelope for full Debezium compatibility:

```sql
CREATE CHANGEFEED FOR TABLE <table_name> 
INTO 'kafka://<kafka-broker>:9092' 
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

### SSL Configuration Examples

#### Basic SSL (require)
```json
{
  "database.sslmode": "require"
}
```

#### SSL with Certificate Verification (verify-ca)
```json
{
  "database.sslmode": "verify-ca",
  "database.sslrootcert": "/path/to/ca.crt"
}
```

#### SSL with Client Authentication (verify-full)
```json
{
  "database.sslmode": "verify-full",
  "database.sslrootcert": "/path/to/ca.crt",
  "database.sslcert": "/path/to/client.crt",
  "database.sslkey": "/path/to/client.key"
}
```

## Advanced Troubleshooting

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
# Clone the repository
git clone <repository-url>
cd debezium-connector-cockroachdb

# Build the project
./mvnw clean package

# Run tests
./mvnw test

# Run integration tests
./mvnw verify
```

### Project Structure

```
src/
├── main/java/io/debezium/connector/cockroachdb/
│   ├── CockroachDBConnector.java          # Main connector class
│   ├── CockroachDBConnectorConfig.java    # Configuration handling
│   ├── CockroachDBConnectorTask.java      # Task implementation
│   ├── CockroachDBStreamingChangeEventSource.java  # Streaming logic
│   ├── CockroachDBConnection.java         # Database connection
│   ├── CockroachDBErrorHandler.java       # Error handling
│   ├── CockroachDBSchema.java             # Schema management
│   ├── CockroachDBOffsetContext.java      # Offset management
│   ├── CockroachDBPartition.java          # Partition handling
│   ├── CockroachDBTaskContext.java        # Task context
│   ├── CockroachDBValueConverterProvider.java  # Value conversion
│   ├── CockroachDBEventMetadataProvider.java   # Event metadata
│   ├── CockroachDBChangeEventCreator.java      # Event creation
│   ├── CockroachDBSourceInfoStructMaker.java   # Source info
│   ├── Module.java                         # Module information
│   ├── SourceInfo.java                     # Source information
│   └── serialization/
│       └── ChangefeedSchemaParser.java     # Schema parsing
└── test/java/io/debezium/connector/cockroachdb/
    ├── CockroachDBConnectorTest.java       # Unit tests
    ├── CockroachDBConnectorConfigTest.java # Config tests
    ├── CockroachDBErrorHandlerTest.java    # Error handler tests
    ├── CockroachDBConnectionTest.java      # Connection tests
    ├── ChangefeedSchemaParserTest.java     # Parser tests
    └── integration/
        └── CockroachDBConnectorIT.java     # Integration tests
```

### Running Tests

```bash
# Unit tests only (fast)
./mvnw test

# Integration tests (requires Docker)
./mvnw verify

# Skip tests
./mvnw package -DskipTests

# Run specific test
./mvnw test -Dtest=CockroachDBConnectorTest

# Run with debug output
./mvnw test -Dorg.slf4j.simpleLogger.defaultLogLevel=debug
```

### Debugging

#### Enable Debug Logging

Add to `src/test/resources/logback-test.xml`:
```xml
<logger name="io.debezium.connector.cockroachdb" level="DEBUG"/>
```

#### Check Changefeed Status

```sql
-- Check all changefeed jobs
SHOW CHANGEFEED JOBS;

-- Check specific job details
SHOW CHANGEFEED JOB <job_id>;

-- Check changefeed logs
SELECT * FROM [SHOW CHANGEFEED JOB <job_id>] WHERE event_type = 'error';
```

#### Check Kafka Topics

```bash
# List topics
docker exec -it kafka-test kafka-topics --bootstrap-server kafka-test:9092 --list

# Describe topic
docker exec -it kafka-test kafka-topics --bootstrap-server kafka-test:9092 --describe --topic users

# Check consumer groups
docker exec -it kafka-test kafka-consumer-groups --bootstrap-server kafka-test:9092 --list
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [Debezium Documentation](https://debezium.io/documentation/)
- **Issues**: [GitHub Issues](https://github.com/debezium/debezium-connector-cockroachdb/issues)
- **Discussions**: [GitHub Discussions](https://github.com/debezium/debezium-connector-cockroachdb/discussions)
- **Community**: [Debezium Community](https://debezium.io/community/)

## Version History

- **3.3.0** - Initial MVP release with enriched envelope support
- **3.2.0** - Development version