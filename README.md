[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-connector-cockroachdb/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-connector-cockroachdb)
[![Build Status](https://github.com/debezium/debezium-connector-cockroachdb/workflows/CI/badge.svg)](https://github.com/debezium/debezium-connector-cockroachdb/actions)
[![Community](https://img.shields.io/badge/Community-Zulip-blue.svg)](https://debezium.zulipchat.com/#narrow/channel/510960-community-cockroachdb)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Connector for CockroachDB

A [Debezium](https://debezium.io/) connector for capturing changes from [CockroachDB](https://www.cockroachlabs.com/) databases.

## Overview

The Debezium CockroachDB connector captures row-level changes from CockroachDB databases and streams them to Apache Kafka topics using Debezium's event processing pipeline. The connector leverages CockroachDB's native [changefeed mechanism](https://www.cockroachlabs.com/docs/v25.2/create-and-configure-changefeeds) for reliable change capture.

**Status**: This connector is currently in incubation phase and is being developed and tested.

## Building CockroachDB Connector

### Prerequisites

* CockroachDB v25.2+ with [rangefeed enabled](https://www.cockroachlabs.com/docs/v25.2/create-and-configure-changefeeds.html#enable-rangefeeds) (enriched envelope support introduced in v25.2)
* Kafka Connect (tested with Debezium Connect 3.0.0.Final)
* JDK 21+
* Maven 3.9.8 or later 
* Docker or Podman Desktop (with podman-compose)

### 1. Build the Connector

```bash
./mvnw clean package -Passembly -DskipTests
# Extract the plugin zip for Docker Compose
unzip -o target/debezium-connector-cockroachdb-3.2.0-SNAPSHOT-plugin.zip -d target/plugin
```

### 2. Start Infrastructure

**Important:** Use the new `start-services.sh` script for reliable service startup:

```bash
# Start all services with correct plugin mounting
./src/test/scripts/start-services.sh
```

This script automatically:
- Calculates the correct absolute path for the plugin directory
- Starts all required services (Zookeeper, Kafka, CockroachDB, Kafka Connect)
- Ensures the connector plugin is properly mounted and discoverable

### 3. Setup CockroachDB

```bash
# Run the setup script to configure CockroachDB with test data
./src/test/scripts/setup-cockroachdb.sh
```

This script will:
- Enable rangefeed (required for changefeeds)
- Create test database and user with proper permissions
- Create a realistic test table with UUID primary key
- Insert sample data for testing

### 4. Deploy the Connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @src/test/scripts/configs/cockroachdb-source.json
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

## 🏗️ **Architecture**

### **Core Architecture Flow**

```mermaid
graph LR
    A[CockroachDB<br/>Database] --> B[Changefeed<br/>Stream]
    B --> C[Kafka Topic<br/>Storage]
    C --> D[Debezium<br/>Connector]
    D --> E[Final Debezium<br/>Topics]
    
    style A fill:#0F4C81,stroke:#0A3A6B,stroke-width:3px,color:#ffffff
    style B fill:#1565C0,stroke:#0D47A1,stroke-width:3px,color:#ffffff
    style C fill:#6A1B9A,stroke:#4A148C,stroke-width:3px,color:#ffffff
    style D fill:#D84315,stroke:#BF360C,stroke-width:3px,color:#ffffff
    style E fill:#2E7D32,stroke:#1B5E20,stroke-width:3px,color:#ffffff
```

### **Detailed Component Architecture**

```mermaid
graph TB
    subgraph "CockroachDB Cluster"
        A1[CockroachDB<br/>Node 1] 
        A2[CockroachDB<br/>Node 2]
        A3[CockroachDB<br/>Node 3]
    end
    
    subgraph "Changefeed Layer"
        B1[Changefeed<br/>Job]
        B2[Enriched<br/>Envelope]
        B3[Real-time<br/>Streaming]
    end
    
    subgraph "Kafka Infrastructure"
        C1[Kafka<br/>Broker]
        C2[Zookeeper<br/>Coordination]
        C3[Topic<br/>Storage]
    end
    
    subgraph "Debezium Processing"
        D1[Debezium<br/>Connector]
        D2[Schema<br/>Registry]
        D3[Event<br/>Transformation]
    end
    
    subgraph "Output Destinations"
        E1[Analytics<br/>Platforms]
        E2[Data<br/>Warehouses]
        E3[Event<br/>Streaming]
    end
    
    A1 --> B1
    A2 --> B1
    A3 --> B1
    B1 --> B2
    B2 --> B3
    B3 --> C1
    C2 --> C1
    C1 --> C3
    C3 --> D1
    D1 --> D2
    D2 --> D3
    D3 --> E1
    D3 --> E2
    D3 --> E3
    
    style A1 fill:#0F4C81,stroke:#0A3A6B,stroke-width:2px,color:#ffffff
    style A2 fill:#0F4C81,stroke:#0A3A6B,stroke-width:2px,color:#ffffff
    style A3 fill:#0F4C81,stroke:#0A3A6B,stroke-width:2px,color:#ffffff
    style B1 fill:#1565C0,stroke:#0D47A1,stroke-width:2px,color:#ffffff
    style B2 fill:#1565C0,stroke:#0D47A1,stroke-width:2px,color:#ffffff
    style B3 fill:#1565C0,stroke:#0D47A1,stroke-width:2px,color:#ffffff
    style C1 fill:#6A1B9A,stroke:#4A148C,stroke-width:2px,color:#ffffff
    style C2 fill:#6A1B9A,stroke:#4A148C,stroke-width:2px,color:#ffffff
    style C3 fill:#6A1B9A,stroke:#4A148C,stroke-width:2px,color:#ffffff
    style D1 fill:#D84315,stroke:#BF360C,stroke-width:2px,color:#ffffff
    style D2 fill:#D84315,stroke:#BF360C,stroke-width:2px,color:#ffffff
    style D3 fill:#D84315,stroke:#BF360C,stroke-width:2px,color:#ffffff
    style E1 fill:#2E7D32,stroke:#1B5E20,stroke-width:2px,color:#ffffff
    style E2 fill:#2E7D32,stroke:#1B5E20,stroke-width:2px,color:#ffffff
    style E3 fill:#2E7D32,stroke:#1B5E20,stroke-width:2px,color:#ffffff
```

### **Deployment Architecture**

```mermaid
graph LR
    subgraph "CockroachDB Environment"
        A[CockroachDB<br/>Database Cluster]
        B[Kafka<br/>Cluster]
        C[Debezium<br/>Connect Cluster]
        D[Monitoring<br/>& Alerting]
    end
    
    subgraph "Data Flow"
        E[Real-time<br/>Changes]
        F[Event<br/>Processing]
        G[Data<br/>Delivery]
    end
    
    subgraph "Consumer Applications"
        H[Analytics<br/>Platforms]
        I[Data<br/>Lakes]
        J[Event-Driven<br/>Services]
    end
    
    A --> E
    E --> B
    B --> F
    F --> C
    C --> G
    G --> H
    G --> I
    G --> J
    D -.-> A
    D -.-> B
    D -.-> C
    
    style A fill:#0F4C81,stroke:#0A3A6B,stroke-width:3px,color:#ffffff
    style B fill:#6A1B9A,stroke:#4A148C,stroke-width:3px,color:#ffffff
    style C fill:#D84315,stroke:#BF360C,stroke-width:3px,color:#ffffff
    style D fill:#FF6F00,stroke:#E65100,stroke-width:3px,color:#ffffff
    style E fill:#1565C0,stroke:#0D47A1,stroke-width:2px,color:#ffffff
    style F fill:#1565C0,stroke:#0D47A1,stroke-width:2px,color:#ffffff
    style G fill:#1565C0,stroke:#0D47A1,stroke-width:2px,color:#ffffff
    style H fill:#2E7D32,stroke:#1B5E20,stroke-width:2px,color:#ffffff
    style I fill:#2E7D32,stroke:#1B5E20,stroke-width:2px,color:#ffffff
    style J fill:#2E7D32,stroke:#1B5E20,stroke-width:2px,color:#ffffff
```

### **Multi-Tenant Architecture**

```mermaid
graph TB
    subgraph "Tenant A"
        A1[CockroachDB<br/>Tenant A]
        A2[Changefeed<br/>A]
        A3[Kafka Topic<br/>tenant-a.*]
    end
    
    subgraph "Tenant B"
        B1[CockroachDB<br/>Tenant B]
        B2[Changefeed<br/>B]
        B3[Kafka Topic<br/>tenant-b.*]
    end
    
    subgraph "Shared Infrastructure"
        C1[Debezium<br/>Connector Pool]
        C2[Schema<br/>Registry]
        C3[Monitoring<br/>Dashboard]
    end
    
    subgraph "Output Processing"
        D1[Tenant A<br/>Topics]
        D2[Tenant B<br/>Topics]
        D3[Cross-Tenant<br/>Analytics]
    end
    
    A1 --> A2
    A2 --> A3
    B1 --> B2
    B2 --> B3
    A3 --> C1
    B3 --> C1
    C1 --> C2
    C1 --> C3
    C1 --> D1
    C1 --> D2
    C1 --> D3
    
    style A1 fill:#0F4C81,stroke:#0A3A6B,stroke-width:2px,color:#ffffff
    style A2 fill:#1565C0,stroke:#0D47A1,stroke-width:2px,color:#ffffff
    style A3 fill:#6A1B9A,stroke:#4A148C,stroke-width:2px,color:#ffffff
    style B1 fill:#0F4C81,stroke:#0A3A6B,stroke-width:2px,color:#ffffff
    style B2 fill:#1565C0,stroke:#0D47A1,stroke-width:2px,color:#ffffff
    style B3 fill:#6A1B9A,stroke:#4A148C,stroke-width:2px,color:#ffffff
    style C1 fill:#D84315,stroke:#BF360C,stroke-width:2px,color:#ffffff
    style C2 fill:#D84315,stroke:#BF360C,stroke-width:2px,color:#ffffff
    style C3 fill:#FF6F00,stroke:#E65100,stroke-width:2px,color:#ffffff
    style D1 fill:#2E7D32,stroke:#1B5E20,stroke-width:2px,color:#ffffff
    style D2 fill:#2E7D32,stroke:#1B5E20,stroke-width:2px,color:#ffffff
    style D3 fill:#2E7D32,stroke:#1B5E20,stroke-width:2px,color:#ffffff
```

## **🔄 Architecture Evolution**

### **Current: Kafka-Centric Architecture**

Our current implementation is optimized for Kafka as the primary sink, providing excellent performance and reliability for Kafka-based data pipelines.

```mermaid
graph LR
    A[CockroachDB<br/>Database] --> B[Changefeed<br/>Stream]
    B --> C[Kafka Topic<br/>Storage]
    C --> D[Debezium<br/>Connector]
    D --> E[Final Debezium<br/>Topics]
    
    style A fill:#0F4C81,stroke:#0A3A6B,stroke-width:3px,color:#ffffff
    style B fill:#1565C0,stroke:#0D47A1,stroke-width:3px,color:#ffffff
    style C fill:#6A1B9A,stroke:#4A148C,stroke-width:3px,color:#ffffff
    style D fill:#D84315,stroke:#BF360C,stroke-width:3px,color:#ffffff
    style E fill:#2E7D32,stroke:#1B5E20,stroke-width:3px,color:#ffffff
```

### **Future: Sink-Agnostic Architecture**

We're evolving to a sink-agnostic architecture that supports multiple sink types including Kafka, webhooks, and other destinations.

```mermaid
graph LR
    subgraph "CockroachDB Layer"
        A[CockroachDB<br/>Database]
        B[Changefeed<br/>Jobs]
    end
    
    subgraph "Sink Infrastructure"
        C[Kafka<br/>Brokers]
        D[Webhook<br/>Endpoints]
        E[Pub/Sub<br/>Topics]
    end
    
    subgraph "Debezium Processing"
        F[Debezium<br/>Connect]
        G[Sink<br/>Router]
        H[Event<br/>Transformation]
    end
    
    subgraph "Consumer Applications"
        I[Analytics<br/>Platforms]
        J[Data<br/>Warehouses]
        K[Webhook<br/>Consumers]
    end
    
    A --> B
    B --> C
    B --> D
    B --> E
    C --> F
    D --> F
    E --> F
    F --> G
    G --> H
    H --> I
    H --> J
    H --> K
    
    style A fill:#0F4C81,stroke:#0A3A6B,stroke-width:3px,color:#ffffff
    style B fill:#1565C0,stroke:#0D47A1,stroke-width:3px,color:#ffffff
    style C fill:#6A1B9A,stroke:#4A148C,stroke-width:3px,color:#ffffff
    style D fill:#FF6F00,stroke:#E65100,stroke-width:3px,color:#ffffff
    style E fill:#6A1B9A,stroke:#4A148C,stroke-width:3px,color:#ffffff
    style F fill:#D84315,stroke:#BF360C,stroke-width:3px,color:#ffffff
    style G fill:#D84315,stroke:#BF360C,stroke-width:3px,color:#ffffff
    style H fill:#D84315,stroke:#BF360C,stroke-width:3px,color:#ffffff
    style I fill:#2E7D32,stroke:#1B5E20,stroke-width:3px,color:#ffffff
    style J fill:#2E7D32,stroke:#1B5E20,stroke-width:3px,color:#ffffff
    style K fill:#FF6F00,stroke:#E65100,stroke-width:3px,color:#ffffff
```

## Testing

### Automated Tests

Run all unit and integration tests (including Testcontainers-based integration tests):
```bash
./mvnw clean test
```

To run only integration tests:
```bash
./mvnw clean test -Dtest="*IT"
```

### Manual Integration Tests

For manual, end-to-end integration tests using Docker Compose and test scripts, see:
[src/test/scripts/README.md](src/test/scripts/README.md)

This includes:
- How to start the full test environment
- How to run end-to-end and scenario-based tests
- Troubleshooting and log analysis tips

---

For more details, advanced scenarios, and troubleshooting, please refer to the test README above.

## Troubleshooting

- **Permission Errors**: Ensure [CHANGEFEED and SELECT privileges](https://www.cockroachlabs.com/docs/v25.2/grant#supported-privileges) are granted on all monitored tables.
- **Rangefeed Disabled**: Enable with `SET CLUSTER SETTING kv.rangefeed.enabled = true;`
- **No Events**: Check connector logs and [changefeed job status](https://www.cockroachlabs.com/docs/v25.2/monitor-and-debug-changefeeds.html#monitor-a-changefeed).
- **Configuration Issues**: Verify all required [changefeed parameters](https://www.cockroachlabs.com/docs/v25.2/create-and-configure-changefeeds#parameters) are properly configured.
