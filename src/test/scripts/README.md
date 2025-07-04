# Scripts Directory

This directory contains essential scripts and configuration files for testing and deploying the CockroachDB connector.

## 🚀 **Quick Start**

### **1. Start the Environment**
```bash
# Use the reliable start-services.sh script (recommended)
./src/test/scripts/start-services.sh

# Setup CockroachDB
./src/test/scripts/setup-cockroachdb.sh
```

**Alternative (manual approach):**
```bash
# Navigate to the scripts directory
cd src/test/scripts

# Start all services (run from this directory for correct path resolution)
docker-compose up -d

# Return to project root
cd ../../

# Setup CockroachDB
./src/test/scripts/setup-cockroachdb.sh
```

### **2. Run End-to-End Test**
```bash
# Run the complete test suite
./src/test/scripts/test-simple.sh
```

### **3. Clean Up**
```bash
# Clean up all containers and volumes
./src/test/scripts/cleanup.sh
```

## 📋 **Available Scripts**

### **Core Scripts**

#### **`start-services.sh`** ⭐ **NEW - RECOMMENDED**
**Reliable service startup script that automatically handles plugin path resolution:**
- Calculates the correct absolute path for the plugin directory
- Starts all required services (Zookeeper, Kafka, CockroachDB, Kafka Connect)
- Ensures the connector plugin is properly mounted and discoverable
- Works from any directory (project root or scripts directory)

**Usage:**
```bash
# From project root (recommended)
./src/test/scripts/start-services.sh

# From scripts directory
cd src/test/scripts
./start-services.sh
```

#### **`setup-cockroachdb.sh`**
Sets up CockroachDB with required configuration:
- Enables rangefeed feature
- Creates test database and user
- Creates test table with sample data
- Grants necessary permissions

**Usage:**
```bash
# From project root
./src/test/scripts/setup-cockroachdb.sh
```

#### **`test-simple.sh`**
Comprehensive end-to-end test that:
- Builds the connector
- Starts all services
- Sets up CockroachDB
- Creates and tests the connector
- Generates test data
- Verifies data flow through Kafka
- Cleans up resources

**Usage:**
```bash
# From project root
./src/test/scripts/test-simple.sh
```

#### **`cleanup.sh`**
Cleans up the test environment:
- Stops and removes Docker containers
- Removes networks and volumes
- Cleans up temporary files

**Usage:**
```bash
# From project root
./src/test/scripts/cleanup.sh
./src/test/scripts/cleanup.sh --force  # Skip confirmation
./src/test/scripts/cleanup.sh --preview # Show what will be cleaned
```

### **Utility Scripts**

#### **`process-config-template.sh`**
Processes configuration templates with environment variables.

**Usage:**
```bash
# Process template with default values
./scripts/process-config-template.sh \
  scripts/configs/cockroachdb-source-template.json \
  scripts/configs/cockroachdb-production.json

# Process with custom environment variables
SCHEMA_NAME=my_schema TABLE_NAME=orders \
  ./scripts/process-config-template.sh \
  scripts/configs/cockroachdb-source-template.json \
  scripts/configs/cockroachdb-custom.json
```

#### **`kafka_consumer.py`**
Python script to consume and display Kafka messages.

**Usage:**
```bash
python3 scripts/kafka_consumer.py \
  --bootstrap-servers localhost:9092 \
  --topic cockroachdb.public.products
```

## ⚙️ **Configuration Files**

### **`configs/cockroachdb-source.json`**
- **Purpose:** Production-ready connector configuration
- **Features:** Complete changefeed setup with enriched envelope
- **Usage:** Direct deployment to Kafka Connect

### **`configs/cockroachdb-source-template.json`**
- **Purpose:** Template configuration with environment variable support
- **Features:** Parameterized settings for flexible deployment
- **Usage:** Process with `process-config-template.sh` for custom environments

## 🔧 **Environment Variables**

| Variable | Default | Description |
|----------|---------|-------------|
| `CONNECTOR_VERSION` | `3.2.0-SNAPSHOT` | Connector JAR version |
| `DB_HOST` | `localhost` | CockroachDB hostname |
| `DB_PORT` | `26257` | CockroachDB port |
| `DB_USER` | `debezium` | Database username |
| `DB_PASSWORD` | `dbz` | Database password |
| `DB_NAME` | `testdb` | Database name |
| `KAFKA_HOST` | `localhost` | Kafka hostname |
| `KAFKA_PORT` | `9092` | Kafka port |
| `CONNECTOR_NAME` | `cockroachdb-connector` | Connector name in Kafka Connect |
| `TOPIC_NAME` | `cockroachdb.public.products` | Kafka topic for changefeed events |

## 🧪 **Testing Workflows**

### **End-to-End Testing (`test-simple.sh`)**
- **Purpose:** Complete integration test from CockroachDB to Kafka
- **Scope:** Full CRUD operations (INSERT, UPDATE, DELETE)
- **Output:** Validates data flow and event processing

### **Connector Discovery (`test-connect-discovery.sh`)**
- **Purpose:** Verify connector availability in Kafka Connect
- **Scope:** Plugin discovery and basic connectivity
- **Output:** Confirms connector is ready for deployment

## 🚀 **Quick Start**

### **1. Build and Test**
```bash
# Build the connector
./mvnw clean package

# Run end-to-end test
./src/test/scripts/test-simple.sh --force
```

### **2. Custom Configuration**
```bash
# Use custom database settings
DB_HOST=my-cockroachdb DB_USER=myuser ./src/test/scripts/test-simple.sh

# Use custom topic name
TOPIC_NAME=my-custom-topic ./src/test/scripts/test-simple.sh
```

### **3. Cleanup**
```bash
# Clean up test environment
./src/test/scripts/cleanup.sh --force
```

## 🔍 **Troubleshooting**

### **Common Issues**

**Connector Not Found:**
```bash
# Check if connector JAR exists
ls -la target/debezium-connector-cockroachdb-*.jar

# Verify plugin path in Kafka Connect
curl -s http://localhost:8083/connector-plugins | jq '.[] | select(.class | contains("cockroachdb"))'
```

**Database Connection Issues:**
```bash
# Test CockroachDB connectivity
docker exec cockroachdb cockroach sql --insecure --host=localhost:26257 --execute="SELECT 1;"

# Check user permissions
docker exec cockroachdb cockroach sql --insecure --host=localhost:26257 --execute="SHOW GRANTS FOR debezium;"
```

**Kafka Topic Issues:**
```bash
# List Kafka topics
docker exec kafka-test kafka-topics --bootstrap-server localhost:9092 --list

# Check topic messages
docker exec kafka-test kafka-console-consumer --bootstrap-server localhost:9092 --topic cockroachdb.public.products --from-beginning --max-messages 5
```

### **Log Analysis**

**Connector Logs:**
```bash
# View connector logs
docker logs connect | grep -i cockroachdb

# Check for errors
docker logs connect | grep -i error
```

**Changefeed Status:**
```bash
# Check changefeed jobs
docker exec cockroachdb cockroach sql --insecure --host=localhost:26257 --execute="SHOW CHANGEFEED JOBS;"
```

## 📚 **Architecture Overview**

The connector implements a **direct changefeed approach**:

```
CockroachDB → Changefeed → Kafka Topic → Debezium Connector → Final Topics
```

**Key Components:**
1. **CockroachDB Changefeed:** Direct streaming to Kafka topic
2. **Kafka Topic:** Standard Debezium topic format (`cockroachdb.{schema}.{table}`)
3. **Debezium Connector:** Processes changefeed events into Debezium format
4. **Final Topics:** Standard Debezium change event topics

**Benefits:**
- **Simplified Architecture:** Single topic per table
- **Standard Debezium Pattern:** Compatible with existing Debezium ecosystem
- **Operational Clarity:** Clear data flow and monitoring
- **Production Ready:** Proven pattern used by other Debezium connectors

## 🔄 **Data Flow**

### **1. Changefeed Creation**
```sql
CREATE CHANGEFEED FOR TABLE public.products 
INTO 'kafka://localhost:9092?topic_name=cockroachdb.public.products'
WITH envelope = 'enriched', updated, diff, resolved = '10s';
```

### **2. Event Processing**
- **Raw Events:** CockroachDB changefeed events in enriched envelope format
- **Transformation:** Connector converts to Debezium change event format
- **Output:** Standard Debezium topics with schema evolution support

### **3. Event Types**
- **INSERT:** `op: "c"` with full row data
- **UPDATE:** `op: "u"` with before/after data and diff
- **DELETE:** `op: "d"` with deleted row data
- **Resolved:** Timestamp resolution for consistency

## 🛠 **Development**

### **Building**
```bash
# Clean build
./mvnw clean package

# Run tests
./mvnw test

# Build with assembly
./mvnw clean package -Passembly
```

### **Testing**
```bash
# Unit tests
./mvnw test

# Integration tests
./src/test/scripts/test-simple.sh --force

# Custom test environment
DB_HOST=localhost DB_USER=testuser ./src/test/scripts/test-simple.sh
```

### **Debugging**
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG

# Run with verbose output
./src/test/scripts/test-simple.sh --force 2>&1 | tee test.log
```

## 🤝 **Contributing**

1. **Fork** the repository
2. **Create** a feature branch
3. **Test** your changes with the provided scripts
4. **Submit** a pull request
