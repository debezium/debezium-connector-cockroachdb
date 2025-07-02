# Scripts Directory

This directory contains essential scripts and configuration files for testing and deploying the CockroachDB connector.

## 📁 **Directory Structure**

```
scripts/
├── docker-compose.yml              # Docker Compose configuration
├── setup-cockroachdb.sh            # CockroachDB setup script
├── test-simple.sh                  # End-to-end test script
├── cleanup.sh                      # Environment cleanup script
├── kafka_consumer.py               # Kafka consumer for testing
├── process-config-template.sh      # Configuration template processor
└── configs/                        # Configuration files
    ├── cockroachdb-source.json     # Main connector configuration
    └── cockroachdb-source-template.json  # Template with environment variables
```

## 🚀 **Quick Start**

### **1. Start the Environment**
```bash
# Start all services
docker compose -f scripts/docker-compose.yml up -d

# Setup CockroachDB
./scripts/setup-cockroachdb.sh
```

### **2. Run End-to-End Test**
```bash
# Run the complete test suite
./scripts/test-simple.sh
```

### **3. Clean Up**
```bash
# Clean up all containers and volumes
./scripts/cleanup.sh
```

## 📋 **Available Scripts**

### **Core Scripts**

#### **`setup-cockroachdb.sh`**
Sets up CockroachDB with required configuration:
- Enables rangefeed feature
- Creates test database and user
- Creates test table with sample data
- Grants necessary permissions

**Usage:**
```bash
./scripts/setup-cockroachdb.sh
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
./scripts/test-simple.sh
```

#### **`cleanup.sh`**
Cleans up the test environment:
- Stops and removes Docker containers
- Removes networks and volumes
- Cleans up temporary files

**Usage:**
```bash
./scripts/cleanup.sh
./scripts/cleanup.sh --force  # Skip confirmation
./scripts/cleanup.sh --preview # Show what will be cleaned
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
Main connector configuration file with hardcoded values.

### **`configs/cockroachdb-source-template.json`**
Template configuration with environment variable placeholders for parameterization.

**Environment Variables Supported:**
- `COCKROACHDB_HOST` - CockroachDB hostname
- `COCKROACHDB_PORT` - CockroachDB port
- `DATABASE_USER` - Database username
- `DATABASE_PASSWORD` - Database password
- `DATABASE_NAME` - Database name
- `SCHEMA_NAME` - Schema name to monitor
- `TABLE_NAME` - Table name to monitor
- `KAFKA_HOST` - Kafka hostname
- `KAFKA_PORT` - Kafka port

## 🔧 **Environment Variables**

You can customize the test environment using these variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `COCKROACHDB_HOST` | `cockroachdb` | CockroachDB hostname |
| `COCKROACHDB_PORT` | `26257` | CockroachDB port |
| `DATABASE_USER` | `testuser` | Database username |
| `DATABASE_PASSWORD` | `` | Database password |
| `DATABASE_NAME` | `testdb` | Database name |
| `SCHEMA_NAME` | `public` | Schema name to monitor |
| `TABLE_NAME` | `products` | Table name to monitor |
| `KAFKA_HOST` | `kafka-test` | Kafka hostname |
| `KAFKA_PORT` | `9092` | Kafka port |

## 🧪 **Testing Workflows**

### **Development Testing**
```bash
# Quick test with default settings
./scripts/test-simple.sh
```

### **Custom Schema Testing**
```bash
# Test with custom schema and table
export SCHEMA_NAME=my_schema
export TABLE_NAME=orders
./scripts/test-simple.sh
```

### **Production-like Testing**
```bash
# Test with production-like configuration
export DATABASE_PASSWORD=secure_password
export COCKROACHDB_HOST=prod-cockroachdb.example.com
./scripts/process-config-template.sh \
  scripts/configs/cockroachdb-source-template.json \
  scripts/configs/cockroachdb-production.json
```

## 🚨 **Troubleshooting**

### **Common Issues**

1. **Docker not running**
   ```bash
   # Check Docker status
   docker info
   ```

2. **Port conflicts**
   ```bash
   # Check what's using the ports
   lsof -i :26257  # CockroachDB
   lsof -i :9092   # Kafka
   lsof -i :8083   # Kafka Connect
   ```

3. **Permission issues**
   ```bash
   # Check CockroachDB permissions
   docker exec cockroachdb cockroach sql --insecure --execute="
   SHOW GRANTS FOR debezium;
   "
   ```

4. **Clean slate**
   ```bash
   # Complete cleanup and restart
   ./scripts/cleanup.sh --force
   docker compose -f scripts/docker-compose.yml up -d
   ./scripts/setup-cockroachdb.sh
   ```

### **Logs and Debugging**

```bash
# Check service logs
docker compose -f scripts/docker-compose.yml logs cockroachdb
docker compose -f scripts/docker-compose.yml logs kafka
docker compose -f scripts/docker-compose.yml logs connect

# Check connector status
curl -s http://localhost:8083/connectors/cockroachdb-connector/status | jq '.'

# Check Kafka topics
docker exec kafka-test kafka-topics --bootstrap-server localhost:9092 --list
```

## 📚 **Related Documentation**

- [Main README](../README.md) - Project overview and configuration
- [Parameterization Guide](../PARAMETERIZATION_GUIDE.md) - Environment variable usage
- [Architecture Research](../ARCHITECTURE_RESEARCH.md) - Technical details
- [TODO](../TODO.md) - Development roadmap 