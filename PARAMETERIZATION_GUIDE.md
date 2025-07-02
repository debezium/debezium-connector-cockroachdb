# CockroachDB Connector Parameterization Guide

## Overview

This guide explains how the CockroachDB connector has been parameterized to avoid hardcoded values and provide flexibility for different environments.

## 🎯 **What Has Been Parameterized**

### **Phase 1: Critical Schema Parameterization (✅ Complete)**

#### **Java Code Changes**
- **`CockroachDBConnectorConfig.java`**: Added `SCHEMA_NAME` configuration field
- **`CockroachDBSchema.java`**: Updated SQL queries to use parameterized schema
- **`CockroachDBConnection.java`**: Updated permission checks to use parameterized schema

#### **Configuration Parameter**
```java
public static final Field SCHEMA_NAME = Field.create("cockroachdb.schema.name")
    .withDisplayName("Schema name")
    .withType(Type.STRING)
    .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 1))
    .withDefault("public")
    .withWidth(Width.MEDIUM)
    .withImportance(Importance.MEDIUM)
    .withDescription("The schema name to monitor for changes. Defaults to 'public' schema.");
```

### **Phase 2: Configuration Files Parameterization (🔄 In Progress)**

#### **Environment Variables**
The following environment variables can be used to customize the connector:

| Variable | Default | Description |
|----------|---------|-------------|
| `COCKROACHDB_HOST` | `cockroachdb` | CockroachDB hostname |
| `COCKROACHDB_PORT` | `26257` | CockroachDB port |
| `DATABASE_USER` | `testuser` | Database username |
| `DATABASE_PASSWORD` | `` | Database password |
| `DATABASE_NAME` | `testdb` | Database name |
| `DATABASE_SERVER_NAME` | `cockroachdb` | Server name for Kafka topics |
| `SCHEMA_NAME` | `public` | Schema name to monitor |
| `TABLE_NAME` | `products` | Table name to monitor |
| `KAFKA_HOST` | `kafka-test` | Kafka hostname |
| `KAFKA_PORT` | `9092` | Kafka port |

#### **Configuration Templates**
- **Template**: `scripts/configs/cockroachdb-source-template.json`
- **Processor**: `scripts/process-config-template.sh`

## 🚀 **How to Use Parameterization**

### **1. Using Environment Variables**

```bash
# Set environment variables
export SCHEMA_NAME=my_schema
export TABLE_NAME=orders
export DATABASE_NAME=production_db

# Process template to generate configuration
cd scripts
./process-config-template.sh configs/cockroachdb-source-template.json configs/cockroachdb-production.json
```

### **2. Using the Connector Configuration**

```json
{
  "name": "cockroachdb-connector",
  "config": {
    "connector.class": "io.debezium.connector.cockroachdb.CockroachDBConnector",
    "database.hostname": "my-cockroachdb-host",
    "database.port": "26257",
    "database.user": "debezium",
    "database.password": "secret",
    "database.dbname": "my_database",
    "cockroachdb.schema.name": "my_schema",
    "table.include.list": "my_schema.my_table"
  }
}
```

### **3. Using Docker Compose with Environment Variables**

```yaml
version: '3.8'
services:
  connect:
    image: debezium/connect:3.0.0.Final
    environment:
      - COCKROACHDB_HOST=cockroachdb
      - SCHEMA_NAME=public
      - DATABASE_NAME=testdb
      - DATABASE_USER=debezium
    volumes:
      - ./scripts/configs:/kafka/config
```

## 📋 **Remaining Hardcoded Values (Future Phases)**

### **Phase 3: Docker and Version Management**
- [ ] Docker image versions in `docker-compose.yml`
- [ ] Version numbers in scripts and configuration
- [ ] Container names in scripts

### **Phase 4: Test Data and Scripts**
- [ ] Test data values in scripts
- [ ] Hardcoded table names in test scripts
- [ ] Port numbers in test scripts

## 🔧 **Tools and Scripts**

### **Configuration Template Processor**
```bash
# Usage
./scripts/process-config-template.sh <template_file> <output_file>

# Example
./scripts/process-config-template.sh \
  scripts/configs/cockroachdb-source-template.json \
  scripts/configs/cockroachdb-production.json
```

### **Environment Variable Examples**
```bash
# Development
export SCHEMA_NAME=public
export TABLE_NAME=products
export DATABASE_NAME=testdb

# Production
export SCHEMA_NAME=app_schema
export TABLE_NAME=orders
export DATABASE_NAME=production_db
export DATABASE_PASSWORD=secure_password

# Testing
export SCHEMA_NAME=test_schema
export TABLE_NAME=test_table
export DATABASE_NAME=test_db
```

## ✅ **Benefits of Parameterization**

1. **Environment Flexibility**: Easy to switch between dev, staging, and production
2. **Security**: Sensitive values can be managed via environment variables
3. **Maintainability**: No need to modify code for different environments
4. **Scalability**: Support for multiple schemas and configurations
5. **CI/CD Friendly**: Easy integration with deployment pipelines

## 🧪 **Testing Parameterization**

### **Test Different Schemas**
```bash
# Test with custom schema
SCHEMA_NAME=custom_schema TABLE_NAME=orders \
  ./scripts/process-config-template.sh \
  scripts/configs/cockroachdb-source-template.json \
  scripts/configs/cockroachdb-custom.json

# Verify the generated configuration
cat scripts/configs/cockroachdb-custom.json
```

### **Test with Different Environments**
```bash
# Development environment
export COCKROACHDB_HOST=localhost
export DATABASE_NAME=dev_db
./scripts/process-config-template.sh ...

# Production environment  
export COCKROACHDB_HOST=prod-cockroachdb.example.com
export DATABASE_NAME=prod_db
export DATABASE_PASSWORD=prod_secret
./scripts/process-config-template.sh ...
```

## 🔄 **Migration Guide**

### **From Hardcoded to Parameterized**

**Before (Hardcoded)**:
```json
{
  "table.include.list": "public.products",
  "database.hostname": "cockroachdb"
}
```

**After (Parameterized)**:
```json
{
  "table.include.list": "${SCHEMA_NAME:-public}.${TABLE_NAME:-products}",
  "database.hostname": "${COCKROACHDB_HOST:-cockroachdb}"
}
```

### **Backward Compatibility**
- All changes maintain backward compatibility
- Default values ensure existing configurations continue to work
- No breaking changes to existing deployments

## 📚 **Related Files**

- `src/main/java/io/debezium/connector/cockroachdb/CockroachDBConnectorConfig.java`
- `src/main/java/io/debezium/connector/cockroachdb/CockroachDBSchema.java`
- `src/main/java/io/debezium/connector/cockroachdb/connection/CockroachDBConnection.java`
- `scripts/configs/cockroachdb-source-template.json`
- `scripts/process-config-template.sh`
- `docker-compose.yml`

## 🎯 **Next Steps**

1. **Complete Phase 2**: Update all configuration files to use templates
2. **Phase 3**: Parameterize Docker image versions and container names
3. **Phase 4**: Parameterize test data and script configurations
4. **Documentation**: Update README and test scripts with parameterization examples
5. **CI/CD**: Integrate parameterization into deployment pipelines 