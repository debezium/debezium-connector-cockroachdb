# Test Scripts for Debezium CockroachDB Connector

This directory contains test scripts for the Debezium CockroachDB connector. All scripts are parameterized and can be customized via environment variables or command-line arguments.

## Quick Start

Use the test runner script for easy execution:

```bash
# Run connector discovery test
./scripts/run-tests.sh test-connect-discovery.sh

# Run end-to-end test with cleanup
./scripts/run-tests.sh --cleanup-start --cleanup-exit test-end-to-end-sink.sh

# Clean up test environment
./scripts/run-tests.sh cleanup.sh
```

## Available Scripts

### 1. `run-tests.sh` - Test Runner
Main test runner script that provides a unified interface for all tests.

**Usage:**
```bash
./scripts/run-tests.sh [OPTIONS] <test-script>
```

**Options:**
- `--version VERSION` - Connector version (default: 3.2.0-SNAPSHOT)
- `--port PORT` - Kafka Connect port (default: 8083)
- `--database DATABASE` - Database name (default: testdb)
- `--user USER` - Database user (default: debezium)
- `--cleanup-start` - Clean up containers before starting test
- `--cleanup-exit` - Clean up containers after test completes
- `--help` - Show help message

**Examples:**
```bash
# Basic usage
./scripts/run-tests.sh test-connect-discovery.sh

# Custom configuration
./scripts/run-tests.sh --version 3.1.0 --port 8084 test-end-to-end-sink.sh

# With cleanup
./scripts/run-tests.sh --cleanup-start --cleanup-exit test-connect-discovery.sh
```

### 2. `cleanup.sh` - Environment Cleanup
Standalone script to clean up test environment containers and networks.

**Usage:**
```bash
./scripts/cleanup.sh [OPTIONS]
```

**Options:**
- `--preview, -p` - Show what will be cleaned up without actually doing it
- `--force, -f` - Force cleanup without confirmation
- `--help, -h` - Show help message

**Examples:**
```bash
# Interactive cleanup
./scripts/cleanup.sh

# Preview what will be cleaned up
./scripts/cleanup.sh --preview

# Force cleanup without confirmation
./scripts/cleanup.sh --force
```

### 3. `test-connect-discovery.sh` - Connector Discovery Test
Tests if the connector can be discovered by Kafka Connect.

**Features:**
- Starts Kafka Connect with the connector JAR
- Verifies connector discovery
- Tests connector configuration validation
- Includes cleanup functionality

**Usage:**
```bash
# Direct execution
./scripts/test-connect-discovery.sh

# With cleanup
CLEANUP_ON_START=true ./scripts/test-connect-discovery.sh

# With custom parameters
VERSION=3.1.0 KAFKA_CONNECT_PORT=8084 ./scripts/test-connect-discovery.sh
```

### 4. `test-end-to-end-sink.sh` - End-to-End Test with Sink Support
Comprehensive test that demonstrates the full workflow including sink support.

**Features:**
- Sets up CockroachDB with test data
- Configures connector with enriched envelope
- Tests change capture and processing
- Validates sink compatibility
- Includes permission checking
- Comprehensive cleanup

**Usage:**
```bash
# Direct execution
./scripts/test-end-to-end-sink.sh

# With cleanup
CLEANUP_ON_START=true CLEANUP_ON_EXIT=true ./scripts/test-end-to-end-sink.sh

# With custom database
DATABASE_NAME=mydb DATABASE_USER=myuser ./scripts/test-end-to-end-sink.sh
```

## Configuration Parameters

All scripts support the following configuration parameters:

### Core Parameters
- `VERSION` - Connector version (default: 3.2.0-SNAPSHOT)
- `CONNECTOR_NAME` - Connector name (default: debezium-connector-cockroachdb)
- `JAR_NAME` - JAR file name (auto-generated from version)

### Port Configuration
- `KAFKA_CONNECT_PORT` - Kafka Connect REST API port (default: 8083)
- `COCKROACHDB_PORT` - CockroachDB port (default: 26257)
- `KAFKA_PORT` - Kafka broker port (default: 9092)
- `ZOOKEEPER_PORT` - Zookeeper port (default: 2181)

### Database Configuration
- `DATABASE_NAME` - Database name (default: testdb)
- `DATABASE_USER` - Database user (default: debezium)
- `DATABASE_PASSWORD` - Database password (default: empty for insecure mode)

### Connector Configuration
- `SERVER_NAME` - Server name for connector (default: cockroachdb-server)
- `TOPIC_PREFIX` - Topic prefix (default: cockroachdb)
- `CONNECTOR_INSTANCE_NAME` - Connector instance name (default: cockroachdb-connector)

### Timing Configuration
- `MAX_CONNECT_ATTEMPTS` - Max connection attempts (default: 20)
- `CONNECT_RETRY_DELAY` - Connection retry delay in seconds (default: 5)
- `DATA_PROCESSING_DELAY` - Data processing delay in seconds (default: 15)
- `CHANGE_PROCESSING_DELAY` - Change processing delay in seconds (default: 10)
- `CONNECTOR_START_DELAY` - Connector start delay in seconds (default: 10)
- `MAX_DISCOVERY_ATTEMPTS` - Max discovery attempts (default: 10)
- `DISCOVERY_RETRY_DELAY` - Discovery retry delay in seconds (default: 5)
- `CONNECT_START_TIMEOUT` - Connect start timeout in seconds (default: 15)

### Cleanup Configuration
- `CLEANUP_ON_START` - Clean up containers before starting (default: false)
- `CLEANUP_ON_EXIT` - Clean up containers after completion (default: false)

## Environment Variables

You can override any parameter using environment variables:

```bash
# Set custom version and ports
export VERSION="3.1.0"
export KAFKA_CONNECT_PORT="8084"
export DATABASE_NAME="mydb"

# Run test with custom configuration
./scripts/run-tests.sh test-connect-discovery.sh
```

## Cleanup and Container Management

### Automatic Cleanup
Scripts can automatically clean up containers to prevent conflicts:

```bash
# Clean up before starting
CLEANUP_ON_START=true ./scripts/test-connect-discovery.sh

# Clean up after completion
CLEANUP_ON_EXIT=true ./scripts/test-end-to-end-sink.sh

# Both
CLEANUP_ON_START=true CLEANUP_ON_EXIT=true ./scripts/test-end-to-end-sink.sh
```

### Manual Cleanup
Use the cleanup script to manually clean up the test environment:

```bash
# Interactive cleanup
./scripts/cleanup.sh

# Preview what will be cleaned up
./scripts/cleanup.sh --preview

# Force cleanup
./scripts/cleanup.sh --force
```

### What Gets Cleaned Up
The cleanup process removes:
- All docker-compose containers and volumes
- Containers with project labels
- Specific containers (zookeeper, cockroachdb, kafka-test, connect)
- Custom networks (debezium-test)
- Dangling volumes and networks

## Troubleshooting

### Container Name Conflicts
If you encounter container name conflicts:

```bash
# Clean up existing containers
./scripts/cleanup.sh

# Or use automatic cleanup
CLEANUP_ON_START=true ./scripts/run-tests.sh test-connect-discovery.sh
```

### Port Conflicts
If ports are already in use:

```bash
# Use different ports
KAFKA_CONNECT_PORT=8084 COCKROACHDB_PORT=26258 ./scripts/run-tests.sh test-connect-discovery.sh
```

### Permission Issues
If you encounter permission issues:

```bash
# Make scripts executable
chmod +x scripts/*.sh

# Run with sudo if needed (for Docker)
sudo ./scripts/run-tests.sh test-connect-discovery.sh
```

### Build Issues
If the connector JAR is not found:

```bash
# Build the connector first
mvn clean package -Passembly

# Then run tests
./scripts/run-tests.sh test-connect-discovery.sh
```

## Best Practices

1. **Always use cleanup** when running tests to prevent conflicts
2. **Use the test runner** for consistent parameter handling
3. **Set environment variables** for repeated custom configurations
4. **Check logs** if tests fail for detailed error information
5. **Use preview mode** with cleanup to see what will be affected

## Contributing

When adding new test scripts:

1. Follow the existing parameterization pattern
2. Include cleanup functionality
3. Add proper error handling and status messages
4. Document the script in this README
5. Test with different parameter combinations

## Examples

### Basic Testing Workflow
```bash
# 1. Clean up any existing environment
./scripts/cleanup.sh

# 2. Test connector discovery
./scripts/run-tests.sh test-connect-discovery.sh

# 3. Run full end-to-end test
./scripts/run-tests.sh --cleanup-start --cleanup-exit test-end-to-end-sink.sh
```

### Custom Configuration Testing
```bash
# Test with custom version and database
VERSION=3.1.0 DATABASE_NAME=customdb DATABASE_USER=customuser \
./scripts/run-tests.sh --cleanup-start test-end-to-end-sink.sh
```

### Development Workflow
```bash
# Quick discovery test during development
./scripts/run-tests.sh --cleanup-start test-connect-discovery.sh

# Full test with cleanup
./scripts/run-tests.sh --cleanup-start --cleanup-exit test-end-to-end-sink.sh
``` 