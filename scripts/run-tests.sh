#!/bin/bash

# Test runner script that demonstrates how to use parameterized test scripts
# This script shows different ways to run tests with custom configurations

set -e

echo "=========================================="
echo "  CockroachDB Connector Test Runner"
echo "=========================================="
echo

# Configuration variables with defaults
# These can be overridden by environment variables or command line arguments
VERSION=${VERSION:-"3.2.0-SNAPSHOT"}
CONNECTOR_NAME=${CONNECTOR_NAME:-"debezium-connector-cockroachdb"}
JAR_NAME=${JAR_NAME:-"debezium-connector-cockroachdb-${VERSION}-test-uber-jar-with-dependencies.jar"}
KAFKA_CONNECT_PORT=${KAFKA_CONNECT_PORT:-"8083"}
COCKROACHDB_PORT=${COCKROACHDB_PORT:-"26257"}
KAFKA_PORT=${KAFKA_PORT:-"9092"}
ZOOKEEPER_PORT=${ZOOKEEPER_PORT:-"2181"}
DATABASE_NAME=${DATABASE_NAME:-"testdb"}
DATABASE_USER=${DATABASE_USER:-"debezium"}
DATABASE_PASSWORD=${DATABASE_PASSWORD:-""}
SERVER_NAME=${SERVER_NAME:-"cockroachdb-server"}
TOPIC_PREFIX=${TOPIC_PREFIX:-"cockroachdb"}
CONNECTOR_INSTANCE_NAME=${CONNECTOR_INSTANCE_NAME:-"cockroachdb-connector"}
MAX_CONNECT_ATTEMPTS=${MAX_CONNECT_ATTEMPTS:-"20"}
CONNECT_RETRY_DELAY=${CONNECT_RETRY_DELAY:-"5"}
DATA_PROCESSING_DELAY=${DATA_PROCESSING_DELAY:-"15"}
CHANGE_PROCESSING_DELAY=${CHANGE_PROCESSING_DELAY:-"10"}
CONNECTOR_START_DELAY=${CONNECTOR_START_DELAY:-"10"}
MAX_DISCOVERY_ATTEMPTS=${MAX_DISCOVERY_ATTEMPTS:-"10"}
DISCOVERY_RETRY_DELAY=${DISCOVERY_RETRY_DELAY:-"5"}
CONNECT_START_TIMEOUT=${CONNECT_START_TIMEOUT:-"15"}
CLEANUP_ON_START=${CLEANUP_ON_START:-"false"}
CLEANUP_ON_EXIT=${CLEANUP_ON_EXIT:-"false"}

# Function to print usage
print_usage() {
    echo "Usage: $0 [OPTIONS] <test-script>"
    echo
    echo "Test Scripts:"
    echo "  test-connect-discovery.sh    Test Kafka Connect connector discovery"
    echo "  test-end-to-end-sink.sh      Test end-to-end workflow with sink support"
    echo "  cleanup.sh                   Clean up test environment"
    echo
    echo "Options:"
    echo "  --version VERSION            Connector version (default: 3.2.0-SNAPSHOT)"
    echo "  --port PORT                  Kafka Connect port (default: 8083)"
    echo "  --database DATABASE          Database name (default: testdb)"
    echo "  --user USER                  Database user (default: debezium)"
    echo "  --cleanup-start              Clean up containers before starting test"
    echo "  --cleanup-exit               Clean up containers after test completes"
    echo "  --help                       Show this help message"
    echo
    echo "Environment Variables:"
    echo "  VERSION                      Connector version"
    echo "  KAFKA_CONNECT_PORT           Kafka Connect port"
    echo "  DATABASE_NAME                Database name"
    echo "  DATABASE_USER                Database user"
    echo "  CLEANUP_ON_START             Clean up containers before starting"
    echo "  CLEANUP_ON_EXIT              Clean up containers after completion"
    echo
    echo "Examples:"
    echo "  $0 test-connect-discovery.sh"
    echo "  $0 --version 3.1.0 --port 8084 test-end-to-end-sink.sh"
    echo "  $0 --cleanup-start --cleanup-exit test-connect-discovery.sh"
    echo "  CLEANUP_ON_START=true $0 test-end-to-end-sink.sh"
    echo
}

# Parse command line arguments
TEST_SCRIPT=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            VERSION="$2"
            shift 2
            ;;
        --port)
            KAFKA_CONNECT_PORT="$2"
            shift 2
            ;;
        --database)
            DATABASE_NAME="$2"
            shift 2
            ;;
        --user)
            DATABASE_USER="$2"
            shift 2
            ;;
        --cleanup-start)
            CLEANUP_ON_START="true"
            shift
            ;;
        --cleanup-exit)
            CLEANUP_ON_EXIT="true"
            shift
            ;;
        --help)
            print_usage
            exit 0
            ;;
        *)
            if [ -z "$TEST_SCRIPT" ]; then
                TEST_SCRIPT="$1"
            else
                echo "Error: Multiple test scripts specified"
                exit 1
            fi
            shift
            ;;
    esac
done

# Check if test script is provided
if [ -z "$TEST_SCRIPT" ]; then
    echo "Error: No test script specified"
    print_usage
    exit 1
fi

# Check if test script exists
if [ ! -f "scripts/$TEST_SCRIPT" ]; then
    echo "Error: Test script 'scripts/$TEST_SCRIPT' not found"
    print_usage
    exit 1
fi

# Set environment variables for the test script
export VERSION="$VERSION"
export KAFKA_CONNECT_PORT="$KAFKA_CONNECT_PORT"
export DATABASE_NAME="$DATABASE_NAME"
export DATABASE_USER="$DATABASE_USER"

# Export configuration for test scripts
export CONNECTOR_NAME
export JAR_NAME
export COCKROACHDB_PORT
export KAFKA_PORT
export ZOOKEEPER_PORT
export DATABASE_PASSWORD
export SERVER_NAME
export TOPIC_PREFIX
export CONNECTOR_INSTANCE_NAME
export MAX_CONNECT_ATTEMPTS
export CONNECT_RETRY_DELAY
export DATA_PROCESSING_DELAY
export CHANGE_PROCESSING_DELAY
export CONNECTOR_START_DELAY
export MAX_DISCOVERY_ATTEMPTS
export DISCOVERY_RETRY_DELAY
export CONNECT_START_TIMEOUT
export CLEANUP_ON_START
export CLEANUP_ON_EXIT

echo "Configuration:"
echo "  Version: $VERSION"
echo "  Kafka Connect Port: $KAFKA_CONNECT_PORT"
echo "  Database: $DATABASE_NAME"
echo "  User: $DATABASE_USER"
echo "  Test Script: $TEST_SCRIPT"
echo

# Run the test script
echo "Running test script: scripts/$TEST_SCRIPT"
echo "=========================================="
./scripts/$TEST_SCRIPT

echo
echo "Test completed!" 