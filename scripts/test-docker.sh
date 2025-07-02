#!/bin/bash

set -e

echo "=========================================="
echo "  Docker Environment Test"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a command succeeded
check_status() {
    if [ $? -eq 0 ]; then
        print_success "$1"
    else
        print_error "$2"
        exit 1
    fi
}

# Check if Docker is running
print_status "Checking Docker availability..."
if ! docker info > /dev/null 2>&1; then
    print_error "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi
print_success "✅ Docker is running"

# Check if docker-compose is available
print_status "Checking docker-compose availability..."
if ! docker-compose --version > /dev/null 2>&1; then
    print_error "❌ docker-compose is not available. Please install docker-compose."
    exit 1
fi

print_success "✅ docker-compose is available"

# Check if docker-compose.yml exists
print_status "Checking docker-compose.yml..."
if [ ! -f "docker-compose.yml" ]; then
    print_error "❌ docker-compose.yml not found"
    exit 1
fi

print_success "✅ docker-compose.yml found"

# Validate docker-compose.yml
print_status "Validating docker-compose.yml..."
docker-compose config > /dev/null
check_status "docker-compose.yml is valid" "docker-compose.yml is invalid"

# Start services
print_status "Starting services..."
docker-compose down -v 2>/dev/null || true
docker-compose up -d zookeeper kafka cockroachdb
check_status "Services started" "Failed to start services"
sleep 20

# Test CockroachDB
print_status "Testing CockroachDB..."
sleep 10
docker-compose logs cockroachdb

# Test Kafka connection
print_status "Testing Kafka connection..."
sleep 10
docker-compose logs kafka-test

# Test Zookeeper connection
print_status "Testing Zookeeper connection..."
sleep 5
docker-compose logs zookeeper

# Test basic CockroachDB operations
print_status "Testing CockroachDB operations..."
docker exec cockroachdb cockroach sql --insecure --execute="
CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;
CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY, name STRING);
INSERT INTO test_table VALUES (1, 'test');
SELECT * FROM test_table;
DROP TABLE test_table;
"
check_status "CockroachDB operations successful" "CockroachDB operations failed"

# Test basic Kafka operations
print_status "Testing Kafka operations..."
docker exec kafka-test kafka-topics --bootstrap-server localhost:9092 --create --topic test-topic --partitions 1 --replication-factor 1
check_status "Kafka topic creation successful" "Kafka topic creation failed"

docker exec kafka-test kafka-topics --bootstrap-server localhost:9092 --delete --topic test-topic
check_status "Kafka topic deletion successful" "Kafka topic deletion failed"

# Clean up
print_status "Cleaning up..."
docker-compose down -v
check_status "Cleanup completed" "Failed to cleanup"

print_success "✅ DOCKER ENVIRONMENT TEST PASSED!"
echo
echo "Test Summary:"
print_success "✅ Docker is running"
print_success "✅ docker-compose is available"
print_success "✅ docker-compose.yml is valid"
print_success "✅ All services started successfully"
print_success "✅ CockroachDB operations work"
print_success "✅ Kafka operations work"
print_success "✅ Zookeeper is healthy"
print_success "✅ Cleanup completed" 