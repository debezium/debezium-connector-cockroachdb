#!/bin/bash

set -e

echo "=========================================="
echo "  Simple CockroachDB Connector Test"
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

# Function to check if connector is discovered
check_connector_discovery() {
    print_status "Checking for CockroachDB connector discovery..."
    
    local response=$(curl -s http://localhost:8083/connector-plugins 2>/dev/null)
    if [ $? -ne 0 ]; then
        print_error "Failed to get connector plugins"
        return 1
    fi
    
    local cockroach_connector=$(echo "$response" | jq -r '.[] | select(.class | contains("cockroachdb")) | .class' 2>/dev/null)
    
    if [ "$cockroach_connector" != "" ] && [ "$cockroach_connector" != "null" ]; then
        print_success "✅ CockroachDB connector found: $cockroach_connector"
        return 0
    else
        print_error "❌ CockroachDB connector not found"
        print_status "Available connectors:"
        echo "$response" | jq '.[].class' 2>/dev/null || print_error "Failed to get connector list"
        return 1
    fi
}

# Step 1: Build the connector
print_status "Step 1: Building the connector..."
./mvnw clean package -DskipTests
check_status "Connector built successfully" "Failed to build connector"

# Step 2: Start services
print_status "Step 2: Starting Docker Compose services..."
docker-compose down -v 2>/dev/null || true
docker-compose up -d zookeeper kafka cockroachdb
check_status "Docker services started" "Failed to start Docker services"
sleep 20

# Step 3: Setup CockroachDB
print_status "Step 3: Setting up CockroachDB..."
./scripts/setup-cockroachdb.sh
check_status "CockroachDB setup completed" "Failed to setup CockroachDB"

# Step 4: Start Kafka Connect
print_status "Step 4: Starting Kafka Connect..."
docker-compose up -d connect
check_status "Kafka Connect container started" "Failed to start Kafka Connect container"
sleep 30

# Step 5: Check if Kafka Connect is running
print_status "Step 5: Checking Kafka Connect status..."
connect_ready=false
for i in {1..15}; do
    if curl -s http://localhost:8083/ > /dev/null 2>&1; then
        print_success "Kafka Connect is running"
        connect_ready=true
        break
    else
        print_warning "Waiting for Kafka Connect... (attempt $i/15)"
        sleep 10
    fi
done

if [ "$connect_ready" = false ]; then
    print_error "❌ Kafka Connect failed to start"
    docker-compose logs connect
    exit 1
fi

# Step 6: Check logs
print_status "Step 6: Checking Kafka Connect logs..."
docker-compose logs connect

# Step 7: Check connector discovery
if ! check_connector_discovery; then
    print_error "❌ CONNECTOR DISCOVERY FAILED - Cannot proceed with test"
    exit 1
fi

# Step 8: Create connector configuration
print_status "Step 7: Creating connector configuration..."
cat > cockroachdb-source.json << EOF
{
  "name": "cockroachdb-connector",
  "config": {
    "connector.class": "io.debezium.connector.cockroachdb.CockroachDBConnector",
    "database.hostname": "cockroachdb",
    "database.port": "26257",
    "database.user": "debezium",
    "database.password": "",
    "database.dbname": "testdb",
    "database.server.name": "cockroachdb-server",
    "topic.prefix": "cockroachdb",
    "table.include.list": "testdb.public.products"
  }
}
EOF
check_status "Connector configuration created" "Failed to create connector configuration"

# Step 9: Create the connector
print_status "Step 8: Creating the connector..."
local connector_response=$(curl -s -X POST -H "Content-Type: application/json" \
  --data @cockroachdb-source.json \
  http://localhost:8083/connectors 2>/dev/null)

if echo "$connector_response" | jq -e '.error_code' > /dev/null 2>&1; then
    print_error "❌ Failed to create connector:"
    echo "$connector_response" | jq '.'
    exit 1
else
    print_success "✅ Connector created successfully"
fi

# Step 10: Wait for connector to start
print_status "Step 9: Waiting for connector to start..."
sleep 10

# Step 11: Check connector status
print_status "Step 10: Checking connector status..."
local status_response=$(curl -s http://localhost:8083/connectors/cockroachdb-connector/status)
if echo "$status_response" | jq -e '.connector.state' > /dev/null 2>&1; then
    local connector_state=$(echo "$status_response" | jq -r '.connector.state')
    if [ "$connector_state" = "RUNNING" ]; then
        print_success "✅ Connector is running"
    else
        print_error "❌ Connector is not running. State: $connector_state"
        echo "$status_response" | jq '.'
        exit 1
    fi
else
    print_error "❌ Failed to get connector status"
    exit 1
fi

# Step 12: Create test data
print_status "Step 11: Creating test data..."
docker exec cockroachdb cockroach sql --insecure --execute="
USE testdb;
INSERT INTO products (name, description, price, category) VALUES 
  ('Test Product 1', 'Description 1', 10.99, 'Test Category'),
  ('Test Product 2', 'Description 2', 20.99, 'Test Category');
"
check_status "Test data created" "Failed to create test data"

# Step 13: Wait for data to be processed
print_status "Step 12: Waiting for data to be processed..."
sleep 15

# Step 14: Check Kafka topics
print_status "Step 13: Checking Kafka topics..."
docker exec kafka-test kafka-topics --bootstrap-server localhost:9092 --list
check_status "Kafka topics listed" "Failed to list Kafka topics"

# Step 15: Consume messages from Kafka
print_status "Step 14: Consuming messages from Kafka..."
docker exec kafka-test kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cockroachdb.public.products \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 10000
check_status "Messages consumed successfully" "Failed to consume messages"

# Step 16: Clean up
print_status "Step 15: Cleaning up..."
curl -X DELETE http://localhost:8083/connectors/cockroachdb-connector 2>/dev/null || true
rm -f cockroachdb-source.json
check_status "Cleanup completed" "Failed to cleanup"

print_success "✅ SIMPLE TEST PASSED!"
echo
echo "Test Summary:"
print_success "✅ Connector built successfully"
print_success "✅ Infrastructure started"
print_success "✅ CockroachDB configured"
print_success "✅ Connector discovered and created"
print_success "✅ Test data processed"
print_success "✅ Messages consumed from Kafka"
print_success "✅ Cleanup completed" 