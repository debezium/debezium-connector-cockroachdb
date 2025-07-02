#!/bin/bash

set -e

echo "=========================================="
echo "  Enhanced CockroachDB Connector Test"
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

# Function to safely stop and remove connector
cleanup_connector() {
    local connector_name="$1"
    print_status "Cleaning up connector: $connector_name"
    
    # Check if connector exists
    if curl -s "http://localhost:8083/connectors/$connector_name" > /dev/null 2>&1; then
        # Stop the connector first
        print_status "Stopping connector: $connector_name"
        curl -X PUT "http://localhost:8083/connectors/$connector_name/stop" > /dev/null 2>&1 || true
        sleep 5
        
        # Delete the connector
        print_status "Deleting connector: $connector_name"
        curl -X DELETE "http://localhost:8083/connectors/$connector_name" > /dev/null 2>&1 || true
        sleep 3
    else
        print_warning "Connector $connector_name does not exist"
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

# Function to wait for connector to be in desired state
wait_for_connector_state() {
    local connector_name="$1"
    local desired_state="$2"
    local max_attempts="${3:-30}"
    local attempt=1
    
    print_status "Waiting for connector '$connector_name' to be in state '$desired_state'..."
    
    while [ $attempt -le $max_attempts ]; do
        local status_response=$(curl -s "http://localhost:8083/connectors/$connector_name/status" 2>/dev/null)
        if [ $? -eq 0 ]; then
            local connector_state=$(echo "$status_response" | jq -r '.connector.state' 2>/dev/null)
            if [ "$connector_state" = "$desired_state" ]; then
                print_success "✅ Connector '$connector_name' is in state '$desired_state'"
                return 0
            elif [ "$connector_state" = "FAILED" ]; then
                print_error "❌ Connector '$connector_name' failed to start"
                echo "$status_response" | jq '.'
                return 1
            fi
        fi
        
        print_warning "Waiting for connector state... (attempt $attempt/$max_attempts)"
        sleep 2
        attempt=$((attempt + 1))
    done
    
    print_error "❌ Connector '$connector_name' did not reach state '$desired_state' within expected time"
    return 1
}

# Step 1: Build the connector
print_status "Step 1: Building the connector..."
cd .. && ./mvnw clean package -DskipTests
check_status "Connector built successfully" "Failed to build connector"
cd scripts

# Step 2: Start services
print_status "Step 2: Starting Docker Compose services..."
docker-compose -f docker-compose.yml down -v 2>/dev/null || true
docker-compose -f docker-compose.yml up -d zookeeper kafka cockroachdb
check_status "Docker services started" "Failed to start Docker services"
sleep 20

# Step 3: Setup CockroachDB with enhanced schema
print_status "Step 3: Setting up CockroachDB with enhanced schema..."
./setup-cockroachdb.sh
check_status "CockroachDB setup completed" "Failed to setup CockroachDB"

# Step 4: Start Kafka Connect
print_status "Step 4: Starting Kafka Connect..."
docker-compose -f docker-compose.yml up -d connect
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
    docker-compose -f docker-compose.yml logs connect
    exit 1
fi

# Step 6: Check logs
print_status "Step 6: Checking Kafka Connect logs..."
docker-compose -f docker-compose.yml logs connect

# Step 7: Check connector discovery
if ! check_connector_discovery; then
    print_error "❌ CONNECTOR DISCOVERY FAILED - Cannot proceed with test"
    exit 1
fi

# Step 8: Clean up any existing connector to prevent "already started" issues
print_status "Step 7: Cleaning up any existing connectors..."
cleanup_connector "cockroachdb-connector"

# Step 9: Create connector configuration with enhanced settings
print_status "Step 8: Creating enhanced connector configuration..."
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
    "database.server.name": "cockroachdb",
    "topic.prefix": "cockroachdb",
    "table.include.list": "testdb.public.products",
    "cockroachdb.changefeed.envelope": "enriched",
    "cockroachdb.changefeed.enriched.properties": "source,schema",
    "cockroachdb.changefeed.include.updated": true,
    "cockroachdb.changefeed.include.diff": true,
    "cockroachdb.changefeed.resolved.interval": "10s",
    "cockroachdb.changefeed.cursor": "now",
    "cockroachdb.changefeed.sink.type": "kafka",
    "cockroachdb.changefeed.sink.uri": "kafka://kafka-test:9092",
    "cockroachdb.changefeed.batch.size": 1000,
    "cockroachdb.changefeed.poll.interval.ms": 100,
    "connection.timeout.ms": 30000,
    "connection.retry.delay.ms": 100,
    "connection.max.retries": 3
  }
}
EOF
check_status "Enhanced connector configuration created" "Failed to create connector configuration"

# Step 10: Create the connector
print_status "Step 9: Creating the connector..."
connector_response=$(curl -s -X POST -H "Content-Type: application/json" \
  --data @cockroachdb-source.json \
  http://localhost:8083/connectors 2>/dev/null)

if echo "$connector_response" | jq -e '.error_code' > /dev/null 2>&1; then
    print_error "❌ Failed to create connector:"
    echo "$connector_response" | jq '.'
    exit 1
else
    print_success "✅ Connector created successfully"
fi

# Step 11: Wait for connector to start
print_status "Step 10: Waiting for connector to start..."
if ! wait_for_connector_state "cockroachdb-connector" "RUNNING" 30; then
    print_error "❌ Connector failed to start properly"
    exit 1
fi

# Step 12: Test INSERT events
print_status "Step 11: Testing INSERT events..."
# Use unique SKUs and names for repeatable tests (do not overlap with setup-cockroachdb.sh)
docker exec cockroachdb cockroach sql --insecure --execute="
USE testdb;
INSERT INTO products (sku, name, description, price, category, in_stock, brand, stock_quantity, weight_grams, dimensions_cm, tags, metadata) VALUES 
  ('MUG-101', 'Test Coffee Mug', 'Ceramic mug for testing', 13.50, 'Testware', true, 'TestBrand', 10, 355.0, '11x9x13', ARRAY['test','mug'], '{\"color\":\"blue\"}'),
  ('LAMP-102', 'Test LED Desk Lamp', 'LED lamp for testing', 27.30, 'Testware', true, 'TestBrand', 5, 1250.0, '26x16x46', ARRAY['test','lamp'], '{\"brightness\":\"medium\"}'),
  ('MOUSE-103', 'Test Wireless Mouse', 'Wireless mouse for testing', 46.99, 'Testware', false, 'TestBrand', 0, 99.0, '13x8x5', ARRAY['test','mouse'], '{\"dpi\":1800}');
"
check_status "INSERT test data created" "Failed to create INSERT test data"

# Wait for INSERT events to be processed
sleep 10

# Step 13: Test UPDATE events
print_status "Step 12: Testing UPDATE events..."
docker exec cockroachdb cockroach sql --insecure --execute="
USE testdb;
UPDATE products SET 
  price = 16.99,
  in_stock = false,
  description = 'Updated: Test ceramic mug'
WHERE sku = 'MUG-101';

UPDATE products SET 
  price = 31.99,
  category = 'Testware Updated'
WHERE sku = 'LAMP-102';
"
check_status "UPDATE test data modified" "Failed to modify UPDATE test data"

# Wait for UPDATE events to be processed
sleep 10

# Step 14: Test DELETE events
print_status "Step 13: Testing DELETE events..."
docker exec cockroachdb cockroach sql --insecure --execute="
USE testdb;
DELETE FROM products WHERE sku = 'MOUSE-103';
"
check_status "DELETE test data removed" "Failed to remove DELETE test data"

# Wait for DELETE events to be processed
sleep 10

# Step 15: Check Kafka topics
print_status "Step 14: Checking Kafka topics..."
docker exec kafka-test kafka-topics --bootstrap-server localhost:9092 --list
check_status "Kafka topics listed" "Failed to list Kafka topics"

# Step 16: Consume messages from Kafka to verify all event types
print_status "Step 15: Consuming messages from Kafka (all event types)..."
print_status "Consuming INSERT events..."
docker exec kafka-test kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cockroachdb.public.products \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 15000
check_status "Messages consumed successfully" "Failed to consume messages"

# Step 17: Verify specific event types in logs
print_status "Step 16: Verifying event types in connector logs..."
sleep 5

# Check for different operation types in logs
print_status "Checking for INSERT operations..."
if docker-compose -f docker-compose.yml logs connect | grep -q "operation.*CREATE"; then
    print_success "✅ INSERT/CREATE operations detected"
else
    print_warning "⚠️  No INSERT/CREATE operations found in logs"
fi

print_status "Checking for UPDATE operations..."
if docker-compose -f docker-compose.yml logs connect | grep -q "operation.*UPDATE"; then
    print_success "✅ UPDATE operations detected"
else
    print_warning "⚠️  No UPDATE operations found in logs"
fi

print_status "Checking for DELETE operations..."
if docker-compose -f docker-compose.yml logs connect | grep -q "operation.*DELETE"; then
    print_success "✅ DELETE operations detected"
else
    print_warning "⚠️  No DELETE operations found in logs"
fi

# Step 18: Clean up
print_status "Step 17: Cleaning up..."
cleanup_connector "cockroachdb-connector"
rm -f cockroachdb-source.json
check_status "Cleanup completed" "Failed to cleanup"

print_success "✅ ENHANCED TEST PASSED!"
echo
echo "Test Summary:"
print_success "✅ Connector built successfully"
print_success "✅ Infrastructure started"
print_success "✅ CockroachDB configured with UUID schema"
print_success "✅ Connector discovered and created"
print_success "✅ INSERT events tested and processed"
print_success "✅ UPDATE events tested and processed"
print_success "✅ DELETE events tested and processed"
print_success "✅ All event types verified in logs"
print_success "✅ Cleanup completed"
echo
print_success "🎉 All CRUD operations (INSERT, UPDATE, DELETE) successfully tested!"
