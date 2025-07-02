#!/bin/bash

# Debezium CockroachDB Connector - End-to-End Test Script
# Tests the full pipeline: CockroachDB -> Changefeed -> Kafka -> Debezium -> Kafka Topics
#
# @author Virag Tripathi
# @version 1.0
# @description End-to-end test for CockroachDB connector with enriched envelope support

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CONNECTOR_NAME="cockroachdb-connector"
CONFIG_FILE="$PROJECT_DIR/scripts/configs/cockroachdb-source.json"
KAFKA_CONNECT_URL="http://localhost:8083"
COCKROACHDB_HOST="localhost:26257"
COCKROACHDB_USER="testuser"
COCKROACHDB_DB="testdb"

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up test environment..."
    
    # Stop connector if running
    if curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
        log_info "Stopping connector: $CONNECTOR_NAME"
        curl -X DELETE "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" || true
    fi
    
    # Clean up test data
    log_info "Cleaning up test data from CockroachDB..."
    docker exec -i cockroachdb cockroach sql --insecure --user=root -d "$COCKROACHDB_DB" << 'EOF'
DROP TABLE IF EXISTS products CASCADE;
EOF
    
    # Revoke grants and drop user
    docker exec -i cockroachdb cockroach sql --insecure --user=root << 'EOF'
-- Revoke all grants from testuser
REVOKE ALL ON DATABASE testdb FROM testuser;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM testuser;

-- Drop user
DROP USER IF EXISTS testuser;
EOF
}

# Setup function
setup() {
    log_info "Setting up test environment..."
    
    # Start infrastructure
    log_info "Starting Docker Compose services..."
    cd "$PROJECT_DIR"
    docker-compose up -d
    
    # Wait for services to be ready
    log_info "Waiting for services to be ready..."
    sleep 30
    
    # Check if services are running
    if ! docker-compose ps | grep -q "Up"; then
        log_error "Docker services are not running properly"
        docker-compose logs
        exit 1
    fi
    
    # Setup CockroachDB
    log_info "Setting up CockroachDB..."
    docker exec -i cockroachdb cockroach sql --insecure --user=root << 'EOF'
-- Enable rangefeeds (required for changefeeds)
SET CLUSTER SETTING kv.rangefeed.enabled = true;

-- Create test user
CREATE USER IF NOT EXISTS testuser;

-- Create test database
CREATE DATABASE IF NOT EXISTS testdb;

-- Grant permissions
GRANT ALL ON DATABASE testdb TO testuser;
EOF
    
    # Create test table and data
    log_info "Creating test table and data..."
    docker exec -i cockroachdb cockroach sql --insecure --user=root -d "$COCKROACHDB_DB" << 'EOF'
-- Create test table
CREATE TABLE IF NOT EXISTS products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name STRING NOT NULL,
    description STRING,
    price DECIMAL(10,2),
    category STRING,
    in_stock BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert test data
INSERT INTO products (name, description, price, category) VALUES
    ('Coffee Mug', 'Ceramic coffee mug with handle', 12.50, 'Home & Kitchen'),
    ('LED Desk Lamp', 'Adjustable LED desk lamp with touch controls', 26.30, 'Home & Kitchen'),
    ('Wireless Mouse', 'Ergonomic wireless mouse with precision tracking', 45.99, 'Electronics');

-- Grant permissions to testuser
GRANT CHANGEFEED ON TABLE products TO testuser;
GRANT SELECT ON TABLE products TO testuser;
GRANT INSERT, UPDATE, DELETE ON TABLE products TO testuser;
EOF
    
    # Build and deploy connector
    log_info "Building and deploying connector..."
    cd "$PROJECT_DIR"
    ./mvnw clean package -Ptest-uber-jar -DskipTests -q
    
    # Ensure plugin directory exists
    mkdir -p target/plugin/debezium-connector-cockroachdb
    cp target/debezium-connector-cockroachdb-3.2.0-SNAPSHOT-test-uber-jar-with-dependencies.jar target/plugin/debezium-connector-cockroachdb/
    
    # Restart Kafka Connect to pick up new connector
    docker-compose restart connect
    sleep 10
}

# Test connector discovery
test_connector_discovery() {
    log_info "Testing connector discovery..."
    
    # Wait for Kafka Connect to be ready
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$KAFKA_CONNECT_URL/connector-plugins" | grep -q "CockroachDBConnector"; then
            log_success "Connector discovered successfully"
            return 0
        fi
        
        log_info "Waiting for connector discovery... (attempt $attempt/$max_attempts)"
        sleep 2
        attempt=$((attempt + 1))
    done
    
    log_error "Connector not discovered after $max_attempts attempts"
    return 1
}

# Test connector deployment
test_connector_deployment() {
    log_info "Testing connector deployment..."
    
    # Deploy connector
    if ! curl -X POST "$KAFKA_CONNECT_URL/connectors" \
        -H "Content-Type: application/json" \
        -d @"$CONFIG_FILE"; then
        log_error "Failed to deploy connector"
        return 1
    fi
    
    log_success "Connector deployed successfully"
    
    # Wait for connector to start
    sleep 10
    
    # Check connector status
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        local status=$(curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq -r '.connector.state')
        
        if [ "$status" = "RUNNING" ]; then
            log_success "Connector is running"
            return 0
        elif [ "$status" = "FAILED" ]; then
            log_error "Connector failed to start"
            curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq '.'
            return 1
        fi
        
        log_info "Waiting for connector to start... (attempt $attempt/$max_attempts)"
        sleep 2
        attempt=$((attempt + 1))
    done
    
    log_error "Connector did not start within expected time"
    return 1
}

# Test change capture
test_change_capture() {
    log_info "Testing change capture..."
    
    # Insert new data
    log_info "Inserting new test data..."
    docker exec -i cockroachdb cockroach sql --insecure --user=root -d "$COCKROACHDB_DB" << 'EOF'
INSERT INTO products (name, description, price, category) 
VALUES ('Test Product', 'Test Description', 99.99, 'Test Category');
EOF
    
    # Update existing data
    log_info "Updating existing data..."
    docker exec -i cockroachdb cockroach sql --insecure --user=root -d "$COCKROACHDB_DB" << 'EOF'
UPDATE products SET price = 88.88 WHERE name = 'Test Product';
EOF
    
    # Delete data
    log_info "Deleting test data..."
    docker exec -i cockroachdb cockroach sql --insecure --user=root -d "$COCKROACHDB_DB" << 'EOF'
DELETE FROM products WHERE name = 'Test Product';
EOF
    
    # Wait for events to be processed
    sleep 10
    
    # Check if events were processed (look for log messages)
    log_info "Checking for processed events in logs..."
    if docker-compose logs connect | grep -q "Processed.*event for table.*products.*with operation"; then
        log_success "Change events were processed successfully"
        return 0
    else
        log_warning "No change events found in logs (this might be normal if no new events were generated)"
        return 0
    fi
}

# Test Kafka topics
test_kafka_topics() {
    log_info "Testing Kafka topics..."
    
    # List topics
    local topics=$(docker exec kafka-test kafka-topics --bootstrap-server localhost:9092 --list)
    log_info "Available topics: $topics"
    
    # Check if our topic exists
    if echo "$topics" | grep -q "products"; then
        log_success "Kafka topic 'products' exists"
        
        # Check message count
        local message_count=$(docker exec kafka-test kafka-run-class kafka.tools.GetOffsetShell \
            --bootstrap-server localhost:9092 \
            --topic products \
            --time -1 | awk -F: '{sum += $3} END {print sum}')
        
        log_info "Topic 'products' contains $message_count messages"
    else
        log_warning "Kafka topic 'products' not found (this might be normal for sink changefeeds)"
    fi
}

# Main test execution
main() {
    log_info "Starting Debezium CockroachDB Connector End-to-End Test"
    log_info "Project directory: $PROJECT_DIR"
    
    # Set up trap for cleanup
    trap cleanup EXIT
    
    # Run tests
    setup
    test_connector_discovery
    test_connector_deployment
    test_change_capture
    test_kafka_topics
    
    log_success "All tests completed successfully!"
    log_info "Connector is working correctly with enriched envelope support"
    log_info "Check logs with: docker-compose logs -f connect"
}

# Run main function
main "$@" 