#!/bin/bash

set -e

# Configuration variables with defaults
# These can be overridden by environment variables
VERSION=${VERSION:-"3.2.0-SNAPSHOT"}
CONNECTOR_NAME=${CONNECTOR_NAME:-"debezium-connector-cockroachdb"}
JAR_NAME=${JAR_NAME:-"debezium-connector-cockroachdb-${VERSION}-test-uber-jar-with-dependencies.jar"}
KAFKA_CONNECT_PORT=${KAFKA_CONNECT_PORT:-"8083"}
COCKROACHDB_PORT=${COCKROACHDB_PORT:-"26257"}
KAFKA_PORT=${KAFKA_PORT:-"9092"}
ZOOKEEPER_PORT=${ZOOKEEPER_PORT:-"2181"}
MAX_DISCOVERY_ATTEMPTS=${MAX_DISCOVERY_ATTEMPTS:-"10"}
DISCOVERY_RETRY_DELAY=${DISCOVERY_RETRY_DELAY:-"5"}
CONNECT_START_TIMEOUT=${CONNECT_START_TIMEOUT:-"15"}
CONNECT_RETRY_DELAY=${CONNECT_RETRY_DELAY:-"10"}
CLEANUP_ON_START=${CLEANUP_ON_START:-"false"}
CLEANUP_ON_EXIT=${CLEANUP_ON_EXIT:-"false"}

echo "=========================================="
echo "  Testing Kafka Connect Connector Discovery"
echo "=========================================="
echo "Version: $VERSION"
echo "Connector: $CONNECTOR_NAME"
echo "JAR: $JAR_NAME"
echo "Kafka Connect Port: $KAFKA_CONNECT_PORT"
echo "Max Discovery Attempts: $MAX_DISCOVERY_ATTEMPTS"
echo "Cleanup on Start: $CLEANUP_ON_START"
echo "Cleanup on Exit: $CLEANUP_ON_EXIT"
echo

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

# Function to cleanup containers and networks
cleanup_containers() {
    print_status "Cleaning up existing containers and networks..."
    
    # Stop and remove containers
    docker-compose down -v 2>/dev/null || true
    
    # Remove any remaining containers with our project name
    docker ps -a --filter "label=com.docker.compose.project=debezium-connector-cockroachdb" --format "{{.Names}}" | xargs -r docker rm -f 2>/dev/null || true
    
    # Remove any containers with our specific names (in case they exist outside docker-compose)
    for container in zookeeper cockroachdb kafka-test connect; do
        docker rm -f "$container" 2>/dev/null || true
    done
    
    # Remove our custom network
    docker network rm debezium-test 2>/dev/null || true
    
    print_success "Cleanup completed"
}

# Function to cleanup on script exit
cleanup_on_exit() {
    local exit_code=$?
    print_status "Script exiting with code $exit_code"
    
    if [ "$CLEANUP_ON_EXIT" = "true" ]; then
        cleanup_containers
    fi
    
    exit $exit_code
}

# Set up trap for cleanup on script exit
trap cleanup_on_exit EXIT

# Check if cleanup is requested
if [ "$1" = "--cleanup" ] || [ "$CLEANUP_ON_START" = "true" ]; then
    cleanup_containers
fi

# Check for cleanup parameter from test runner
if [ "$CLEANUP_ON_START" = "true" ]; then
    print_status "Cleanup requested - cleaning up existing containers..."
    cleanup_containers
fi

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

# Step 1: Prepare plugin directory
print_status "Step 1: Preparing plugin directory..."
rm -rf target/plugin
mkdir -p target/plugin
cp plugin/debezium-connector-cockroachdb/*uber-jar-with-dependencies.jar target/plugin/
print_success "Plugin directory prepared"

# Step 2: Start services
print_status "Step 2: Starting Docker Compose services..."
docker-compose down -v 2>/dev/null || true
docker-compose up -d zookeeper kafka cockroachdb
check_status "Docker services started" "Failed to start Docker services"
sleep 20

# Step 3: Start Kafka Connect
print_status "Step 3: Starting Kafka Connect..."
docker-compose up -d connect
check_status "Kafka Connect container started" "Failed to start Kafka Connect container"
sleep 30

# Step 4: Check if Kafka Connect is running
print_status "Step 4: Checking Kafka Connect status..."
connect_ready=false
for i in $(seq 1 $CONNECT_START_TIMEOUT); do
    if curl -s http://localhost:$KAFKA_CONNECT_PORT/ > /dev/null 2>&1; then
        print_success "Kafka Connect is running"
        connect_ready=true
        break
    else
        print_warning "Waiting for Kafka Connect... (attempt $i/$CONNECT_START_TIMEOUT)"
        sleep $CONNECT_RETRY_DELAY
    fi
done

if [ "$connect_ready" = false ]; then
    print_error "❌ Kafka Connect failed to start"
    docker-compose logs connect
    exit 1
fi

# Step 5: Check connector discovery
check_connector_discovery

print_success "✅ CONNECTOR DISCOVERY TEST PASSED!"
echo
echo "Next steps:"
echo "✅ Connector is available - you can now create a connector instance"
echo "   curl -X POST http://localhost:$KAFKA_CONNECT_PORT/connectors \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d @cockroachdb-source.json"

# Clean up
# print_status "🧹 Cleaning up..."
# curl -X DELETE http://localhost:$KAFKA_CONNECT_PORT/connectors/$CONNECTOR_INSTANCE_NAME 2>/dev/null || true
# curl -X DELETE http://localhost:$KAFKA_CONNECT_PORT/connectors/cockroachdb-no-privileges 2>/dev/null || true
# rm -f cockroachdb-sink-config.json cockroachdb-no-privileges-config.json
# print_success "✅ Cleanup completed" 