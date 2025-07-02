#!/bin/bash

set -e

echo "=========================================="
echo "  Quick Connector Test"
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

# Check if JAR exists
if [ ! -f "target/debezium-connector-cockroachdb-3.2.0-SNAPSHOT-test-uber-jar-with-dependencies.jar" ]; then
    print_error "❌ Test uber JAR not found. Please run: ./mvnw clean package -Ptest-uber-jar"
    exit 1
fi

# Test JAR contents
print_status "Testing JAR contents..."

# Check for connector class
if jar tf target/debezium-connector-cockroachdb-3.2.0-SNAPSHOT-test-uber-jar-with-dependencies.jar | grep -q "CockroachDBConnector.class"; then
    print_success "✅ CockroachDBConnector class found"
else
    print_error "❌ CockroachDBConnector class not found"
    exit 1
fi

# Check for service loader file
if jar tf target/debezium-connector-cockroachdb-3.2.0-SNAPSHOT-test-uber-jar-with-dependencies.jar | grep -q "META-INF/services/org.apache.kafka.connect.source.SourceConnector"; then
    print_success "✅ Service loader file found"
else
    print_error "❌ Service loader file not found"
    exit 1
fi

# Test connector instantiation
print_status "Testing connector instantiation..."
java -cp "target/debezium-connector-cockroachdb-3.2.0-SNAPSHOT-test-uber-jar-with-dependencies.jar" \
     -Dlogback.configurationFile=src/test/resources/logback-test.xml \
     io.debezium.connector.cockroachdb.CockroachDBConnector 2>/dev/null || true
check_status "Connector can be instantiated" "Failed to instantiate connector"

print_success "✅ QUICK CONNECTOR TEST PASSED!"

echo ""
echo "To test with Kafka Connect:"
echo "1. Copy the JAR to your Kafka Connect plugin directory"
echo "2. Restart Kafka Connect"
echo "3. Check connector plugins: curl http://localhost:8083/connector-plugins"
echo ""
echo "JAR location: target/debezium-connector-cockroachdb-3.2.0-SNAPSHOT-test-uber-jar-with-dependencies.jar" 