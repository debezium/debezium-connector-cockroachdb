#!/bin/bash

# Process configuration template and replace environment variables
# Usage: ./process-config-template.sh <template_file> <output_file>

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if required arguments are provided
if [ $# -lt 2 ]; then
    print_error "Usage: $0 <template_file> <output_file>"
    echo "Example: $0 cockroachdb-source-template.json cockroachdb-source.json"
    exit 1
fi

TEMPLATE_FILE="$1"
OUTPUT_FILE="$2"

# Check if template file exists
if [ ! -f "$TEMPLATE_FILE" ]; then
    print_error "Template file '$TEMPLATE_FILE' not found"
    exit 1
fi

print_info "Processing template: $TEMPLATE_FILE -> $OUTPUT_FILE"

# Create output directory if it doesn't exist
OUTPUT_DIR=$(dirname "$OUTPUT_FILE")
if [ ! -d "$OUTPUT_DIR" ]; then
    mkdir -p "$OUTPUT_DIR"
    print_info "Created output directory: $OUTPUT_DIR"
fi

# Process the template using sed-based substitution
print_info "Using sed-based variable substitution"

# Read template and replace variables
cp "$TEMPLATE_FILE" "$OUTPUT_FILE"

# Replace common environment variables
sed -i.bak \
    -e "s/\${COCKROACHDB_HOST:-cockroachdb}/${COCKROACHDB_HOST:-cockroachdb}/g" \
    -e "s/\${COCKROACHDB_PORT:-26257}/${COCKROACHDB_PORT:-26257}/g" \
    -e "s/\${DATABASE_USER:-testuser}/${DATABASE_USER:-testuser}/g" \
    -e "s/\${DATABASE_PASSWORD:-}/${DATABASE_PASSWORD:-}/g" \
    -e "s/\${DATABASE_NAME:-testdb}/${DATABASE_NAME:-testdb}/g" \
    -e "s/\${DATABASE_SERVER_NAME:-cockroachdb}/${DATABASE_SERVER_NAME:-cockroachdb}/g" \
    -e "s/\${SCHEMA_NAME:-public}/${SCHEMA_NAME:-public}/g" \
    -e "s/\${TABLE_NAME:-products}/${TABLE_NAME:-products}/g" \
    -e "s/\${KAFKA_HOST:-kafka-test}/${KAFKA_HOST:-kafka-test}/g" \
    -e "s/\${KAFKA_PORT:-9092}/${KAFKA_PORT:-9092}/g" \
    "$OUTPUT_FILE"

# Remove backup file
rm -f "$OUTPUT_FILE.bak"

print_success "Configuration file generated: $OUTPUT_FILE"

# Show the generated configuration (first few lines)
print_info "Generated configuration preview:"
head -10 "$OUTPUT_FILE" | sed 's/^/  /'

print_info "Environment variables used:"
echo "  COCKROACHDB_HOST: ${COCKROACHDB_HOST:-cockroachdb}"
echo "  COCKROACHDB_PORT: ${COCKROACHDB_PORT:-26257}"
echo "  DATABASE_USER: ${DATABASE_USER:-testuser}"
echo "  DATABASE_NAME: ${DATABASE_NAME:-testdb}"
echo "  SCHEMA_NAME: ${SCHEMA_NAME:-public}"
echo "  TABLE_NAME: ${TABLE_NAME:-products}"
echo "  KAFKA_HOST: ${KAFKA_HOST:-kafka-test}"
echo "  KAFKA_PORT: ${KAFKA_PORT:-9092}" 