#!/bin/bash

# Setup script for CockroachDB with proper permissions and configuration
# This script should be run after CockroachDB starts in Docker Compose

set -e

echo "🔧 Setting up CockroachDB for Debezium connector..."

# Wait for CockroachDB to be ready
echo "⏳ Waiting for CockroachDB to be ready..."
until cockroach sql --insecure --host=localhost:26257 -e "SELECT 1" > /dev/null 2>&1; do
    echo "Waiting for CockroachDB..."
    sleep 2
done

echo "✅ CockroachDB is ready!"

# Enable rangefeed (required for changefeeds)
echo "🔧 Enabling rangefeed..."
cockroach sql --insecure --host=localhost:26257 --execute="
SET CLUSTER SETTING kv.rangefeed.enabled = true;
"

# Create test database and user with proper permissions
echo "🔧 Creating test database and user..."
cockroach sql --insecure --host=localhost:26257 --execute="
CREATE DATABASE IF NOT EXISTS testdb;
CREATE USER IF NOT EXISTS debezium;
GRANT CONNECT ON DATABASE testdb TO debezium;
"

# Create a test table
echo "🔧 Creating test table..."
cockroach sql --insecure --host=localhost:26257 --execute="
USE testdb;
CREATE TABLE IF NOT EXISTS products (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    name STRING NOT NULL,
    description STRING NULL,
    price DECIMAL(10,2) NOT NULL,
    in_stock BOOL NULL DEFAULT true,
    category STRING NULL,
    created_at TIMESTAMP NULL DEFAULT current_timestamp():::TIMESTAMP,
    CONSTRAINT products_pkey PRIMARY KEY (id ASC)
);

-- Grant permissions on the table
GRANT SELECT ON TABLE products TO debezium;
GRANT CHANGEFEED ON TABLE products TO debezium;
"

# Verify setup
echo "🔍 Verifying setup..."
cockroach sql --insecure --host=localhost:26257 --execute="
SHOW CLUSTER SETTING kv.rangefeed.enabled;
USE testdb;
SHOW GRANTS ON TABLE products;
"

echo "✅ CockroachDB setup complete!"
echo
echo "Database: testdb"
echo "User: debezium"
echo "Password: (none - insecure mode)"
echo "Rangefeed: Enabled"
echo "CHANGEFEED privilege: Granted on table"
echo
echo "You can now create the Debezium connector with these credentials." 