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

# Create a realistic test table with UUID primary key and comprehensive schema
echo "🔧 Creating realistic test table with UUID schema..."
cockroach sql --insecure --host=localhost:26257 --execute="
USE testdb;

-- Drop existing table if it exists
DROP TABLE IF EXISTS products CASCADE;

-- Create realistic products table with UUID primary key
CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku STRING UNIQUE NOT NULL,
    name STRING NOT NULL,
    description STRING,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    cost DECIMAL(10,2) CHECK (cost >= 0),
    category STRING NOT NULL,
    brand STRING,
    in_stock BOOLEAN DEFAULT true,
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    weight_grams DECIMAL(8,2),
    dimensions_cm STRING, -- Format: 'LxWxH'
    is_active BOOLEAN DEFAULT true,
    tags STRING[], -- Array of tags
    metadata JSONB, -- Flexible metadata storage
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp(),
    created_by STRING,
    updated_by STRING,
    
    -- Add indexes for better performance
    INDEX idx_products_sku (sku),
    INDEX idx_products_category (category),
    INDEX idx_products_brand (brand),
    INDEX idx_products_in_stock (in_stock),
    INDEX idx_products_created_at (created_at),
    INDEX idx_products_price_range (price) WHERE price > 0
);

-- Create a trigger to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS \$\$
BEGIN
    NEW.updated_at = current_timestamp();
    RETURN NEW;
END;
\$\$ language 'plpgsql';

CREATE TRIGGER update_products_updated_at 
    BEFORE UPDATE ON products 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Insert some realistic sample data
INSERT INTO products (sku, name, description, price, cost, category, brand, in_stock, stock_quantity, weight_grams, dimensions_cm, tags, metadata) VALUES
    ('MUG-001', 'Premium Ceramic Coffee Mug', 'High-quality ceramic coffee mug with ergonomic handle and microwave-safe design', 12.99, 8.50, 'Home & Kitchen', 'KitchenCraft', true, 150, 350.0, '10x8x12', ARRAY['coffee', 'ceramic', 'microwave-safe'], '{\"material\": \"ceramic\", \"capacity_ml\": 350, \"dishwasher_safe\": true}'),
    ('LAMP-002', 'LED Desk Lamp with Touch Control', 'Modern LED desk lamp featuring touch controls, adjustable brightness, and USB charging port', 45.99, 32.00, 'Home & Office', 'LightMax', true, 75, 1200.0, '25x15x45', ARRAY['led', 'touch-control', 'usb-charging'], '{\"wattage\": \"15W\", \"color_temp\": \"3000K-6500K\", \"usb_output\": \"5V/2A\"}'),
    ('MOUSE-003', 'Wireless Ergonomic Mouse', 'Ergonomic wireless mouse with precision tracking, long battery life, and customizable buttons', 29.99, 18.50, 'Electronics', 'TechPro', false, 0, 95.0, '12x7x4', ARRAY['wireless', 'ergonomic', 'gaming'], '{\"dpi\": \"12000\", \"battery_life_hours\": 72, \"connectivity\": \"2.4GHz\"}'),
    ('BOOK-004', 'Programming Fundamentals Guide', 'Comprehensive guide to programming fundamentals with practical examples and exercises', 24.99, 15.00, 'Books', 'TechBooks', true, 200, 450.0, '15x3x23', ARRAY['programming', 'education', 'beginner'], '{\"pages\": 350, \"language\": \"English\", \"format\": \"paperback\"}'),
    ('TOOL-005', 'Multi-function Screwdriver Set', 'Professional screwdriver set with 15 interchangeable bits and magnetic tip', 18.99, 12.00, 'Tools & Hardware', 'ProTools', true, 50, 280.0, '18x3x3', ARRAY['tools', 'screwdriver', 'magnetic'], '{\"bits_included\": 15, \"handle_material\": \"rubber\", \"case_included\": true}');

-- Grant permissions on the table
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE products TO debezium;
GRANT CHANGEFEED ON TABLE products TO debezium;
"

# Verify setup
echo "🔍 Verifying setup..."
cockroach sql --insecure --host=localhost:26257 --execute="
SHOW CLUSTER SETTING kv.rangefeed.enabled;
USE testdb;
SHOW GRANTS ON TABLE products;
SELECT COUNT(*) as product_count FROM products;
SELECT id, sku, name, price, category, in_stock FROM products LIMIT 3;
"

echo "✅ CockroachDB setup complete!"
echo
echo "Database: testdb"
echo "User: debezium"
echo "Password: (none - insecure mode)"
echo "Rangefeed: Enabled"
echo "CHANGEFEED privilege: Granted on table"
echo "Table: products (UUID primary key with realistic schema)"
echo "Sample data: 5 realistic products inserted"
echo
echo "Schema features:"
echo "- UUID primary key (CockroachDB recommended)"
echo "- Comprehensive product fields (SKU, price, category, etc.)"
echo "- Constraints and validation"
echo "- Indexes for performance"
echo "- Automatic timestamp updates"
echo "- JSONB metadata field for flexibility"
echo "- Array tags for categorization"
echo
echo "You can now create the Debezium connector with these credentials." 