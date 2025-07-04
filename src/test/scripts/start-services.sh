#!/bin/bash

# Get the absolute path to the project root (three directories up from this script)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PLUGIN_PATH="$PROJECT_ROOT/target/plugin"

echo "Project root: $PROJECT_ROOT"
echo "Plugin path: $PLUGIN_PATH"

# Check if plugin directory exists and has content
if [ ! -d "$PLUGIN_PATH" ] || [ -z "$(ls -A "$PLUGIN_PATH" 2>/dev/null)" ]; then
    echo "❌ Plugin directory is empty or doesn't exist. Please build the connector first:"
    echo "   ./mvnw clean package -Passembly -DskipTests"
    echo "   unzip -o target/debezium-connector-cockroachdb-3.2.0-SNAPSHOT-plugin.zip -d target/plugin"
    exit 1
fi

echo "✅ Plugin directory found with content"

# Start services with the correct absolute path
PLUGIN_PATH="$PLUGIN_PATH" docker-compose up -d

echo "🚀 Services started successfully!"
echo "📊 Check service status: docker-compose ps"
echo "📋 View logs: docker-compose logs -f"