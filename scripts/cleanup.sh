#!/bin/bash

# Standalone cleanup script for CockroachDB connector test environment
# This script removes all containers, networks, and volumes related to the test environment

set -e

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

# Function to cleanup containers and networks
cleanup_containers() {
    print_status "Cleaning up CockroachDB connector test environment..."
    
    # Stop and remove containers using docker-compose
print_status "Stopping docker-compose services..."
docker-compose -f scripts/docker-compose.yml down -v 2>/dev/null || true
    
    # Remove any remaining containers with our project name
    print_status "Removing containers with project label..."
    docker ps -a --filter "label=com.docker.compose.project=debezium-connector-cockroachdb" --format "{{.Names}}" | xargs -r docker rm -f 2>/dev/null || true
    
    # Remove any containers with our specific names (in case they exist outside docker-compose)
    print_status "Removing specific containers..."
    for container in zookeeper cockroachdb kafka-test connect; do
        if docker ps -a --format "{{.Names}}" | grep -q "^${container}$"; then
            print_status "Removing container: $container"
            docker rm -f "$container" 2>/dev/null || true
        fi
    done
    
    # Remove our custom network
    print_status "Removing custom network..."
    docker network rm debezium-test 2>/dev/null || true
    
    # Remove any dangling volumes
    print_status "Removing dangling volumes..."
    docker volume prune -f 2>/dev/null || true
    
    # Remove any dangling networks
    print_status "Removing dangling networks..."
    docker network prune -f 2>/dev/null || true
    
    print_success "Cleanup completed successfully!"
}

# Function to show what will be cleaned up
show_cleanup_preview() {
    print_status "Preview of what will be cleaned up:"
    echo
    
    # Show containers that will be removed
    print_status "Containers to be removed:"
    docker ps -a --filter "label=com.docker.compose.project=debezium-connector-cockroachdb" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || print_warning "No project containers found"
    
    # Show specific containers
    for container in zookeeper cockroachdb kafka-test connect; do
        if docker ps -a --format "{{.Names}}" | grep -q "^${container}$"; then
            print_status "Container '$container' will be removed"
        fi
    done
    
    # Show networks
    print_status "Networks to be removed:"
    docker network ls --filter "name=debezium-test" --format "table {{.Name}}\t{{.Driver}}\t{{.Scope}}" 2>/dev/null || print_warning "No debezium-test network found"
    
    echo
}

# Function to print usage
print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  --preview, -p    Show what will be cleaned up without actually doing it"
    echo "  --force, -f      Force cleanup without confirmation"
    echo "  --help, -h       Show this help message"
    echo
    echo "Examples:"
    echo "  $0               # Interactive cleanup with confirmation"
    echo "  $0 --preview     # Show what will be cleaned up"
    echo "  $0 --force       # Clean up without confirmation"
    echo
}

# Parse command line arguments
PREVIEW_ONLY=false
FORCE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --preview|-p)
            PREVIEW_ONLY=true
            shift
            ;;
        --force|-f)
            FORCE=true
            shift
            ;;
        --help|-h)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "  CockroachDB Connector Test Environment Cleanup"
echo "=========================================="
echo

if [ "$PREVIEW_ONLY" = true ]; then
    show_cleanup_preview
    exit 0
fi

# Show preview first
show_cleanup_preview

# Ask for confirmation unless --force is used
if [ "$FORCE" = false ]; then
    echo
    read -p "Do you want to proceed with cleanup? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_warning "Cleanup cancelled by user"
        exit 0
    fi
fi

# Perform cleanup
cleanup_containers

echo
print_success "All test environment resources have been cleaned up!"
echo
echo "You can now run test scripts without container name conflicts." 