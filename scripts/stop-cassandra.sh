#!/bin/bash

# Script to stop all Cassandra containers
# Usage: ./scripts/stop-cassandra.sh [container_name]

CONTAINER_NAME=${1:-""}

if [ -n "$CONTAINER_NAME" ]; then
    echo "Stopping specific container: $CONTAINER_NAME"
    if docker stop "$CONTAINER_NAME" 2>/dev/null; then
        echo "  Container stopped successfully"
    else
        echo "  Warning: Could not stop container (may not be running)"
    fi
    
    if docker rm "$CONTAINER_NAME" 2>/dev/null; then
        echo "  Container removed successfully"
    else
        echo "  Warning: Could not remove container (may not exist)"
    fi
else
    echo "Stopping all Cassandra containers..."
    
    # Find containers with 'cassandra' in the name
    CASSANDRA_CONTAINERS=$(docker ps -a --format "{{.Names}}" | grep -i cassandra || true)
    
    if [ -n "$CASSANDRA_CONTAINERS" ]; then
        echo "Found containers by name:"
        echo "$CASSANDRA_CONTAINERS" | while IFS= read -r container; do
            echo "  $container"
        done
        
        # Stop and remove containers by name
        echo "$CASSANDRA_CONTAINERS" | while IFS= read -r container; do
            echo "Stopping: $container"
            docker stop "$container" 2>/dev/null || echo "  Warning: Could not stop $container"
            docker rm "$container" 2>/dev/null || echo "  Warning: Could not remove $container"
        done
    fi
    
    # Also find containers based on cassandra image
    CASSANDRA_IMAGE_CONTAINERS=$(docker ps -a --filter "ancestor=cassandra" --format "{{.Names}}" || true)
    
    if [ -n "$CASSANDRA_IMAGE_CONTAINERS" ]; then
        echo "Found containers by image:"
        echo "$CASSANDRA_IMAGE_CONTAINERS" | while IFS= read -r container; do
            echo "  $container"
        done
        
        # Stop and remove containers by image
        echo "$CASSANDRA_IMAGE_CONTAINERS" | while IFS= read -r container; do
            if ! echo "$CASSANDRA_CONTAINERS" | grep -q "^$container$"; then
                echo "Stopping: $container (by image)"
                docker stop "$container" 2>/dev/null || echo "  Warning: Could not stop $container"
                docker rm "$container" 2>/dev/null || echo "  Warning: Could not remove $container"
            fi
        done
    fi
    
    if [ -z "$CASSANDRA_CONTAINERS" ] && [ -z "$CASSANDRA_IMAGE_CONTAINERS" ]; then
        echo "No Cassandra containers found"
    fi
fi

# Verify final state
echo ""
REMAINING_CONTAINERS=$(docker ps -a --format "{{.Names}}" | grep -i cassandra || true)
if [ -n "$REMAINING_CONTAINERS" ]; then
    echo "⚠️  Warning: Some Cassandra containers are still present:"
    echo "$REMAINING_CONTAINERS" | while IFS= read -r container; do
        STATUS=$(docker ps -a --filter "name=$container" --format "{{.Status}}")
        echo "  $container ($STATUS)"
    done
    echo ""
    echo "To force remove all:"
    echo "  docker ps -a --format \"{{.Names}}\" | grep -i cassandra | xargs -r docker rm -f"
else
    echo "✅ All Cassandra containers stopped and removed"
fi