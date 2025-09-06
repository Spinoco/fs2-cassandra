#!/bin/bash

# Script to start Cassandra locally for testing
# Usage: ./scripts/start-cassandra.sh [version] [port]
# Example: ./scripts/start-cassandra.sh 4.1 12000

VERSION=${1:-"3.11"}
PORT=${2:-12000}

# Supported versions
SUPPORTED_VERSIONS=("3.11" "4.0" "4.1" "5.0")

# Check if version is supported
if [[ ! " ${SUPPORTED_VERSIONS[@]} " =~ " ${VERSION} " ]]; then
    echo "Error: Unsupported Cassandra version: $VERSION"
    echo "Supported versions: ${SUPPORTED_VERSIONS[*]}"
    exit 1
fi

echo "Starting Cassandra $VERSION on port $PORT..."

# Note: This script does not stop existing containers
# Use ./scripts/stop-cassandra.sh if you need to clean up first

# Start new Cassandra container
# Use CONTAINER_NAME if set, otherwise generate one
if [ -z "$CONTAINER_NAME" ]; then
    CONTAINER_NAME="cassandra-${VERSION}-${PORT}"
fi
echo "Starting container: $CONTAINER_NAME"

docker run -d \
  --name "$CONTAINER_NAME" \
  -p "$PORT:9042" \
  -e CASSANDRA_START_TIMEOUT=60 \
  -e CASSANDRA_START_RPC=true \
  "cassandra:$VERSION"

echo "Waiting for Cassandra to be ready..."

# Wait for Cassandra to be ready using nodetool
MAX_ATTEMPTS=30
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if docker exec "$CONTAINER_NAME" nodetool status 2>/dev/null | grep -q "^UN"; then
        echo "✅ Cassandra $VERSION is ready on port $PORT (node is UP and NORMAL)"
        echo "Container name: $CONTAINER_NAME"
        echo ""
        echo "Node status:"
        docker exec "$CONTAINER_NAME" nodetool status
        echo ""
        echo "To run tests against this version:"
        echo "  CASSANDRA_SPEC_VERSION=$VERSION sbt test"
        echo ""
        echo "To stop the container:"
        echo "  docker stop $CONTAINER_NAME && docker rm $CONTAINER_NAME"
        echo "  # or use: ./scripts/stop-cassandra.sh $CONTAINER_NAME"
        echo ""
        echo "To check container status:"
        echo "  docker exec $CONTAINER_NAME nodetool status"
        echo "  docker logs $CONTAINER_NAME"
        exit 0
    fi
    
    ATTEMPT=$((ATTEMPT + 1))
    echo "Waiting... (attempt $ATTEMPT/$MAX_ATTEMPTS)"
    sleep 3
done

echo "❌ Timeout waiting for Cassandra to start"
echo "Container logs:"
docker logs "$CONTAINER_NAME" --tail 50
echo "Nodetool status attempt:"
docker exec "$CONTAINER_NAME" nodetool status 2>/dev/null || echo "nodetool command failed"
exit 1