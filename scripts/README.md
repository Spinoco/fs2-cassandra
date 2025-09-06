# Testing Scripts

This directory contains scripts for managing Cassandra Docker containers during testing.

## Local Development

### Starting Cassandra

```bash
# Start default version (3.11) on default port (12000)  
./scripts/start-cassandra.sh

# Start specific version
./scripts/start-cassandra.sh 4.1

# Start specific version on custom port
./scripts/start-cassandra.sh 5.0 12001

# Note: The start script will fail if a container with the same name already exists
# Use the stop script first if you need to replace an existing container
```

### Running Tests

After starting Cassandra, run tests with the matching version:

```bash
# Test against the running Cassandra version
CASSANDRA_SPEC_VERSION=4.1 sbt test

# Run specific test
CASSANDRA_SPEC_VERSION=4.1 sbt "testOnly *CrudSpec"
```

### Stopping Cassandra

```bash
# Stop all Cassandra containers
./scripts/stop-cassandra.sh

# Stop specific container
./scripts/stop-cassandra.sh cassandra-4.1-12000
```
 
## Supported Versions

- `3.11` - Last of 3.x series (EOL but supported for compatibility)
- `4.0` - Stable LTS release
- `4.1` - Latest 4.x release  
- `5.0` - Current major release with new features

## CI/CD Integration

The GitHub Actions workflow uses the same scripts as local development:

1. **Start**: Uses `./scripts/start-cassandra.sh` with CI environment settings
2. **Test**: Runs the test suite against the started container
3. **Debug**: On failure, runs `./scripts/debug-containers.sh` for troubleshooting  
4. **Cleanup**: Uses `./scripts/stop-cassandra.sh` to remove containers (even on failure)

**Key behaviors:**
- Start script never stops existing containers (use stop script first if needed)
- Uses predictable container names: `cassandra-{version}-ci` 
- All scripts share the same nodetool-based readiness checking
