# Fluree Server

Fluree Server wraps the [fluree/db](https://github.com/fluree/db) library with
an HTTP API server and consensus capabilities. While fluree/db can be used
directly in applications for embedded database functionality, Fluree Server
provides the infrastructure needed for multi-client environments requiring
consistent transaction ordering, ledger state management, and distributed
consensus.

The server can run as either a consensus-participating node for transaction
processing or as a query-only node for horizontal scaling. Multiple servers can
form a cluster for fault tolerance and performance.

**Key Features:**
- **High Availability**: Automatic failover and work redistribution when
  servers join or leave
- **Guaranteed Ordering**: Consistent transaction processing across all nodes
- **Horizontal Scaling**: Add query-only nodes for read performance
- **Load Distribution**: Automatic workload balancing across consensus nodes
- **Duplicate Prevention**: Built-in transaction deduplication

## Quick Start

The quickest way to run Fluree Server with Docker:

```bash
docker run -p 58090:8090 -v `pwd`/data:/opt/fluree-server/data fluree/server
```

For comprehensive documentation, visit
[https://developers.flur.ee](https://developers.flur.ee).

The sections below cover custom configurations and building from source.

## Usage

### Settings
Configuration is handled through JSON-LD configuration files with samples (and
the default) located in the `resources/` directory. The server can be started
with different configuration files and profiles. 

**Note:** The Fluree server does not currently support environment variables for
configuration overrides. All configuration must be provided through JSON-LD
files or command-line arguments.

#### Configuration Files
Sample configuration files are provided in the `resources/` directory:
- `file-config.jsonld` - Default standalone server configuration with
  file-based storage
- `memory-config.jsonld` - Standalone server configuration with in-memory
  storage
- `config-raft.jsonld` - Raft consensus cluster configuration
- `config-raft-standalone.jsonld` - Single-node Raft configuration for
  development

#### Starting the Server
The server can be started in different ways:
- Default: Uses `file-config.jsonld` from resources
- With specific config file: `--config /path/to/config.jsonld`
- With specific resource: `--resource config-name.jsonld`
- With config string: `--string "{config json}"`
- With profile: `--profile dev` (applies profile-specific overrides)

#### Configuration Options
The configuration files use the Fluree system vocabulary. Common configuration
settings include:

**Storage Configuration:**
- `filePath` - Directory path for file-based storage (e.g.,
  "/opt/fluree-server/data" or "dev/data")
- `@type: "Storage"` - Defines storage configuration (omit filePath for memory
  storage)

**Connection Configuration:**
- `parallelism` - Number of parallel operations (default: 4)
- `cacheMaxMb` - Maximum memory for caching in MB (default: 1000)
- `commitStorage` - Reference to storage configuration for commits
- `indexStorage` - Reference to storage configuration for indexes
- `primaryPublisher` - Primary publisher configuration
- `secondaryPublishers` - Optional array of secondary publishers
- `defaults.indexing.reindexMinBytes` - Minimum bytes before reindexing
  (default: 1000000)
- `defaults.indexing.reindexMaxBytes` - Maximum bytes before reindexing
  (default: 1000000000)

**Consensus Configuration:**
- `consensusProtocol` - Either "standalone" or "raft"
- `maxPendingTxns` - Maximum pending transactions (standalone mode,
  default: 512)

**Raft-specific Configuration:**
- `raftLogHistory` - Number of log entries to keep (default: 10)
- `raftEntriesMax` - Maximum entries per batch (default: 200)
- `raftCatchUpRounds` - Catch-up rounds for lagging nodes (default: 10)
- `raftServers` - Array of server addresses in multi-address format (e.g.,
  "/ip4/127.0.0.1/tcp/62071")
- `raftThisServer` - This server's address from the raftServers list

**HTTP API Configuration:**
- `httpPort` - Port for HTTP API (default: 8090)
- `maxTxnWaitMs` - Maximum transaction wait time in milliseconds
  (default: 120000)
- `closedMode` - Enable closed mode (requires authentication)
- `rootIdentities` - Array of root identity DIDs for authentication

#### Example Configuration
Here's a minimal standalone server configuration:
```json
{
  "@context": {
    "@base": "https://ns.flur.ee/config/main/",
    "@vocab": "https://ns.flur.ee/system#"
  },
  "@id": "myServer",
  "@graph": [
    {
      "@id": "storage",
      "@type": "Storage",
      "filePath": "/data/fluree"
    },
    {
      "@id": "connection",
      "@type": "Connection",
      "cacheMaxMb": 500,
      "commitStorage": {"@id": "storage"},
      "indexStorage": {"@id": "storage"}
    },
    {
      "@id": "consensus",
      "@type": "Consensus",
      "consensusProtocol": "standalone",
      "connection": {"@id": "connection"}
    },
    {
      "@id": "http",
      "@type": "API",
      "httpPort": 8090
    }
  ]
}
```

#### Profiles
Configuration files can include profiles that override base settings. For
example, a "dev" profile might use less memory and local file paths:
```json
"profiles": {
  "dev": [
    {
      "@id": "storage",
      "filePath": "dev/data"
    },
    {
      "@id": "connection",
      "cacheMaxMb": 200
    }
  ]
}
```

### Network Architecture

**Server Types:**
- **Consensus nodes**: Process transactions and maintain ledger state. Minimum 3
  for production (allows 1 failure).
- **Query-only nodes**: Ephemeral servers for horizontal query scaling. Great
  for containers and edge deployment.

**Deployment Patterns:**
- **Small**: 3 consensus nodes (handles 1 failure)
- **Medium**: 3-5 consensus nodes + multiple query nodes
- **Large**: 5+ consensus nodes (formula: `2f+1=n` where `f` = allowed
  failures)

**Best Practices:**
- Route client traffic through query-only nodes
- Keep consensus nodes in private networks
- Query nodes forward transactions to consensus nodes using CQRS pattern
- Query nodes cache data in-memory with LRU eviction

## Development

### Dependencies

Run `make help` to see all available tasks.

### Building

- `make` or `make uberjar` - Build an executable server uberjar
- `make docker-build` - Build the server Docker container
- `make docker-push` - Build and publish the server Docker container

### Testing

- `make test` - Run tests
- `make benchmark` - Run performance benchmarks
- `make pending-tests` or `make pt` - Run pending tests

### Code linting

- `bb lint` - Run both cljfmt and clj-kondo on the entire codebase
- `bb fix` - Automatically fix formatting errors with cljfmt
- `make clj-kondo-lint` - Run clj-kondo separately
- `make cljfmt-check` - Check formatting with cljfmt
- `make cljfmt-fix` - Fix formatting errors with cljfmt

### Git hooks

- `bb git-hooks install` - Set up a pre-commit git hook that checks your staged
  changes with cljfmt and clj-kondo before allowing them to be committed