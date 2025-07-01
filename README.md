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

Or run directly with Java (requires Java 11+):

```bash
# Download the latest JAR from releases or build from source (instructions below)
java -jar fluree-server.jar --config /path/to/config.jsonld
```

For comprehensive documentation, visit
[https://developers.flur.ee](https://developers.flur.ee).

The sections below cover custom configurations and building from source.

## API Examples

Here are examples using curl to demonstrate key Fluree Server features. These
examples assume the server is running on the default port 8090.

### Create a New Ledger with an Initial Commit

```bash
curl -X POST http://localhost:8090/fluree/create \
  -H "Content-Type: application/json" \
  -d '{
    "ledger": "example/ledger",
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    },
    "insert": [{
      "@id": "ex:alice",
      "@type": "schema:Person",
      "schema:name": "Alice Johnson",
      "schema:email": "alice@example.com"
    }]
  }'
```

Expected output:
```json
{
  "ledger": "example/ledger",
  "t": 1,
  "commit": "fluree:commit:sha256:...",
  "address": "fluree:memory://..."
}
```

### Transact Additional Data

```bash
curl -X POST http://localhost:8090/fluree/transact \
  -H "Content-Type: application/json" \
  -d '{
    "ledger": "example/ledger",
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    },
    "insert": [{
      "@id": "ex:bob",
      "@type": "schema:Person",
      "schema:name": "Bob Smith",
      "schema:email": "bob@example.com",
      "schema:knows": {"@id": "ex:alice"}
    }]
  }'
```

Expected output:
```json
{
  "ledger": "example/ledger",
  "t": 2,
  "commit": "fluree:commit:sha256:...",
  "address": "fluree:memory://..."
}
```

### Query Data

```bash
curl -X POST http://localhost:8090/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "from": "example/ledger",
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    },
    "select": {"?person": ["*"]},
    "where": {
      "@id": "?person",
      "@type": "schema:Person"
    }
  }'
```

Expected output:
```json
[
  {
    "@id": "ex:alice",
    "@type": "schema:Person",
    "schema:name": "Alice Johnson",
    "schema:email": "alice@example.com"
  },
  {
    "@id": "ex:bob",
    "@type": "schema:Person",
    "schema:name": "Bob Smith",
    "schema:email": "bob@example.com",
    "schema:knows": {"@id": "ex:alice"}
  }
]
```

### SPARQL Query

Fluree also supports SPARQL queries. Put the ledger name in the `FROM` clause:

```bash
curl -X POST http://localhost:8090/fluree/sparql \
  -H "Content-Type: application/sparql" \
  -d '
    PREFIX schema: <http://schema.org/>
    PREFIX ex: <http://example.org/>
    
    SELECT ?person ?name ?email
    FROM <example/ledger>
    WHERE {
      ?person a schema:Person ;
              schema:name ?name ;
              schema:email ?email .
    }
  '
```

### Update Data

```bash
curl -X POST http://localhost:8090/fluree/transact \
  -H "Content-Type: application/json" \
  -d '{
    "ledger": "example/ledger",
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    },
    "where": {
      "@id": "?person",
      "schema:email": "alice@example.com"
    },
    "delete": {
      "@id": "?person",
      "schema:email": "alice@example.com"
    },
    "insert": {
      "@id": "?person",
      "schema:email": "alice@newdomain.com"
    }
  }'
```

Expected output:
```json
{
  "ledger": "example/ledger",
  "t": 3,
  "commit": "fluree:commit:sha256:...",
  "address": "fluree:memory://..."
}
```

### Time Travel: Query at a Specific Time

Query the ledger state at transaction 2:

```bash
curl -X POST http://localhost:8090/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "from": "example/ledger",
    "t": 2,
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    },
    "select": {"?person": ["*"]},
    "where": {
      "@id": "?person",
      "@type": "schema:Person"
    }
  }'
```

Expected output (shows state before Alice's email was updated):
```json
[
  {
    "@id": "ex:alice",
    "@type": "schema:Person",
    "schema:name": "Alice Johnson",
    "schema:email": "alice@example.com"
  },
  {
    "@id": "ex:bob",
    "@type": "schema:Person",
    "schema:name": "Bob Smith",
    "schema:email": "bob@example.com",
    "schema:knows": {"@id": "ex:alice"}
  }
]
```

Query at a specific ISO-8601 timestamp:

```bash
curl -X POST http://localhost:8090/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "from": "example/ledger",
    "t": "2025-01-15T10:30:00Z",
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    },
    "select": {"?person": ["*"]},
    "where": {
      "@id": "?person",
      "@type": "schema:Person"
    }
  }'
```

Expected output (returns data as it existed at that timestamp):
```json
[
  {
    "@id": "ex:alice",
    "@type": "schema:Person",
    "schema:name": "Alice Johnson",
    "schema:email": "alice@newdomain.com"
  },
  {
    "@id": "ex:bob",
    "@type": "schema:Person",
    "schema:name": "Bob Smith",
    "schema:email": "bob@example.com",
    "schema:knows": {"@id": "ex:alice"}
  }
]
```

Query the ledger state from 5 minutes ago using ISO-8601 relative time format:

```bash
curl -X POST http://localhost:8090/fluree/query \
  -H "Content-Type: application/json" \
  -d '{
    "from": "example/ledger",
    "t": "PT5M",
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    },
    "select": {"?person": ["*"]},
    "where": {
      "@id": "?person",
      "@type": "schema:Person"
    }
  }'
```

Expected output (same format as above, showing state from 5 minutes ago):
```json
[
  {
    "@id": "ex:alice",
    "@type": "schema:Person",
    "schema:name": "Alice Johnson",
    "schema:email": "alice@newdomain.com"
  },
  {
    "@id": "ex:bob",
    "@type": "schema:Person",
    "schema:name": "Bob Smith",
    "schema:email": "bob@example.com",
    "schema:knows": {"@id": "ex:alice"}
  }
]
```


### History Query for a Specific Subject

Get the history of changes for just `ex:alice` (optionally limit to a property with e.g. `["ex:alice", "schema:email"]`):

```bash
curl -X POST http://localhost:8090/fluree/history \
  -H "Content-Type: application/json" \
  -d '{
    "from": "example/ledger",
    "commit-details": true,
    "history": ["ex:alice"],
    "t": {"from": 1},
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    }
  }'
```

Expected output (showing only changes related to ex:alice):
```json
[
  {
    "f:commit": {
      "id": "fluree:commit:sha256:...",
      "f:address": "fluree:memory://...",
      "f:alias": "example/ledger",
      "f:branch": "main",
      "f:previous": null,
      "f:time": 1704384000000,
      "f:v": 0,
      "f:data": {
        "f:address": "fluree:memory://...",
        "f:assert": [
          {
            "@id": "ex:alice",
            "@type": "schema:Person",
            "schema:name": "Alice Johnson",
            "schema:email": "alice@example.com"
          }
        ],
        "f:retract": [],
        "f:flakes": 4,
        "f:size": 256,
        "f:t": 1
      }
    }
  },
  {
    "f:commit": {
      "id": "fluree:commit:sha256:...",
      "f:address": "fluree:memory://...",
      "f:alias": "example/ledger",
      "f:branch": "main",
      "f:previous": {"id": "fluree:commit:sha256:..."},
      "f:time": 1704384120000,
      "f:v": 0,
      "f:data": {
        "f:address": "fluree:memory://...",
        "f:assert": [
          {
            "@id": "ex:alice",
            "schema:email": "alice@newdomain.com"
          }
        ],
        "f:retract": [
          {
            "@id": "ex:alice",
            "schema:email": "alice@example.com"
          }
        ],
        "f:flakes": 2,
        "f:size": 128,
        "f:t": 3
      }
    }
  }
]
```

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