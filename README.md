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

Or run from source (requires Java 11+ and Clojure):

```bash
# Run directly without building JAR (development mode)
make run

# Build and run in one command
make build-and-run

# Run with custom config
java -jar target/server-*.jar --config /path/to/config.jsonld
```

For comprehensive documentation, visit
[https://developers.flur.ee](https://developers.flur.ee).

The sections below cover custom configurations and building from source.

## API Examples

Here are examples using curl to demonstrate key Fluree Server features. These
examples assume the server is running on the default port 8090.

### Create a New Ledger

```bash
curl -X POST http://localhost:8090/fluree/create \
  -H "Content-Type: application/json" \
  -d '{
    "ledger": "example/ledger"
  }'
```

Expected output:
```json
{
  "ledger": "example/ledger",
  "t": 0,
  "tx-id": "089aeb0dd3a5cbef5cafd76aee57b71ff039ec35c9ce574daafa891c6c401381",
  "commit": {
    "address": "fluree:memory://...",
    "hash": "..."
  }
}
```

### Insert Data

After creating the ledger, you can add data using the `/insert` endpoint:

```bash
curl -X POST http://localhost:8090/fluree/insert \
  -H "Content-Type: application/json" \
  -H "fluree-ledger: example/ledger" \
  -d '{
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    },
    "@graph": [{
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
  "tx-id": "f4de14124f6121125b20dace04f6867326639b786d65eb542a3d02e1d6c1787e",
  "commit": {
    "address": "fluree:memory://...",
    "hash": "..."
  }
}
```

### Insert Additional Data

```bash
curl -X POST http://localhost:8090/fluree/insert \
  -H "Content-Type: application/json" \
  -H "fluree-ledger: example/ledger" \
  -d '{
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    },
    "@graph": [{
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
  "tx-id": "f4de14124f6121125b20dace04f6867326639b786d65eb542a3d02e1d6c1787e",
  "commit": {
    "address": "fluree:memory://...",
    "hash": "..."
  }
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
curl -X POST http://localhost:8090/fluree/query \
  -H "Content-Type: application/sparql-query" \
  -H "Accept: application/sparql-results+json" \
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

Expected output (SPARQL Results JSON format):
```json
{
  "head": {
    "vars": ["person", "name", "email"]
  },
  "results": {
    "bindings": [
      {
        "person": {
          "type": "uri",
          "value": "http://example.org/alice"
        },
        "name": {
          "type": "literal",
          "value": "Alice Johnson"
        },
        "email": {
          "type": "literal",
          "value": "alice@example.com"
        }
      },
      {
        "person": {
          "type": "uri",
          "value": "http://example.org/bob"
        },
        "name": {
          "type": "literal",
          "value": "Bob Smith"
        },
        "email": {
          "type": "literal",
          "value": "bob@example.com"
        }
      }
    ]
  }
}
```

### Insert More Data

The `/insert` endpoint adds new data to the ledger. If the subject already exists, the operation will merge the new properties with existing ones:

```bash
curl -X POST http://localhost:8090/fluree/insert \
  -H "Content-Type: application/json" \
  -H "fluree-ledger: example/ledger" \
  -d '{
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    },
    "@graph": [{
      "@id": "ex:charlie",
      "@type": "schema:Person",
      "schema:name": "Charlie Brown",
      "schema:email": "charlie@example.com"
    }]
  }'
```

Expected output:
```json
{
  "ledger": "example/ledger",
  "t": 3,
  "tx-id": "...",
  "commit": {
    "address": "fluree:memory://...",
    "hash": "..."
  }
}
```

### Update Data

The `/update` endpoint is the preferred way to modify existing data using `where`, `delete`, and `insert` clauses. This endpoint replaces the deprecated `/transact` endpoint:

```bash
curl -X POST http://localhost:8090/fluree/update \
  -H "Content-Type: application/json" \
  -H "fluree-ledger: example/ledger" \
  -d '{
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
  "t": 4,
  "tx-id": "...",
  "commit": {
    "address": "fluree:memory://...",
    "hash": "..."
  }
}
```

### Upsert Data

The `/upsert` endpoint performs an "update or insert" operation. If the subject exists, it updates the properties; if not, it creates a new subject:

```bash
curl -X POST http://localhost:8090/fluree/upsert \
  -H "Content-Type: application/json" \
  -H "fluree-ledger: example/ledger" \
  -d '{
    "@context": {
      "schema": "http://schema.org/",
      "ex": "http://example.org/"
    },
    "@graph": [{
      "@id": "ex:alice",
      "schema:age": 43
    }, {
      "@id": "ex:diana",
      "@type": "schema:Person",
      "schema:name": "Diana Prince",
      "schema:email": "diana@example.com"
    }]
  }'
```

Expected output:
```json
{
  "ledger": "example/ledger",
  "t": 5,
  "tx-id": "...",
  "commit": {
    "address": "fluree:memory://...",
    "hash": "..."
  }
}
```

### Insert with Turtle Format

The `/insert` endpoint also supports Turtle (TTL) format for RDF data:

```bash
curl -X POST http://localhost:8090/fluree/insert \
  -H "Content-Type: text/turtle" \
  -H "fluree-ledger: example/ledger" \
  -d '@prefix schema: <http://schema.org/> .
@prefix ex: <http://example.org/> .

ex:emily a schema:Person ;
    schema:name "Emily Davis" ;
    schema:email "emily@example.com" ;
    schema:age 28 .'
```

Expected output:
```json
{
  "ledger": "example/ledger",
  "t": 6,
  "tx-id": "...",
  "commit": {
    "address": "fluree:memory://...",
    "hash": "..."
  }
}
```

### Upsert with Turtle Format

The `/upsert` endpoint also accepts Turtle format:

```bash
curl -X POST http://localhost:8090/fluree/upsert \
  -H "Content-Type: text/turtle" \
  -H "fluree-ledger: example/ledger" \
  -d '@prefix schema: <http://schema.org/> .
@prefix ex: <http://example.org/> .

ex:emily schema:jobTitle "Senior Engineer" .
ex:frank a schema:Person ;
    schema:name "Frank Wilson" ;
    schema:email "frank@example.com" .'
```

Expected output:
```json
{
  "ledger": "example/ledger",
  "t": 7,
  "tx-id": "...",
  "commit": {
    "address": "fluree:memory://...",
    "hash": "..."
  }
}
```

**Note:** The `/transact` endpoint is maintained for backward compatibility but `/update` should be preferred for all new implementations.

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

Get the history of changes for just `ex:alice` (optionally limit to a
property with e.g. `["ex:alice", "schema:email"]`):

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
      "ex": "http://example.org/",
      "f": "https://ns.flur.ee/ledger#"
    }
  }'
```

Expected output (showing only changes related to ex:alice):
```json
[
  {
    "f:t": 1,
    "f:assert": [
      {
        "@id": "ex:alice",
        "@type": "schema:Person",
        "schema:name": "Alice Johnson",
        "schema:email": "alice@example.com"
      }
    ],
    "f:retract": [],
    "f:commit": {
      "@id": "fluree:commit:sha256:...",
      "f:address": "fluree:memory://...",
      "f:alias": "example/ledger",
      "f:branch": "main",
      "f:previous": null,
      "f:time": 1704384000000,
      "f:v": 1,
      "f:data": {
        "@id": "fluree:db:sha256:...",
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
        "f:flakes": 3,
        "f:size": 384,
        "f:t": 1
      }
    }
  },
  {
    "f:t": 4,
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
    "f:commit": {
      "@id": "fluree:commit:sha256:...",
      "f:address": "fluree:memory://...",
      "f:alias": "example/ledger",
      "f:branch": "main",
      "f:previous": {"@id": "fluree:commit:sha256:..."},
      "f:time": 1704384120000,
      "f:v": 1,
      "f:data": {
        "@id": "fluree:db:sha256:...",
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
        "f:flakes": 31,
        "f:size": 4090,
        "f:t": 4
      }
    }
  },
  {
    "f:t": 5,
    "f:assert": [
      {
        "@id": "ex:alice",
        "schema:age": 43
      }
    ],
    "f:retract": [],
    "f:commit": {
      "@id": "fluree:commit:sha256:...",
      "f:address": "fluree:memory://...",
      "f:alias": "example/ledger",
      "f:branch": "main",
      "f:previous": {"@id": "fluree:commit:sha256:..."},
      "f:time": 1704384180000,
      "f:v": 1,
      "f:data": {
        "@id": "fluree:db:sha256:...",
        "f:address": "fluree:memory://...",
        "f:assert": [
          {
            "@id": "ex:alice",
            "schema:age": 43
          }
        ],
        "f:retract": [],
        "f:flakes": 32,
        "f:size": 4120,
        "f:t": 5
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

#### Dynamic Configuration with ConfigurationValue
Fluree Server supports dynamic configuration values that can be resolved from
Java system properties, environment variables, or default values. This allows
you to externalize configuration without modifying your JSON-LD files.

To use dynamic configuration, replace any configuration value with a
ConfigurationValue object:

```json
{
  "@type": "ConfigurationValue",  // Optional - included for clarity
  "javaProp": "fluree.http.port",
  "envVar": "FLUREE_HTTP_PORT", 
  "defaultVal": 8090
}
```

Note: The `@type` field is optional but recommended for clarity. At least one
of `javaProp`, `envVar`, or `defaultVal` must be present.

**Resolution Priority:**
1. Java system property (highest priority)
2. Environment variable 
3. Default value (lowest priority)

**Examples:**

Set HTTP port from environment variable with fallback:
```json
{
  "@id": "http",
  "@type": "API",
  "httpPort": {
    "@type": "ConfigurationValue",
    "envVar": "FLUREE_HTTP_PORT",
    "defaultVal": 8090
  }
}
```

Configure cache size from Java property:
```json
{
  "@id": "connection",
  "@type": "Connection",
  "cacheMaxMb": {
    "@type": "ConfigurationValue",
    "javaProp": "fluree.cache.maxMb",
    "envVar": "FLUREE_CACHE_MB",
    "defaultVal": 1000
  }
}
```

Configure file storage path:
```json
{
  "@id": "storage",
  "@type": "Storage",
  "filePath": {
    "@type": "ConfigurationValue",
    "javaProp": "fluree.data.dir",
    "envVar": "FLUREE_DATA_DIR",
    "defaultVal": "/opt/fluree-server/data"
  }
}
```

**Usage:**

With environment variables:
```bash
export FLUREE_HTTP_PORT=9090
export FLUREE_CACHE_MB=2000
java -jar target/fluree-server.jar
```

With Java system properties:
```bash
java -Dfluree.http.port=9090 -Dfluree.cache.maxMb=2000 \
     -jar target/fluree-server.jar
```

**Note:** ConfigurationValue can be used for any configuration property that
accepts a value. If no value can be resolved (no property set, no environment
variable, and no default), the server will fail to start with an error.

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
- With specific config file: `-c` or `--config /path/to/config.jsonld`
- With specific resource: `-r` or `--resource config-name.jsonld`
- With config string: `-s` or `--string "{config json}"`
- With profile: `-p` or `--profile dev` (applies profile-specific overrides)
- Show help: `-h` or `--help`

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

### Running the Server

#### Development Mode
- `make run` - Run the server in development mode with the `dev` profile
  - Uses reduced memory settings (200MB cache instead of 1000MB)
  - Stores data in `dev/data` directory
  - Reduces max pending transactions to 16
  - Equivalent to: `clojure -M:run-dev`

#### Production Mode
- `make run-prod` - Run the server in production mode
  - Uses default configuration from `file-config.jsonld`
  - Full memory allocation (1000MB cache)
  - Stores data in `/opt/fluree-server/data`
  - Equivalent to: `clojure -M -m fluree.server`

#### Running from JAR
After building with `make uberjar`:
```bash
java -jar target/fluree-server.jar
```

With options:
```bash
# With a specific profile (short or long form)
java -jar target/fluree-server.jar -p dev
java -jar target/fluree-server.jar --profile dev

# With a custom config file
java -jar target/fluree-server.jar -c /path/to/config.jsonld
java -jar target/fluree-server.jar --config /path/to/config.jsonld

# With inline configuration
java -jar target/fluree-server.jar -s '{"@context": {...}}'
java -jar target/fluree-server.jar --string '{"@context": {...}}'

# With a resource from the classpath
java -jar target/fluree-server.jar -r memory-config.jsonld
java -jar target/fluree-server.jar --resource memory-config.jsonld

# Show help
java -jar target/fluree-server.jar -h
java -jar target/fluree-server.jar --help
```

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