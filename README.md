# Fluree Server
Creating and updating ledgers must be done by a service that not only has the rights to update them, but is also
responsible for the current ledger state and persisting updates to storage.

While this can be done using just Fluree's db library (using, for example, IPFS for persistent storage), for
an environment that will have many clients, like an app with many users, it is typical to want dedicated server(s)
to handle consistent transaction ordering, communicating out ledger updates, redundancy, and other
"server" features that would be expected for a reliable, scalable, and fault-tolerant environment.

Fluree Server provides these features. It can be run stand-alone, in conjunction with Fluree's db library, or as a cluster
of servers providing scale and/or distributed consensus, or all of the above. You can start with a single server while
in development, then add more servers once ready to scale.

Fluree Server can be run as either a consensus-participating node processing transactions/updates, or as query-only server that scales out
query processing horizontally, and to the edge if desired. Fluree Server can have any number of consensus-participating
nodes that together reliably maintain ledger updates for any number of ledgers.

Note that Fluree's db library, which can be included directly into your application, can also process queries which
not only provides further scale, but can wrap a distributed database with your custom functionality at in-memory speeds.
Your app can benefit from timely, fast, and simple information lookups/queries, eliminating the need to make remote db calls.

### Designing a Fluree Server network
Fluree Server runs as either a consensus participating node + query server, or as a query-only server depending on
the startup configuration. While a single server can be run as both a consensus participating node and a query server,
it lacks fault-tolerance and redundancy (if using local storage, vs cloud/IPFS storage).

Three connected Fluree Servers running consensus is the recommended minimum for a production environment, which will
provide fault-tolerance and redundancy allowing any single server to fail with zero downtime. Three servers also
provide scale, as any of them can serve query results and transaction processing will be split amongst them
for different ledgers being maintained.

For additional query scale, you'd typically start adding one or more query-only servers that connect to the consensus
servers. Query servers are ephemeral, are great for containerization and
auto-scaling. They can also be deployed at the edge for performance. Query-only servers accumulate ledger index data
in-memory (up to your configured max memory) and garbage collect the least recently used data once the memory allocated
is full. The in-memory index data allows for fast query processing at the edge.

When using query-only servers, you'd typically have your clients/apps connect to them instead of directly to the consensus
servers. Transactions received by the query-only servers are forwarded to the consensus servers for processing, and
they utilize a CQRS pattern to communicate ledger updates back to the connected clients. For private ledgers, it
would be typical to put the consensus servers in a private network, and have the query-only servers in a public network
such that no clients could connect directly to the consensus servers.

For added redundancy, scale, fault-tolerance, or to accommodate a number of decentralized participants in a network you
can run more than 3 consensus servers. The failure formula `2f+1=n` applies, where `f` is the number of servers that can fail
with the network still being able to reach consensus, and `n` is the total number of servers in the network.
So, for example, if you wanted to allow for 2 failures while still maintaining consensus and uptime, you'd need 5 servers
(`2*2+1=5`). If you wanted to allow for 3 server failures, you'd need 7 servers (`2*3+1=7`). As you'll note, the number of
servers is always an odd number as >50% of the network must be "alive", and therefore while an even number of servers is allowed, it is not recommended.

## Usage

### Settings
Configuration is handled through environment variables, with defaults provided via `resources/config.edn` file.  See the [Fluree docs](https://docs.flur.ee/docs/running-fluree/configuration) for more information.

The most common environment variables to set would include:
- `FLUREE_HTTP_API_PORT` - the TCP port to serve the HTTP API on (default: 8090). Used primarily to server query requests.
- `FLUREE_CACHE_MAX_MB` - the maximum amount of memory to use for in-memory database index data in megabytes (default: 1000, or 1GB).
- `FLUREE_SERVERS` - Server(s) for a query server to connect to for retrieving state and receiving updates. If set, this server will run as an ephemeral query-only server, which enables horizontal (edge) scalability of query processing
  (it is not a consensus participating node). Provide a comma-separated list of servers in the
  [multi-address format](https://github.com/libp2p/specs/blob/master/addressing/README.md) which this server can connect to for state and update events.
  This would typically be the same servers as `FLUREE_CONSENSUS_SERVERS`, but with their respective configured external `FLUREE_HTTP_API_PORT`.
- `FLUREE_CONSENSUS_SERVERS` - Only needed for consensus servers (disregard for query-only servers). A comma-separated list of server(s) processing transactions and participating in consensus using the
  [multi-address format](https://github.com/libp2p/specs/blob/master/addressing/README.md). The TCP port specified in this list
  is *only* used for consensus-server communication and must be open between the respective listed servers, but can be closed to all other traffic.
  Example of a 3-server cluster specifying IP addresses: `FLUREE_SERVERS=/ip4/10.0.0.1/tcp/62071,/ip4/10.0.0.2/tcp/62071,/ip4/10.0.0.3/tcp/62071`.
- `FLUREE_CONSENSUS_THIS_SERVER` - Only needed for consensus servers (disregard for query-only servers).
  This identifies which of the above `FLUREE_CONSENSUS_SERVERS` is the current server. Using the 3 server cluster example above, each
  of the three servers will have a different value for FLUREE_THIS_SERVER, e.g. the first server would have `FLUREE_THIS_SERVER=/ip4/10.0.0.1/tcp/62071`


## Features
- Can run as either (a) a node participating in consensus + query server, or (b) an ephemeral in-memory query-only server (scale-out server) based on if `FLUREE_SERVERS` environment variable is configured.
- Maintains queue of transactions with guaranteed ordering across all servers
- Maintains consistent state for all ledgers managed in the cluster across all participating servers
- Distributes transaction processing across all servers in the cluster (on a per-ledger basis)
- Caching to quickly verify duplicate transactions are not processed twice
- If a server fails, it is auto-detected and the work of the server is redistributed to the remaining servers in the cluster
- If a server joins, work is redistributed to include the new server

## Development

### Dependencies

Run `make prepare` to compile the required dependencies to enable Clojure development.

### Git hooks

- Run `bb run git-hooks install`
    - This will set up a pre-commit git hook that checks your staged changes
      with cljfmt and clj-kondo.
