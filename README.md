# InstaShard

A PostgreSQL sharding proxy written in Elixir, inspired by Instagram's early sharding architecture. It implements 4096 logical shards backed by physical database clusters, using a Snowflake-like distributed ID scheme that embeds shard information directly into every generated ID — enabling stateless query routing without any external coordination.

---

## Background: Instagram's Sharding Design

When Instagram needed to scale beyond a single PostgreSQL instance, they chose a pragmatic approach: shard at the application layer using logical schemas, not physical tables. The key insight was to **embed shard identity into every ID**, so that any piece of data can be routed to the correct database node just by inspecting its ID — no lookup table required, no central coordinator needed.

The core ideas, as described in their [2012 engineering blog post](https://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram):

1. **4096 logical shards** — each is a PostgreSQL schema (`shard_0000` to `shard_4095`), not a separate database. This number was chosen as a fixed upper bound, so the mapping from ID → shard never changes, even as you add or redistribute physical databases.

2. **Snowflake-style IDs** — each ID is a 64-bit integer with shard info baked in. Any client that knows the ID structure can determine the shard immediately, with no round trips.

3. **Physical cluster indirection** — logical shards are mapped to physical databases by the proxy. Moving data from one physical node to another is a configuration change, not a schema migration.

4. **Transparent wire-protocol proxy** — application code talks standard PostgreSQL. The proxy intercepts the connection, inspects the SQL, extracts the target shard, and forwards the query to the correct physical cluster. All traffic flows through the proxy; clients never connect directly to a physical database. This is what makes zero-downtime shard migration possible.

This project implements that design end-to-end in Elixir.

---

## ID Structure

Every ID generated in this system is a 64-bit signed integer with three packed fields:

```
 63                22 21        10 9         0
 ┌─────────────────────┬───────────┬──────────┐
 │  41-bit timestamp   │ 12-bit    │ 10-bit   │
 │  (ms since epoch)   │ shard ID  │ sequence │
 └─────────────────────┴───────────┴──────────┘
```

| Field     | Bits | Range     | Description                                             |
| --------- | ---- | --------- | ------------------------------------------------------- |
| Timestamp | 41   | ~69 years | Milliseconds since 2026-01-01 00:00:00 UTC              |
| Shard ID  | 12   | 0 – 4095  | Logical shard number, matches the schema (`shard_XXXX`) |
| Sequence  | 10   | 0 – 1023  | Per-shard sequence, supports 1024 IDs/ms per shard      |

**Generation** — PostgreSQL function installed in each shard schema:

```sql
CREATE OR REPLACE FUNCTION generate_snowflake_id(schema_name TEXT)
RETURNS BIGINT AS $$
DECLARE
  epoch        BIGINT := 1767225600000;  -- 2026-01-01T00:00:00Z in ms
  ts_delta     BIGINT;
  shard_id     BIGINT;
  seq          BIGINT;
BEGIN
  ts_delta := EXTRACT(EPOCH FROM clock_timestamp()) * 1000 - epoch;
  shard_id  := CAST(SUBSTRING(schema_name FROM 7) AS BIGINT);
  seq       := nextval(schema_name || '.id_sequence') % 1024;
  RETURN (ts_delta << 23) | (shard_id << 10) | seq;
END;
$$ LANGUAGE plpgsql;
```

**Extraction** — any client can recover the shard from an ID with two operations:

```
shard_id = (id >> 10) & 0xFFF
```

No lookup, no network call. This is the property that makes routing stateless.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Application                      │
│            (standard PostgreSQL client / psql)              │
└──────────────────────────┬──────────────────────────────────┘
                           │ PostgreSQL wire protocol
                           │ port 5400
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    InstaShard Proxy                         │
│                                                             │
│  1. Accept TCP connection                                   │
│  2. Handle PostgreSQL startup handshake                     │
│  3. For each query:                                         │
│     a. Extract shard name via regex (shard_\d{4})           │
│     b. Lookup physical cluster in shard mapping             │
│     c. Forward packet to backend socket                     │
│     d. Bridge response back to client                       │
│                                                             │
│  Backend.Manager (GenServer)                                │
│  ┌──────────────────────────────────────────┐              │
│  │  shard_mapping:                          │              │
│  │    "shard_0000" → :db0                   │              │
│  │    "shard_0001" → :db1                   │              │
│  │    ...          → ...                    │              │
│  │                                          │              │
│  │  sockets:                                │              │
│  │    db0 → persistent TCP socket           │              │
│  │    db1 → persistent TCP socket           │              │
│  └──────────────────────────────────────────┘              │
└──────────┬──────────────────────────┬───────────────────────┘
           │                          │
           ▼                          ▼
┌──────────────────┐        ┌──────────────────┐
│  db0  port 5430  │        │  db1  port 5431  │
│                  │        │                  │
│  shard_0000      │        │  shard_0001      │
│  shard_0002      │        │  shard_0003      │
│  ...             │        │  ...             │
│  (2048 schemas)  │        │  (2048 schemas)  │
└──────────────────┘        └──────────────────┘
```

### Components

**`ProxyListener`** — TCP server on port 5400. Implements the PostgreSQL wire protocol at the message level:

- Handles SSL negotiation (responds `N` — not supported)
- Handles the startup message (version 3.0), extracts database/user params, and sends `AuthenticationOK` + `ReadyForQuery`
- Handles `Q` (simple query) messages: extracts the target shard name from the SQL text, retrieves the pre-established backend socket from the manager, forwards the raw packet, and bridges the response stream back to the client
- Tracks transaction state (`I` / `T` / `E`) from `ReadyForQuery` (`Z`) markers

**`Backend.Manager`** — GenServer that owns all physical database connections:

- Establishes persistent TCP connections to each physical cluster at startup by performing a full PostgreSQL startup handshake
- Maintains a `shard_mapping` table: logical shard name → physical cluster key
- Maintains a `sockets` table: cluster key → live TCP socket
- Answers `get_socket(shard_name)` calls from the proxy in a single map lookup — no per-query connection overhead

**`distributed-pg.sql`** — initialization script that creates all 4096 schemas on each physical node, one sequence per schema, a `users` example table, and the `generate_snowflake_id` function.

---

## Query Routing Flow

```
1. Client sends:
   SELECT * FROM shard_0211.users WHERE id = 864691128455135235;

2. Proxy extracts "shard_0211" via ~r/shard_\d{4}/

3. Manager.get_socket("shard_0211")
   → shard_0211 mapped to :db0
   → return socket connected to 127.0.0.1:5430

4. Proxy forwards raw Q packet to db0

5. db0 executes query on schema shard_0211

6. Proxy bridges response packets back to client
   until ReadyForQuery (Z) is received
```

The ID structure means any layer can determine which shard owns a row using two operations — no lookup, no network call:

```python
shard_id = (id >> 10) & 0xFFF   # e.g. 864691128455135235 → 211
```

Clients compute this to construct shard-qualified SQL (`SELECT * FROM shard_0211.users ...`). What they don't need to know is which physical database shard_0211 lives on — that mapping is owned entirely by the proxy. When a shard is migrated to a new physical node, the SQL the client writes doesn't change at all.

---

## Shard → Cluster Mapping

The `Backend.Manager` holds the authoritative mapping. Currently two physical clusters split the 4096 logical shards:

```elixir
shard_mapping = %{
  "shard_0000" => :db0,
  "shard_0001" => :db1,
  # ... all 4096 entries in production, loaded from config
}
```

Moving a shard from one physical cluster to another — e.g., after migrating its data — is a one-line config change. The shard ID embedded in every existing row never changes.

### Scaling Path

| Physical clusters | Shards each | Expansion strategy                        |
| ----------------- | ----------- | ----------------------------------------- |
| 2                 | 2048        | Current dev setup                         |
| 4                 | 1024        | Add db2/db3, remap even/odd subsets       |
| N                 | 4096 / N    | `shard_id mod N` or explicit range config |
| 4096              | 1           | Maximum granularity — one schema per node |

Because logical shard IDs are fixed in the ID bit layout forever, resharding never invalidates existing data. Only the proxy mapping changes.

---

## Roadmap

### Live Shard Migration (Zero Downtime)

The proxy architecture is deliberately chosen to support live data migration without any application downtime. The planned migration flow is:

```
1. Operator triggers migration: shard_0042 → db0 to db2

2. Proxy enables PostgreSQL logical replication on db0,
   subscribing only to the target shard's schema.
   (Physical replication of one schema, not the whole instance.)

3. db2 catches up. The proxy monitors replication lag continuously.

4. When lag approaches zero, the proxy broadcasts a global pause
   to all connection processes handling shard_0042 traffic.
   Connections are held — clients see a brief stall, not an error.

5. Proxy waits for the final replication flush, then atomically
   updates the shard mapping: shard_0042 → db2.

6. Connections are released. All subsequent queries for shard_0042
   are routed to db2. Migration complete.
```

Because every client connection is owned by a proxy process (an Elixir lightweight process), the "hold all connections" step is a targeted broadcast across those processes — no kernel-level lock, no application change required.

### Distributed Proxy with Mnesia

As the proxy cluster grows beyond a single node, the shard mapping must be consistent across all proxy instances. The plan is to store the mapping in [Mnesia](https://www.erlang.org/doc/man/mnesia.html) — Erlang/OTP's built-in distributed database:

- **Read-heavy, write-rarely** — the mapping changes only during shard migrations, which are infrequent. Mnesia's in-memory table mode gives sub-microsecond reads on every node with no network hop.
- **Strong consistency** — Mnesia uses a synchronous transaction protocol. During a migration cutover, the mapping update is committed as a Mnesia transaction; all proxy nodes see the new mapping atomically before any connection is released.
- **No external coordinator** — the Mnesia cluster is the proxy cluster. Adding a new proxy node joins the Erlang cluster and immediately has a full replica of the mapping. No Redis, no ZooKeeper, no etcd.

```
┌──────────────────────────────────────────────────────────┐
│                  InstaShard Proxy Cluster                │
│                                                          │
│  node1 ──┐                                               │
│  node2 ──┼── Mnesia distributed table (shard_mapping)   │
│  node3 ──┘   strong-consistent, replicated, in-memory   │
│                                                          │
│  During migration cutover:                               │
│    Mnesia.transaction(fn -> update mapping end)          │
│    → committed on all nodes before connections resume    │
└──────────────────────────────────────────────────────────┘
```

### Planned Features Summary

| Feature                             | Status  | Notes                                  |
| ----------------------------------- | ------- | -------------------------------------- |
| PostgreSQL wire protocol proxy      | Done    | Simple query, startup, auth            |
| Snowflake ID generation in PG       | Done    | Per-shard sequence, 1024 IDs/ms        |
| Static shard→cluster mapping        | Done    | In-memory GenServer                    |
| Logical replication-based migration | Planned | Schema-level, not instance-level       |
| Connection hold + atomic cutover    | Planned | Broadcast to proxy processes           |
| Mnesia-backed distributed mapping   | Planned | Strong consistency, no external deps   |
| Multi-node proxy cluster            | Planned | Erlang clustering + Mnesia replication |
| Migration progress telemetry        | Planned | Replication lag monitoring via Phoenix |

---

## Getting Started

### Prerequisites

- Elixir 1.19+
- Docker + Docker Compose

### 1. Start the database clusters

```bash
cd db
docker compose up -d
```

Two PostgreSQL instances start:

| Instance | Port |
| -------- | ---- |
| db0      | 5430 |
| db1      | 5431 |

### 2. Initialize the schemas

Run `distributed-pg.sql` on both instances to create the 4096 schemas, sequences, and the ID generation function:

```bash
psql -h 127.0.0.1 -p 5430 -U postgres -d my_cluster -f db/distributed-pg.sql
psql -h 127.0.0.1 -p 5431 -U postgres -d my_cluster -f db/distributed-pg.sql
```

### 3. Start the proxy

```bash
mix deps.get
mix run --no-halt
```

The proxy listens on port 5400. The Phoenix web endpoint (telemetry/dashboard) runs on port 4000.

### 4. Connect through the proxy

```bash
psql -h 127.0.0.1 -p 5400 -U postgres -d my_cluster
```

Then issue shard-qualified queries:

```sql
-- Insert; ID generated automatically by the snowflake function
INSERT INTO shard_0042.users (username, email)
VALUES ('alice', 'alice@example.com')
RETURNING id;

-- The returned id encodes shard 42: (id >> 10) & 0xFFF = 42

SELECT * FROM shard_0042.users WHERE id = <returned_id>;
```

---

## Project Structure

```
instashard/
├── lib/instashard/
│   ├── application.ex          # OTP supervision tree
│   ├── proxy_listener.ex       # PostgreSQL wire protocol proxy (TCP server)
│   └── backend/
│       └── manager.ex          # Physical DB connections + shard mapping
├── db/
│   ├── docker-compose.yml      # Two-node PostgreSQL cluster
│   └── distributed-pg.sql      # 4096 schema init + Snowflake ID function
└── config/
    ├── config.exs
    ├── dev.exs
    └── runtime.exs
```

---

## Design Tradeoffs

**Why 4096 shards, not fewer?**
The logical shard count is fixed permanently — it's baked into the ID bit layout. Choosing 4096 gives a ceiling of 4096 physical nodes (practically unreachable), and lets you start with 2 nodes and grow without touching any data.

**Why embed shard in the ID rather than hashing the key?**
Embedding shard ID makes routing O(1) bit arithmetic with zero external state. A hash is also O(1), but consistent hashing across a changing cluster size still requires a routing table. With a fixed shard count and explicit shard-to-cluster config, the contract is clearer and migration is explicit and auditable.

**Why a proxy instead of a client-side sharding library?**
Two independent reasons, both pointing to the same answer:

First, PostgreSQL connections are OS processes — each client connection forks a `postgres` backend process. At any meaningful scale, letting application instances open direct connections to physical databases exhausts the connection limit fast. A connection-pooling proxy (analogous to PgBouncer) is standard practice regardless of sharding; InstaShard builds sharding routing into the same layer.

Second, keeping all connections inside the proxy is what makes zero-downtime shard migration possible. The proxy can hold connections, monitor replication lag, and atomically update the shard mapping in one coordinated step. None of that is achievable if clients route themselves directly to physical databases.

**Why Elixir?**
The proxy is primarily I/O concurrency: accept connections, forward bytes, bridge responses. Elixir's lightweight processes and OTP supervision make it straightforward to handle thousands of concurrent connections with per-connection isolated state and automatic crash recovery.

---

## References

- [Sharding & IDs at Instagram (2012)](https://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram)
- [Twitter Snowflake](https://github.com/twitter-archive/snowflake)
- [PostgreSQL Schemas](https://www.postgresql.org/docs/current/ddl-schemas.html)
