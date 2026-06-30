# InstaShard

A PostgreSQL sharding proxy written in Elixir, inspired by Instagram's early sharding architecture. It implements 4096 logical shards backed by physical database clusters, using a Snowflake-like distributed ID scheme that embeds shard information directly into every generated ID — enabling stateless query routing without any external coordination.

---

## Background: Instagram's Sharding Design

When Instagram needed to scale beyond a single PostgreSQL instance, they chose a pragmatic approach: shard at the application layer using logical schemas, not physical tables. The key insight was to **embed shard identity into every ID**, so that any piece of data can be routed to the correct database node just by inspecting its ID — no lookup table required, no central coordinator needed.

The core ideas, as described in their [2012 engineering blog post](https://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram):

1. **4096 logical shards** — each is a PostgreSQL schema (`shard_0000` to `shard_4095`), not a separate database. This number was chosen as a fixed upper bound, so the mapping from ID → shard never changes, even as you add or redistribute physical databases.

2. **Snowflake-style IDs** — each ID is a 64-bit integer with shard info baked in. Any client that knows the ID structure can determine the shard immediately, with no round trips.

3. **Physical cluster indirection** — logical shards are mapped to physical databases by the proxy. Moving data from one physical node to another is a configuration change, not a schema migration.

4. **Transparent wire-protocol proxy** — application code talks standard PostgreSQL. The proxy intercepts the connection, inspects the SQL, extracts the target shard, and forwards the query to the correct physical cluster. All traffic flows through the proxy; clients never connect directly to a physical database.

---

## ID Structure

Every ID generated in this system is a 64-bit signed integer with three packed fields:

```
 63                23 22        10 9         0
 ┌─────────────────────┬───────────┬──────────┐
 │  41-bit timestamp   │ 13-bit    │ 10-bit   │
 │  (ms since epoch)   │ shard ID  │ sequence │
 └─────────────────────┴───────────┴──────────┘
```

| Field     | Bits | Range     | Description                                             |
| --------- | ---- | --------- | ------------------------------------------------------- |
| Timestamp | 41   | ~69 years | Milliseconds since 2026-01-01 00:00:00 UTC              |
| Shard ID  | 13   | 0 – 8191  | Logical shard number, matches the schema (`shard_XXXX`). Currently 4096 shards in use (0–4095), capacity reserved up to 8191. |
| Sequence  | 10   | 0 – 1023  | Per-shard sequence, supports 1024 IDs/ms per shard      |

**Extraction** — any client can recover the shard from an ID with two operations:

```
shard_id = (id >> 10) & 0x1FFF
```

No lookup, no network call.

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
│  Proxy.Listener          accepts TCP connections            │
│  Proxy.SessionSupervisor DynamicSupervisor for sessions     │
│  Proxy.ClientSession     one GenServer per client conn      │
│  Proxy.ShardRouter       extracts shard from SQL            │
│  Proxy.PgProtocol        wire protocol helpers              │
│                                                             │
│  Backend.Pool            ETS connection pool (lock-free)    │
│  Backend.Manager         connection lifecycle + replenish   │
│  Backend.Connection      TCP connect + MD5/SCRAM-SHA-256    │
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

---

## Components

**`Proxy.Listener`** — TCP server on port 5400. Accepts connections and hands each socket to a supervised `ClientSession`.

**`Proxy.ClientSession`** — One GenServer per client connection. Drives the session state machine (handshake → idle ↔ transaction) and handles both Simple Query and Extended Protocol.

**`Proxy.ShardRouter`** — Extracts the target shard name from SQL. Strips string literals first to avoid false matches, then scans `FROM`/`JOIN`/`INTO`/`UPDATE`/`TABLE` clauses for `schema.table` qualified identifiers matching `@shard_pattern`.

**`Proxy.PgProtocol`** — Wire protocol helpers: startup param parsing, ReadyForQuery status extraction, Parse message SQL extraction.

**`Backend.Pool`** — ETS-backed connection pool. Checkout and checkin are lock-free: `select` finds a candidate entry, `ets:take` atomically removes it. The Manager GenServer is off the hot path entirely.

**`Backend.Manager`** — Owns the shard→physical-db mapping. Initializes the pool at startup and replenishes connections on demand. Not involved in query routing.

**`Backend.Connection`** — Establishes a raw TCP connection to PostgreSQL and performs the full authentication handshake, supporting both MD5 and SCRAM-SHA-256.

---

## Query Routing

### Simple Query

```
Client:  Q("SELECT * FROM shard_0000.users WHERE id = $1")
Proxy:   extract_shard → "shard_0000"
         Pool.checkout("shard_0000") → socket
         forward packet to backend
         forward response to client
         Pool.checkin("shard_0000", socket)
```

### Extended Protocol

```
Client:  P("SELECT * FROM shard_0000.users WHERE id = $1")
Proxy:   extract SQL from Parse message
         extract_shard → "shard_0000"
         Pool.checkout → ext_socket
         forward ParseComplete to client

Client:  B(params) → E → S
Proxy:   forward Bind/Execute on ext_socket
         forward responses to client
         on Sync: forward ReadyForQuery, Pool.checkin
```

### Transaction (Lazy Checkout)

```
Client:  BEGIN
Proxy:   fake CommandComplete("BEGIN") + ReadyForQuery('T')
         buffer [BEGIN packet]

Client:  SET search_path = ...        ← no shard yet
Proxy:   fake ReadyForQuery('T')
         buffer [BEGIN, SET packets]

Client:  INSERT INTO shard_0000.users ...
Proxy:   extract_shard → "shard_0000"
         Pool.checkout → tx_socket
         flush buffer to backend (drain + discard responses)
         forward INSERT response to client
         tx_socket locked for this session

Client:  COMMIT
Proxy:   forward on tx_socket
         on ReadyForQuery('I'): Pool.checkin, clear tx state
```

The shard is unknown at `BEGIN` time — checkout is deferred until the first shard-bearing statement. Buffered packets (BEGIN, SET, SAVEPOINT) are replayed to the backend before the first real query, then responses are drained and discarded since the client already received fake responses for them.

---

## Connection Pool

The pool uses a single ETS `ordered_set` table with key `{shard_name, ref}`:

```
checkout:
  select → find a ref for this shard
  ets:take({shard, ref}) → atomic remove
  if taken by another process: retry

checkin:
  ets:insert({{shard, make_ref()}, socket})
```

`ets:take/2` is guaranteed atomic and isolated (OTP 18+). Multiple processes can checkout concurrently with no serialization bottleneck — the ETS bucket-level locking is the only contention point.

The Manager GenServer handles only the cold path: initializing connections at startup and replenishing the pool when `replenish/1` is called after a checkout.

---

## Shard Pattern

The shard name pattern is defined as a module attribute in `ShardRouter`:

```elixir
@shard_pattern ~r/\Ashard_\d{4}\z/
```

Full-string anchoring (`\A...\z`) prevents partial matches. This will become a per-logical-db configuration once the logical→physical DB mapping layer is introduced.

---

## Project Structure

```
lib/instashard/
├── application.ex
├── backend/
│   ├── connection.ex   # TCP connect + MD5/SCRAM-SHA-256 auth
│   ├── pool.ex         # ETS connection pool, lock-free checkout/checkin
│   └── manager.ex      # Shard mapping + connection replenishment
└── proxy/
    ├── listener.ex          # TCP accept loop
    ├── session_supervisor.ex # DynamicSupervisor for client sessions
    ├── client_session.ex    # Per-connection GenServer, session state machine
    ├── shard_router.ex      # SQL → shard extraction
    └── pg_protocol.ex       # Wire protocol helpers
```

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

| Instance | Port |
| -------- | ---- |
| db0      | 5430 |
| db1      | 5431 |

### 2. Initialize the schemas

```bash
psql -h 127.0.0.1 -p 5430 -U postgres -d my_cluster -f db/distributed-pg.sql
psql -h 127.0.0.1 -p 5431 -U postgres -d my_cluster -f db/distributed-pg.sql
```

### 3. Start the proxy

```bash
mix deps.get
iex -S mix phx.server
```

### 4. Connect through the proxy

```bash
psql -h 127.0.0.1 -p 5400 -U postgres -d my_cluster
```

```sql
-- Simple query
SELECT * FROM shard_0000.users;

-- Transaction
BEGIN;
INSERT INTO shard_0000.users (username, email) VALUES ('alice', 'alice@example.com');
COMMIT;

-- Extended protocol (psql 17+, no semicolon)
SELECT * FROM shard_0000.users WHERE id = $1
\bind 1
\g
```

---

## Roadmap

| Feature                             | Status      | Notes                                        |
| ----------------------------------- | ----------- | -------------------------------------------- |
| PostgreSQL wire protocol proxy      | Done        | Simple Query + Extended Protocol             |
| MD5 / SCRAM-SHA-256 authentication  | Done        | Full handshake per backend connection        |
| ETS lock-free connection pool       | Done        | ets:take atomic checkout, no GenServer bottleneck |
| Per-shard transactions              | Done        | Lazy checkout, buffer flush, fake responses  |
| Snowflake ID generation in PG       | Done        | Per-shard sequence, 1024 IDs/ms              |
| Configurable shard pattern          | Planned     | Per logical-db config                        |
| Logical→physical DB mapping layer   | Planned     | Decouple shard pattern from routing config   |
| Prepared statement rewrite          | Planned     | Rewrite named statements to anonymous to handle pool socket switching |
| Extended protocol transactions      | Done        | Parse/Bind/Execute/Sync inside BEGIN..COMMIT |
| Live shard migration                | Planned     | Logical replication + atomic cutover         |
| Mnesia-backed distributed mapping   | Planned     | Multi-node proxy cluster                     |

---

## Design Tradeoffs

**Why 8192 shard capacity?**
The logical shard count is fixed permanently — it's baked into the ID bit layout. The 13-bit shard field gives a ceiling of 8192 physical nodes (practically unreachable). Currently 4096 schemas (0–4095) are provisioned, leaving the upper half as headroom. Either way, you start with 2 physical nodes and grow without touching any existing data or IDs.

**Why embed shard in the ID rather than hashing the key?**
Embedding shard ID makes routing O(1) bit arithmetic with zero external state. A hash is also O(1), but consistent hashing across a changing cluster size still requires a routing table. With a fixed shard count and explicit shard-to-cluster config, the contract is clearer and migration is explicit and auditable.

**Why a proxy instead of a client-side sharding library?**
PostgreSQL connections are OS processes — each client connection forks a `postgres` backend process. At scale, letting application instances open direct connections exhausts the connection limit fast. A proxy pools connections to the physical databases regardless of how many clients connect.

Keeping all connections inside the proxy is also what makes zero-downtime shard migration possible. The proxy can hold connections, monitor replication lag, and atomically update the shard mapping. None of that is achievable if clients route themselves.

**Why Elixir?**
The proxy is primarily I/O concurrency: accept connections, forward bytes, bridge responses. Elixir's lightweight processes and OTP supervision make it straightforward to handle tens of thousands of concurrent connections with per-connection isolated state and automatic crash recovery.

---

## References

- [Sharding & IDs at Instagram (2012)](https://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram)
- [Twitter Snowflake](https://github.com/twitter-archive/snowflake)
- [PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [PostgreSQL Schemas](https://www.postgresql.org/docs/current/ddl-schemas.html)
