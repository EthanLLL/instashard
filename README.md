# InstaShard

A PostgreSQL sharding proxy in Elixir. Implements the PostgreSQL wire protocol, routes queries to physical databases based on shard schema names embedded in SQL, and supports live shard migration with zero client downtime.

Inspired by [Instagram's 2012 sharding design](https://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram).

---

## ID Structure

```
 63                23 22        10 9         0
 ┌─────────────────────┬───────────┬──────────┐
 │  41-bit timestamp   │ 13-bit    │ 10-bit   │
 │  (ms since epoch)   │ shard ID  │ sequence │
 └─────────────────────┴───────────┴──────────┘
```

- Epoch: 2026-01-01 00:00:00 UTC
- 4096 logical shards (0–4095), capacity up to 8191
- Extract shard from any ID: `(id >> 10) & 0x1FFF`

---

## Architecture

```
Client (psql / libpq / asyncpg)
  │ port 5400
  ▼
Proxy.Listener              TCP accept
Proxy.SessionSupervisor     DynamicSupervisor
Proxy.ClientSession         one GenServer per client
Proxy.ShardRouter           SQL → shard name
  │
Backend.Pool                ETS lock-free connection pool
Backend.Manager             pool replenishment
Backend.StmtCache           global prepared stmt rewrite (ETS)
Backend.ShardRoute          shard→db + gate status (Mnesia, single read)
Migration.Supervisor        Horde.DynamicSupervisor for workers
Migration.Registry          Horde.Registry, one entry per migrating shard
Migration.Worker            per-shard migration state machine (GenServer)
Backend.SchemaCloner        DDL copy via pg_catalog
Backend.Connection          TCP + MD5/SCRAM-SHA-256 auth
  │                              │
db0 :5430                   db1 :5431   db2 :5433
```

---

## Features

| Feature | Status |
|---|---|
| Simple Query protocol | Done |
| Extended Protocol (Parse/Bind/Execute/Sync) | Done |
| MD5 / SCRAM-SHA-256 auth | Done |
| ETS lock-free connection pool | Done |
| Per-shard transactions (lazy checkout) | Done |
| Prepared statement rewrite + pool reuse | Done |
| Shard→db routing + gate (Mnesia, single dirty_read) | Done |
| Live shard migration (concurrent, per-shard) | Done |
| Web dashboard (Phoenix Channel + React) | Done |
| Config file persistence (JSON) | Done |
| Migration coordinator HA (Horde) | Done |

---

## Prepared Statement Rewrite

Client statements are rewritten to internal names (`is_<16-char-hash>`) so the same SQL always maps to the same backend name across connections.

- `StmtCache`: global ETS, `sql_hash → {internal_name, sql}`
- Pool entry: `{socket, parse_count, stmt_set}` — which stmts are prepared per socket
- Session `stmt_map`: `client_name → {internal_name, shard}`
- Stmt already on socket → skip Parse, inject fake `ParseComplete` before Sync
- Bind without preceding Parse → checkout, re-parse on new socket if needed
- All send-only messages (P/B/E/D/C) collected into a single write; flushed with Sync/Query/Flush

---

## Live Shard Migration

Online migration between physical databases using PostgreSQL logical replication. Client sessions are transparently held and resumed — no errors, no reconnects. Multiple shards can be migrated concurrently, each managed by an independent Horde worker.

### Flow

```
start_migration(shard, target_db)
  └─ clone DDL (tables, sequences, indexes) to target
  └─ CREATE PUBLICATION on source
  └─ CREATE SUBSCRIPTION on target
  └─ poll replication lag every 5s

drain(shard)
  └─ gate → :closing (no new checkouts; waiting sessions subscribe via PubSub)
  └─ wait for all nodes' active tx to reach 0
  └─ gate → :closed

cutover(shard)  [requires lag = 0]
  └─ update ShardRoute in Mnesia (sync_transaction, all nodes confirmed)
  └─ gate → :open (broadcast via PubSub, all waiting sessions resume)
  └─ replenish pool from new db on all nodes
  └─ cleanup publication, subscription, replication slot
  └─ DROP SCHEMA CASCADE on source (old data removed)
```

### Worker crash recovery

If a migration worker crashes mid-flight, Horde restarts it. On `init`, the worker reads `ShardRoute.status(shard)` to infer the phase and reconnects management sockets:

| Gate status | Recovered phase |
|---|---|
| `:open` + subscription exists | `:replicating` |
| `:closing` | `:draining` |
| `:closed` | drained, awaiting cutover |

### IEx helpers

```elixir
# Database management
DB.list()                                 # all registered databases
DB.shards()                               # all {shard, db_id} mappings
DB.pool("pg-primary-0")                   # idle connection count
DB.set_pool_size("pg-primary-0", 30)      # adjust pool size (persisted)
DB.add("pg-primary-1", "localhost", 5431, "postgres", "postgres", "my_cluster")

# Migration
M.migrate("shard_0002", "pg-primary-1")  # start migration
M.all()                                   # all active migration statuses
M.status("shard_0002")                   # phase, lag_bytes, gate
M.drain("shard_0002")                    # begin drain
M.cutover("shard_0002")                  # cut over (lag must be 0)
M.cancel("shard_0002")                   # abort at any phase

M.gate("shard_0002")                     # :open | :closing | :closed
M.active_tx("shard_0002")               # in-flight tx count on this node
```

---

## Web Dashboard

A React + Phoenix Channel dashboard is included for monitoring and managing migrations via browser.

```bash
# Dev mode (two servers with Vite proxy)
iex -S mix phx.server       # Phoenix on :4000
cd frontend && pnpm dev     # Vite on :5173

# Production build (served by Phoenix)
cd frontend && pnpm build
```

Features:
- Live shard → db mapping and pool stats
- Add databases
- Start / drain / cutover / cancel migrations per shard
- Real-time event log per migration

---

## Getting Started

```bash
# Start databases (wal_level = logical required)
cd db && docker compose up -d

# Init schemas
psql -h 127.0.0.1 -p 5430 -U postgres -d my_cluster -f db/distributed-pg.sql
psql -h 127.0.0.1 -p 5431 -U postgres -d my_cluster -f db/distributed-pg.sql

# Start proxy + web
iex -S mix phx.server

# Connect via proxy
psql -h 127.0.0.1 -p 5400 -U postgres -d my_cluster
```

```sql
-- Simple query
SELECT * FROM shard_0000.users WHERE id = 119639312848388098;

-- Transaction
BEGIN;
INSERT INTO shard_0000.users (username, email) VALUES ('alice', 'alice@example.com');
COMMIT;

-- Extended protocol
SELECT * FROM shard_0000.users WHERE id = $1
\bind 119639312848388098
\g
```

---

## Performance

Benchmarked with pgbench (10 clients, 4 threads, 30s, pool size 10, single shard SELECT).

**Environment**: all instances in the same AZ (us-west-2d)

| Role | Instance |
|---|---|
| pgbench client | c9g.large |
| InstaShard / pgbouncer | c8g.large |
| Aurora PostgreSQL | r8g.large |

**Results**:

| Protocol | Direct | InstaShard | pgbouncer 1.25 | InstaShard overhead |
|---|---|---|---|---|
| Simple (`-M simple`) | 0.348 ms | 0.504 ms | 0.463 ms | +0.156 ms |
| Extended (`-M extended`) | 0.271 ms | 0.482 ms | 0.498 ms | +0.211 ms |
| Prepared (`-M prepared`) | 0.271 ms | 0.482 ms | 0.504 ms | +0.211 ms |

InstaShard matches or beats pgbouncer on extended/prepared protocols despite doing shard routing, statement rewrite, and migration gating. The advantage comes from cross-transaction prepared statement reuse — the same SQL hitting the same backend socket skips Parse entirely, while pgbouncer (transaction mode) loses stmt state on every checkin.

**Key optimizations**:
- **Batch send**: all send-only messages (P/B/E/D/C) collected and flushed in one `writev` with Sync
- **Batch recv**: backend responses scanned in-memory for ReadyForQuery; forwarded as one iolist
- **Stmt dedup**: repeated Parses skipped; fake ParseComplete injected into response stream
- **Lock-free pool**: ETS `ordered_set` + `take` — no GenServer on the checkout path
- **:queue buffer**: O(1) enqueue/dequeue, no list copy

```bash
# Reproduce
pgbench -h proxy-host -p 5431 -U postgres -M prepared \
  -f pgbench/bench_ext_read.sql -c 10 -j 4 -T 30 -r --no-vacuum my_cluster
```

---

## References

- [Sharding & IDs at Instagram (2012)](https://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram)
- [PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
