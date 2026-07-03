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
Proxy.Listener           TCP accept
Proxy.SessionSupervisor  DynamicSupervisor
Proxy.ClientSession      one GenServer per client
Proxy.ShardRouter        SQL → shard name
  │
Backend.Pool             ETS lock-free connection pool
Backend.Manager          pool replenishment
Backend.StmtCache        global prepared stmt rewrite (ETS)
Backend.ShardMapping     shard→db config (Mnesia)
Backend.MigrationGate    per-shard checkout gate (Mnesia + ETS)
Backend.Migration        live migration state machine (GenServer)
Backend.SchemaCloner     DDL copy via pg_catalog
Backend.Connection       TCP + MD5/SCRAM-SHA-256 auth
  │                          │
db0 :5430                db1 :5431
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
| Shard→db mapping (Mnesia) | Done |
| Live shard migration | Done |
| Config file for shard mapping | Planned |
| Migration coordinator HA (Horde / :global) | Planned |

---

## Prepared Statement Rewrite

Client statements are rewritten to internal names (`is_<16-char-hash>`) so the same SQL always maps to the same backend name across connections.

- `StmtCache`: global ETS, `sql_hash → {internal_name, sql}`
- Pool entry: `{socket, parse_count, stmt_set}` — which stmts are prepared per socket
- Session `stmt_map`: `client_name → {internal_name, shard}`
- Stmt already on socket → skip Parse, inject fake `ParseComplete` before Sync
- Bind without preceding Parse → checkout, re-parse on new socket if needed
- Socket replaced after 100 prepares to bound per-connection state

---

## Live Shard Migration

Online migration between physical databases using PostgreSQL logical replication. Client sessions are transparently held and resumed — no errors, no reconnects.

### Flow

```
start_migration(shard, target_db)
  └─ clone DDL (tables, sequences, indexes) to target
  └─ CREATE PUBLICATION on source
  └─ CREATE SUBSCRIPTION on target
  └─ poll replication lag every 5s

drain(shard)
  └─ gate → :closing (no new checkouts)
  └─ wait for all nodes' pools to fully drain (in-flight tx finish naturally)
  └─ gate → :closed

cutover(shard)  [requires lag = 0]
  └─ update ShardMapping in Mnesia
  └─ flush old connections on all nodes
  └─ gate → :open
  └─ replenish pool from new db on all nodes
  └─ notify held sessions → replay buffered pipeline on new db
  └─ cleanup publication, subscription, replication slot
```

### IEx helpers

```elixir
M.migrate("shard_0002", :db1)   # start migration
M.status("shard_0002")          # phase, lag_bytes
M.drain("shard_0002")           # begin drain
M.cutover("shard_0002")         # cut over (lag must be 0)
M.cancel("shard_0002")          # abort at any phase

M.shards()                      # all shard→db mappings
M.pool("shard_0002")            # idle connection count
M.gate("shard_0002")            # :open | :closing | :closed
```

---

## Getting Started

```bash
# Start databases (wal_level = logical required)
cd db && docker compose up -d

# Init schemas
psql -h 127.0.0.1 -p 5430 -U postgres -d my_cluster -f db/distributed-pg.sql
psql -h 127.0.0.1 -p 5431 -U postgres -d my_cluster -f db/distributed-pg.sql

# Start proxy
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

## References

- [Sharding & IDs at Instagram (2012)](https://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram)
- [PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
