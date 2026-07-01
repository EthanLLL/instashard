# InstaShard

A PostgreSQL sharding proxy in Elixir. Implements the PostgreSQL wire protocol, routes queries to physical databases based on shard schema names embedded in SQL, and multiplexes many client connections over a small backend connection pool.

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
- 4096 logical shards provisioned (0–4095), capacity up to 8191
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
Proxy.PgProtocol         wire protocol helpers
  │
Backend.Pool             ETS lock-free connection pool
Backend.Manager          shard→db mapping, replenishment
Backend.StmtCache        global prepared stmt cache (ETS)
Backend.Connection       TCP + MD5/SCRAM-SHA-256 auth
  │                          │
db0 :5430                db1 :5431
shard_0000..shard_2047   shard_2048..shard_4095
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
| Configurable shard→db mapping | Planned |
| Live shard migration | Planned |

---

## Prepared Statement Rewrite

Client-named statements are rewritten to internal names (`is_<hash>`) so the same SQL always maps to the same backend statement name regardless of which client sent it.

- `StmtCache` (ETS): `sql_hash → {internal_name, sql}` — global, never evicted
- Pool entry: `{socket, parse_count, stmt_set}` — tracks which stmts are prepared per socket
- Session state: `stmt_map` — `client_name → {internal_name, shard}`
- If a socket already has the stmt: skip Parse, inject fake `ParseComplete` before Sync
- If Bind arrives without Parse (pure re-execute): checkout socket, re-parse if needed
- Socket replaced when `parse_count > 100` to bound per-connection state

---

## Getting Started

```bash
# Start databases
cd db && docker compose up -d

# Init schemas on both nodes
psql -h 127.0.0.1 -p 5430 -U postgres -d my_cluster -f db/distributed-pg.sql
psql -h 127.0.0.1 -p 5431 -U postgres -d my_cluster -f db/distributed-pg.sql

# Start proxy
iex -S mix phx.server

# Connect
psql -h 127.0.0.1 -p 5400 -U postgres -d my_cluster
```

```sql
-- Simple query
SELECT * FROM shard_0000.users;

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
