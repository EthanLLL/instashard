import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect(host="localhost", port=5400, user="postgres", database="my_cluster")

    print("=== prepare stmt1 and stmt2 ===")
    stmt1 = await conn.prepare("SELECT * FROM shard_0000.users WHERE id = $1")
    stmt2 = await conn.prepare("SELECT * FROM shard_0000.users WHERE username = $1")
    print("both prepared OK")

    print("\n=== execute stmt1 (first time) ===")
    rows = await stmt1.fetch(119639312848388098)
    print(rows)

    print("\n=== execute stmt2 (first time) ===")
    rows = await stmt2.fetch("cammy")
    print(rows)

    print("\n=== execute stmt1 (second time, pure Bind — no Parse) ===")
    rows = await stmt1.fetch(119638145221263361)
    print(rows)

    print("\n=== execute stmt2 (second time, pure Bind — no Parse) ===")
    rows = await stmt2.fetch("mai")
    print(rows)

    await conn.close()
    print("\ndone")

asyncio.run(main())
