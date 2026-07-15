\set id random(1, 100000)
SELECT id, username, email, created_at FROM shard_0000.users WHERE id = :id;
