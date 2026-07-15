\set id1 random(1, 100000)
\set id2 random(1, 100000)
\set id3 random(1, 100000)
SELECT id, username, email FROM shard_0000.users WHERE id = :id1;
SELECT id, username, email FROM shard_0000.users WHERE id = :id2;
SELECT id, username, email FROM shard_0000.users WHERE id = :id3;
