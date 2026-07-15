\set suffix random(1, 999999)
\set id random(1, 100000)
BEGIN;
INSERT INTO shard_0000.users (username, email) VALUES ('user_' || :suffix::text, 'user_' || :suffix::text || '@test.com');
SELECT id, username, email FROM shard_0000.users WHERE id = :id;
END;
