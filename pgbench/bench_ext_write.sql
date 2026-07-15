\set suffix random(1, 999999)
INSERT INTO shard_0000.users (username, email) VALUES ('user_' || :suffix::text, 'user_' || :suffix::text || '@test.com');
