BEGIN;
INSERT INTO shard_0000.users (username, email)
VALUES ('user_:username', 'user_:username@test.com');
COMMIT;
