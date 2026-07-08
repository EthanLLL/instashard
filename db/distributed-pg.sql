-- Create 4096 schemas
DO $$
BEGIN
    FOR i IN 2..2 LOOP
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS shard_%s', to_char(i, 'FM0000'));
    END LOOP;
END $$;

-- Create 4096 sequence numbers
DO $$
BEGIN
    FOR i IN 2..2 LOOP
        EXECUTE format('CREATE SEQUENCE IF NOT EXISTS shard_%s.id_sequence', to_char(i, 'FM0000'));
    END LOOP;
END $$;

-- Create snowflake_id function
CREATE OR REPLACE FUNCTION public.generate_snowflake_id(schema_name text, OUT result bigint) AS $$
DECLARE
    -- Start from 2026-01-01 00:00:00 UTC
    our_epoch bigint := 1767225600000;
    seq_id bigint;
    now_millis bigint;
    shard_id int;
BEGIN
    shard_id := substring(schema_name from 'shard_([0-9]+)')::int;

    SELECT floor(extract(epoch FROM clock_timestamp()) * 1000) INTO now_millis;

    EXECUTE format('SELECT nextval(%L) %% 1024', schema_name || '.id_sequence') INTO seq_id;

    result := (now_millis - our_epoch) << 23;
    result := result | (shard_id << 10);  -- shard_id occupies bits 10–22 (13 bits, 0–8191)
    result := result | seq_id;
END;
$$ LANGUAGE plpgsql;

-- Create table inside of each schema
DO $$
BEGIN
    FOR i IN 2..2 LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS shard_%s.users (
                id bigint NOT NULL DEFAULT public.generate_snowflake_id(%L),
                username varchar(50) NOT NULL,
                email varchar(100),
                created_at timestamp DEFAULT now()
            );',
            to_char(i, 'FM0000'),
            'shard_' || to_char(i, 'FM0000')
        );
    END LOOP;
END $$;
