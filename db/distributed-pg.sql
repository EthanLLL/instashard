-- Create 4096 schemas
DO $$
BEGIN
    FOR i IN 0..4095 LOOP
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS shard_%s', to_char(i, 'FM0000'));
    END LOOP;
END $$;

-- Create 4096 sequence numbers
DO $$
BEGIN
    FOR i IN 0..4095 LOOP
        EXECUTE format('CREATE SEQUENCE IF NOT EXISTS shard_%s.id_sequence', to_char(i, 'FM0000'));
    END LOOP;
END $$;

-- Create snowflake_id function
CREATE OR REPLACE FUNCTION public.generate_snowflake_id(schema_name text, OUT result bigint) AS $$
DECLARE
    -- 设定 2026-01-01 00:00:00 UTC 的毫秒时间戳作为时代起点
    our_epoch bigint := 1767225600000;
    seq_id bigint;
    now_millis bigint;
    shard_id int;
BEGIN
    -- 1. 从输入的 schema_name (如 'shard_0211') 中抠出后四位数字作为逻辑分片 ID
    shard_id := substring(schema_name from 'shard_([0-9]{4})')::int;

    -- 2. 获取当前的系统毫秒级时间戳
    SELECT floor(extract(epoch FROM clock_timestamp()) * 1000) INTO now_millis;

    -- 3. 动态调用对应 schema 下的序列，并对 1024 取模 (限制在 10 bit)
    EXECUTE format('SELECT nextval(%L) %% 1024', schema_name || '.id_sequence') INTO seq_id;

    -- 4. 硬核位运算拼接 (时间差左移23位 | 分片ID左移10位 | 序列号)
    result := (now_millis - our_epoch) << 23;
    result := result | (shard_id << 10);
    result := result | seq_id;
END;
$$ LANGUAGE plpgsql;

-- Create table inside of each schema
DO $$
BEGIN
    FOR i IN 0..4095 LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS shard_%s.users (
                id bigint NOT NULL DEFAULT public.generate_snowflake_id(%L),
                username varchar(50) NOT NULL,
                email varchar(100),
                created_at timestamp DEFAULT now()
            );', 
            to_char(i, 'FM0000'), 
            'shard_' || to_char(i, 'FM0000'), 
            to_char(i, 'FM0000')
        );
    END LOOP;
END $$;
