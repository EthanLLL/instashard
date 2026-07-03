defmodule Instashard.Backend.SchemaCloner do
  @moduledoc """
  Clones a shard schema (DDL only) from a source DB connection to a target DB connection.
  Reads table definitions from pg_catalog — no pg_dump dependency.

  Steps:
    1. List all tables in the shard schema
    2. For each table: build CREATE TABLE from columns + constraints
    3. Create non-constraint indexes
    4. Create the schema itself if missing
  """

  require Logger

  @doc """
  Clone all tables in `shard` from source socket to target socket.
  Both sockets must be authenticated backend connections (from Connection.connect/1).
  """
  def clone(shard, source_socket, target_socket) do
    with {:ok, tables} <- list_tables(source_socket, shard),
         :ok <- ensure_schema(target_socket, shard),
         :ok <- clone_sequences(shard, source_socket, target_socket) do
      Enum.reduce_while(tables, :ok, fn table, :ok ->
        case clone_table(shard, table, source_socket, target_socket) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {table, reason}}}
        end
      end)
    end
  end

  # ── Schema ────────────────────────────────────────────────────────────

  defp ensure_schema(socket, shard) do
    sql = "CREATE SCHEMA IF NOT EXISTS #{quote_ident(shard)}"
    case simple_query(socket, sql) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  # ── Sequences ─────────────────────────────────────────────────────────

  defp clone_sequences(shard, source, target) do
    sql = """
    SELECT
      s.relname,
      p.start_value,
      p.increment_by,
      p.max_value,
      p.min_value,
      p.cache_size,
      p.cycle
    FROM pg_class s
    JOIN pg_namespace n ON n.oid = s.relnamespace
    JOIN pg_sequences p ON p.schemaname = n.nspname AND p.sequencename = s.relname
    WHERE n.nspname = '#{escape(shard)}'
      AND s.relkind = 'S'
    ORDER BY s.relname
    """
    case simple_query(source, sql) do
      {:ok, rows} ->
        Enum.reduce_while(rows, :ok, fn row, :ok ->
          case clone_sequence(shard, row, target) do
            :ok -> {:cont, :ok}
            err -> {:halt, err}
          end
        end)
      err -> err
    end
  end

  defp clone_sequence(shard, [name, start, inc, max, min, cache, cycle], target) do
    qualified = "#{quote_ident(shard)}.#{quote_ident(name)}"
    cycle_clause = if cycle == "t", do: "CYCLE", else: "NO CYCLE"
    create_sql = """
    CREATE SEQUENCE IF NOT EXISTS #{qualified}
      START WITH #{start}
      INCREMENT BY #{inc}
      MINVALUE #{min}
      MAXVALUE #{max}
      CACHE #{cache}
      #{cycle_clause}
    """
    case simple_query(target, create_sql) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  # ── Table list ────────────────────────────────────────────────────────

  defp list_tables(socket, shard) do
    sql = """
    SELECT tablename FROM pg_tables
    WHERE schemaname = '#{escape(shard)}'
    ORDER BY tablename
    """
    case simple_query(socket, sql) do
      {:ok, rows} -> {:ok, Enum.map(rows, fn [name] -> name end)}
      err -> err
    end
  end

  # ── Table clone ───────────────────────────────────────────────────────

  defp clone_table(shard, table, source, target) do
    qualified = "#{quote_ident(shard)}.#{quote_ident(table)}"

    with {:ok, col_defs} <- column_defs(source, shard, table),
         {:ok, constraint_defs} <- constraint_defs(source, shard, table),
         {:ok, index_defs} <- index_defs(source, shard, table) do

      all_defs = col_defs ++ constraint_defs
      create_sql = "CREATE TABLE IF NOT EXISTS #{qualified} (\n  #{Enum.join(all_defs, ",\n  ")}\n)"

      with {:ok, _} <- simple_query(target, create_sql) do
        Enum.reduce_while(index_defs, :ok, fn idx_sql, :ok ->
          # Replace CREATE INDEX with CREATE INDEX IF NOT EXISTS
          safe = String.replace(idx_sql, "CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", global: false)
                 |> String.replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ", global: false)
          case simple_query(target, safe) do
            {:ok, _} -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
      end
    end
  end

  defp column_defs(socket, shard, table) do
    sql = """
    SELECT
      a.attname,
      pg_catalog.format_type(a.atttypid, a.atttypmod),
      a.attnotnull,
      pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_expr
    FROM pg_catalog.pg_attribute a
    LEFT JOIN pg_catalog.pg_attrdef d
      ON a.attrelid = d.adrelid AND a.attnum = d.adnum
    WHERE a.attrelid = '#{escape(shard)}.#{escape(table)}'::regclass
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum
    """
    case simple_query(socket, sql) do
      {:ok, rows} ->
        defs = Enum.map(rows, fn [name, type, notnull, default] ->
          parts = ["#{quote_ident(name)} #{type}"]
          parts = if notnull == "t", do: parts ++ ["NOT NULL"], else: parts
          parts = if default != nil, do: parts ++ ["DEFAULT #{default}"], else: parts
          Enum.join(parts, " ")
        end)
        {:ok, defs}
      err -> err
    end
  end

  defp constraint_defs(socket, shard, table) do
    sql = """
    SELECT pg_catalog.pg_get_constraintdef(oid, true)
    FROM pg_catalog.pg_constraint
    WHERE conrelid = '#{escape(shard)}.#{escape(table)}'::regclass
      AND contype != 'f'
    ORDER BY conname
    """
    case simple_query(socket, sql) do
      {:ok, rows} -> {:ok, Enum.map(rows, fn [def] -> def end)}
      err -> err
    end
  end

  defp index_defs(socket, shard, table) do
    sql = """
    SELECT indexdef FROM pg_indexes
    WHERE schemaname = '#{escape(shard)}'
      AND tablename = '#{escape(table)}'
      AND indexname NOT IN (
        SELECT conname FROM pg_catalog.pg_constraint
        WHERE conrelid = '#{escape(shard)}.#{escape(table)}'::regclass
      )
    ORDER BY indexname
    """
    case simple_query(socket, sql) do
      {:ok, rows} -> {:ok, Enum.map(rows, fn [def] -> def end)}
      err -> err
    end
  end

  # ── Simple query over a raw backend socket ────────────────────────────

  def simple_query(socket, sql) do
    body = sql <> <<0>>
    len = byte_size(body) + 4
    :ok = :gen_tcp.send(socket, <<?Q, len::32, body::binary>>)
    collect_response(socket, [])
  end

  defp collect_response(socket, rows) do
    case Instashard.Backend.Connection.recv_msg(socket) do
      {:ok, <<?Z, _::binary>>} ->
        {:ok, Enum.reverse(rows)}

      {:ok, <<?D, _len::32, _ncols::16, rest::binary>>} ->
        row = parse_data_row(rest)
        collect_response(socket, [row | rows])

      {:ok, <<?E, _len::32, fields::binary>>} ->
        msg = extract_error_message(fields)
        {:error, msg}

      {:ok, _other} ->
        collect_response(socket, rows)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_data_row(bin) do
    parse_cols(bin, [])
  end

  defp parse_cols(<<>>, acc), do: Enum.reverse(acc)
  defp parse_cols(<<-1::32-signed, rest::binary>>, acc), do: parse_cols(rest, [nil | acc])
  defp parse_cols(<<len::32, val::binary-size(len), rest::binary>>, acc),
    do: parse_cols(rest, [val | acc])

  defp extract_error_message(fields) do
    fields
    |> String.split(<<0>>, trim: true)
    |> Enum.find_value("unknown error", fn
      <<?M, msg::binary>> -> msg
      _ -> nil
    end)
  end

  defp quote_ident(name), do: ~s("#{String.replace(name, ~s("), ~s(""))}")
  defp escape(str), do: String.replace(str, "'", "''")
end
