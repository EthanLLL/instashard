defmodule Instashard.Proxy.PgProtocol do
  @doc """
  Extract the SQL string from a Parse (P) message.
  Format: P | int32 len | cstring stmt_name | cstring query | int16 param_count | ...
  """
  def parse_sql_from_parse(<<?P, _len::32, rest::binary>>) do
    # Skip the statement name (first null-terminated string)
    case :binary.split(rest, <<0>>) do
      [_stmt_name, after_name] ->
        # SQL is the next null-terminated string
        case :binary.split(after_name, <<0>>) do
          [sql | _] -> sql
          _ -> ""
        end
      _ -> ""
    end
  end

  def parse_sql_from_parse(_), do: ""

  @doc "Parse null-terminated key=value pairs from a startup message payload."
  def parse_startup_params(payload) do
    payload
    |> String.split(<<0>>, trim: true)
    |> Enum.chunk_every(2)
    |> Map.new(fn
      [k, v] -> {k, v}
      [k] -> {k, ""}
    end)
  end

  @doc """
  Checks if the packet ends with a ReadyForQuery (Z) message.
  Returns {:ok, :idle | :in_transaction | :failed} or :not_ready.
  """
  def ready_for_query_status(packet) do
    size = byte_size(packet)
    case packet do
      <<_::binary-size(size - 6), ?Z, 5::32, status::8>> ->
        tx_status = case status do
          ?I -> :idle
          ?T -> :in_transaction
          ?E -> :failed
          _  -> :idle
        end
        {:ok, tx_status}
      _ ->
        :not_ready
    end
  end
end
