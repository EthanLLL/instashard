defmodule Instashard.Proxy.ShardRouter do
  # Matches a complete shard schema name. Change this when shard naming convention changes.
  # Full-string match (\A...\z) prevents partial matches like "old_shard_0001".
  @shard_pattern ~r/\Ashard_\d{4}\z/

  @doc """
  Extracts the shard name from a SQL string.
  Only inspects schema-qualified identifiers after FROM/JOIN/INTO/UPDATE/TABLE.
  String literals are stripped first to prevent false matches.
  Returns {:ok, shard_name} or :no_shard.
  """
  def extract_shard(sql) do
    stripped = Regex.replace(~r/'(?:[^'\\]|\\.)*'/, sql, "''")

    schema_refs =
      Regex.scan(
        ~r/(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+("?[A-Za-z_][A-Za-z0-9_]*"?)\."?[A-Za-z_][A-Za-z0-9_]*"?/i,
        stripped,
        capture: :all_but_first
      )
      |> List.flatten()
      |> Enum.map(&String.trim(&1, "\""))

    case Enum.find(schema_refs, &Regex.match?(@shard_pattern, &1)) do
      nil -> :no_shard
      shard -> {:ok, shard}
    end
  end
end
