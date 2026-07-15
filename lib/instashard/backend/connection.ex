defmodule Instashard.Backend.Connection do
  require Logger

  @doc """
  Connect and authenticate with a PostgreSQL backend.
  Supports MD5 and SCRAM-SHA-256. Returns an authenticated socket in passive mode.
  """
  def connect(%{host: host, port: port, username: username, password: password, database: database}) do
    host_cl = if is_binary(host), do: String.to_charlist(host), else: host

    with {:ok, socket} <- :gen_tcp.connect(host_cl, port, [:binary, packet: 0, active: false, nodelay: true], 5000),
         :ok <- handshake(socket, username, password, database) do
      {:ok, socket}
    else
      {:error, reason} = err ->
        Logger.error("[Connection] Failed to connect #{host}:#{port} — #{inspect(reason)}")
        err
    end
  end

  defp handshake(socket, username, password, database) do
    payload = <<0, 3, 0, 0,
                "user", 0, username::binary, 0,
                "database", 0, database::binary, 0,
                0>>
    len = byte_size(payload) + 4
    :ok = :gen_tcp.send(socket, <<len::32, payload::binary>>)
    auth_loop(socket, username, password)
  end

  defp auth_loop(socket, username, password) do
    case recv_msg(socket) do
      {:ok, <<?R, _::32, 0::32>>} ->
        drain_ready(socket)

      {:ok, <<?R, _::32, 5::32, salt::binary-4>>} ->
        :ok = send_md5(socket, username, password, salt)
        auth_loop(socket, username, password)

      {:ok, <<?R, _::32, 10::32, rest::binary>>} ->
        mechanisms = String.split(rest, <<0>>, trim: true)
        if "SCRAM-SHA-256" in mechanisms,
          do: scram_sha256(socket, username, password),
          else: {:error, {:unsupported_auth, mechanisms}}

      {:ok, <<?E, _::32, fields::binary>>} ->
        {:error, {:pg_error, error_message(fields)}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # MD5: "md5" + md5(md5(password + username) + salt)
  defp send_md5(socket, username, password, salt) do
    hash = "md5" <> md5hex(md5hex(password <> username) <> salt) <> <<0>>
    len = byte_size(hash) + 4
    :gen_tcp.send(socket, <<?p, len::32, hash::binary>>)
  end

  defp md5hex(data), do: :crypto.hash(:md5, data) |> Base.encode16(case: :lower)

  defp scram_sha256(socket, username, password) do
    client_nonce = Base.encode64(:crypto.strong_rand_bytes(18))
    first_bare = "n=#{username},r=#{client_nonce}"
    client_first = "n,," <> first_bare

    body = "SCRAM-SHA-256" <> <<0, byte_size(client_first)::32>> <> client_first
    len = byte_size(body) + 4
    :ok = :gen_tcp.send(socket, <<?p, len::32, body::binary>>)

    with {:ok, <<?R, _::32, 11::32, server_first::binary>>} <- recv_msg(socket),
         {:ok, sr} <- parse_server_first(server_first),
         true <- String.starts_with?(sr.nonce, client_nonce) do

      final_no_proof = "c=" <> Base.encode64("n,,") <> ",r=" <> sr.nonce
      auth_msg = first_bare <> "," <> server_first <> "," <> final_no_proof

      salted = pbkdf2(password, sr.salt, sr.iterations)
      ck = hmac(salted, "Client Key")
      proof = :crypto.exor(ck, hmac(:crypto.hash(:sha256, ck), auth_msg))

      final = final_no_proof <> ",p=" <> Base.encode64(proof)
      flen = byte_size(final) + 4
      :ok = :gen_tcp.send(socket, <<?p, flen::32, final::binary>>)

      with {:ok, <<?R, _::32, 12::32, server_final::binary>>} <- recv_msg(socket) do
        expected = "v=" <> Base.encode64(hmac(hmac(salted, "Server Key"), auth_msg))

        if server_final == expected,
          do: auth_loop(socket, nil, nil),
          else: {:error, :server_signature_mismatch}
      end
    else
      false -> {:error, :nonce_mismatch}
      err -> err
    end
  end

  defp parse_server_first(msg) do
    parts =
      msg
      |> String.split(",")
      |> Map.new(fn kv ->
        [k, v] = String.split(kv, "=", parts: 2)
        {k, v}
      end)

    with %{"r" => nonce, "s" => s64, "i" => i_str} <- parts,
         {:ok, salt} <- Base.decode64(s64),
         {iters, ""} <- Integer.parse(i_str) do
      {:ok, %{nonce: nonce, salt: salt, iterations: iters}}
    else
      _ -> {:error, :bad_server_first}
    end
  end

  defp pbkdf2(password, salt, iters),
    do: :crypto.pbkdf2_hmac(:sha256, password, salt, iters, 32)

  defp hmac(key, data), do: :crypto.mac(:hmac, :sha256, key, data)

  defp drain_ready(socket) do
    case recv_msg(socket) do
      {:ok, <<?Z, _::32, _::8>>} -> :ok
      {:ok, _} -> drain_ready(socket)
      err -> err
    end
  end

  def recv_msg(socket) do
    with {:ok, <<type::8, len::32>>} <- :gen_tcp.recv(socket, 5, 5000) do
      body_len = len - 4

      if body_len > 0 do
        case :gen_tcp.recv(socket, body_len, 5000) do
          {:ok, body} -> {:ok, <<type::8, len::32, body::binary>>}
          err -> err
        end
      else
        {:ok, <<type::8, len::32>>}
      end
    end
  end

  defp error_message(fields) do
    fields
    |> String.split(<<0>>, trim: true)
    |> Enum.find_value("unknown error", fn
      <<?M, msg::binary>> -> msg
      _ -> nil
    end)
  end
end
