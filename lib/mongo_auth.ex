defmodule Mongo.Auth do
  def auth(opts, mongo) do
    user = opts[:username]
    passwd = opts[:password]
    mod = mechanism(mongo)
    mod.auth(user, passwd, mongo)
  end

  @doc false
  defp mechanism(%Mongo.Server{wire_version: version}) when version >= 3, do: Mongo.Auth.SCRAM
  defp mechanism(_), do: Mongo.Auth.CR
end


defmodule Mongo.Auth.CR do
  def auth(username, password, mongo) do
    nonce = getnonce(mongo)
    hash_password = hash(username <> ":mongo:" <> password)

    Mongo.Server.cmd_sync(mongo, 
                          %{authenticate: 1, nonce: nonce, user: username, 
                            key: hash(nonce <> username <> hash_password)})
    |> case do
      {:ok, resp} ->
        case Mongo.Response.success(resp) do
          :ok -> {:ok, mongo}
          error -> error
        end
      error -> error
    end
  end
  
  # get `nonce` token from server
  defp getnonce(mongo) do
    case Mongo.Server.cmd_sync(mongo, %{getnonce: true}) do
      {:ok, resp} -> resp |> Mongo.Response.getnonce
      error -> error
    end
  end

  # creates a md5 hash in hex with loawercase
  defp hash(data) do
    :crypto.hash(:md5, data) |> binary_to_hex
  end

  # creates an hex string from binary
  defp binary_to_hex(bin) do
    for << <<b::4>> <- bin >>, into: <<>> do
        <<Integer.to_string(b,16)::binary>>
    end |> String.downcase
  end
end

defmodule Mongo.Auth.SCRAM do
  use Bitwise

  def auth(username, password, mongo) do
    nonce      = nonce()
    first_bare = first_bare(username, nonce)
    payload    = first_message(first_bare)
    message    = [saslStart: 1, mechanism: "SCRAM-SHA-1", payload: payload]

    with {:ok, reply} <- command(message, mongo),
         {message, signature} = first(reply, first_bare, username, password, nonce),
         {:ok, reply} <- command(message, mongo),
         message = second(reply, signature),
         {:ok, reply} <- command(message, mongo),
         :ok <- final(reply) do
           {:ok, mongo}
    else
      {:ok, %{ok: z, errmsg: reason, code: code}} when z == 0 ->
        {:error, %Mongo.Error{msg: "auth failed for user #{username}: #{reason}"}}
      error -> error
    end
  end


  defp command(cmd, mongo) do
    case Mongo.Server.cmd_sync(mongo, cmd) do
      {:ok, resp} -> 
        case Mongo.Response.cmd(resp) do
          {:ok, %{ok: ok} = reply} when ok == 1 ->  {:ok, reply}
          error -> error
        end
      error -> error
    end
  end

  defp first(%{conversationId: 1, payload: server_payload, done: false},
             first_bare, username, password, client_nonce) do
    params          = parse_payload(server_payload)
    server_nonce    = params["r"]
    salt            = params["s"] |> Base.decode64!
    iter            = params["i"] |> String.to_integer
    pass            = digest_password(username, password)
    salted_password = hi(pass, salt, iter)

    <<^client_nonce::binary-size(24), _::binary>> = server_nonce

    client_message       = "c=biws,r=#{server_nonce}"
    auth_message         = "#{first_bare},#{server_payload.bin},#{client_message}"
    server_signature     = generate_signature(salted_password, auth_message)
    proof                = generate_proof(salted_password, auth_message)
    client_final_message = %Bson.Bin{bin: "#{client_message},#{proof}"}
    message              = [saslContinue: 1, conversationId: 1, payload: client_final_message]

    {message, server_signature}
  end

  defp second(%{conversationId: 1, payload: payload, done: false}, signature) do
    params = parse_payload(payload)
    ^signature = params["v"] |> Base.decode64!
    [saslContinue: 1, conversationId: 1, payload: %Bson.Bin{bin: ""}]
  end

  defp final(%{conversationId: 1, payload: %Bson.Bin{bin: ""}, done: true}), do: :ok
  defp final(_), do: :failed

  defp first_message(first_bare) do
    %Bson.Bin{bin: "n,,#{first_bare}"}
  end

  defp first_bare(username, nonce) do
    "n=#{encode_username(username)},r=#{nonce}"
  end

  defp hi(password, salt, iterations) do
    Mongo.PBKDF2.generate(password, salt, 
                          iterations: iterations, length: 20, digest: :sha)
  end

  defp generate_proof(salted_password, auth_message) do
    client_key = :crypto.hmac(:sha, salted_password, "Client Key")
    stored_key = :crypto.hash(:sha, client_key)
    signature = :crypto.hmac(:sha, stored_key, auth_message)
    client_proof = xor_keys(client_key, signature, "")
    "p=#{Base.encode64(client_proof)}"
  end

  defp generate_signature(salted_password, auth_message) do
    server_key = :crypto.hmac(:sha, salted_password, "Server Key")
    :crypto.hmac(:sha, server_key, auth_message)
  end

  defp xor_keys("", "", result),
    do: result
  defp xor_keys(<<fa, ra::binary>>, <<fb, rb::binary>>, result),
    do: xor_keys(ra, rb, <<result::binary, fa ^^^ fb>>)


  defp nonce do
    :crypto.strong_rand_bytes(18)
    |> Base.encode64
  end

  defp encode_username(username) do
    username
    |> String.replace("=", "=3D")
    |> String.replace(",", "=2C")
  end

  defp parse_payload(%Bson.Bin{subtype: 0, bin: payload}) do
    payload
    |> String.split(",")
    |> Enum.into(%{}, &List.to_tuple(String.split(&1, "=", parts: 2)))
  end

  defp digest_password(username, password) do
    :crypto.hash(:md5, [username, ":mongo:", password])
    |> Base.encode16(case: :lower)
  end
end

defmodule Mongo.PBKDF2 do
  # From https://github.com/elixir-lang/plug/blob/ef616a9db9c87ec392dd8a0949bc52fafcf37005/lib/plug/crypto/key_generator.ex
  # with modifications

  @moduledoc """
  `PBKDF2` implements PBKDF2 (Password-Based Key Derivation Function 2),
  part of PKCS #5 v2.0 (Password-Based Cryptography Specification).
  It can be used to derive a number of keys for various purposes from a given
  secret. This lets applications have a single secure secret, but avoid reusing
  that key in multiple incompatible contexts.
  see http://tools.ietf.org/html/rfc2898#section-5.2
  """

  use Bitwise
  @max_length bsl(1, 32) - 1

  @doc """
  Returns a derived key suitable for use.
  ## Options
    * `:iterations` - defaults to 1000 (increase to at least 2^16 if used for passwords);
    * `:length`     - a length in octets for the derived key. Defaults to 32;
    * `:digest`     - an hmac function to use as the pseudo-random function. Defaults to `:sha256`;
  """
  def generate(secret, salt, opts \\ []) do
    iterations = Keyword.get(opts, :iterations, 1000)
    length = Keyword.get(opts, :length, 32)
    digest = Keyword.get(opts, :digest, :sha256)

    if length > @max_length do
      raise ArgumentError, "length must be less than or equal to #{@max_length}"
    else
      generate(mac_fun(digest, secret), salt, iterations, length, 1, [], 0)
    end
  end

  defp generate(_fun, _salt, _iterations, max_length, _block_index, acc, length)
      when length >= max_length do
    key = acc |> Enum.reverse |> IO.iodata_to_binary
    <<bin::binary-size(max_length), _::binary>> = key
    bin
  end

  defp generate(fun, salt, iterations, max_length, block_index, acc, length) do
    initial = fun.(<<salt::binary, block_index::integer-size(32)>>)
    block   = iterate(fun, iterations - 1, initial, initial)
    generate(fun, salt, iterations, max_length, block_index + 1,
             [block | acc], byte_size(block) + length)
  end

  defp iterate(_fun, 0, _prev, acc), do: acc

  defp iterate(fun, iteration, prev, acc) do
    next = fun.(prev)
    iterate(fun, iteration - 1, next, :crypto.exor(next, acc))
  end

  defp mac_fun(digest, secret) do
    &:crypto.hmac(digest, secret, &1)
  end
end
