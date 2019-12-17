defmodule Mongo.Server do
  import Kernel, except: [send: 2]
  @moduledoc """
  Manage the connection to a mongodb server
  """
  defstruct [
    host: nil,
    port: nil,
    mode: false,
    timeout: nil,
    wire_version: nil,
    opts: %{},
    id_prefix: nil,
    socket: nil ]

  @port    27017
  @mode    :passive
  @host    "127.0.0.1"
  @timeout 6000

  use Mongo.Helpers
  require Logger

  @doc """
  connects to local mongodb server by defaults to {"127.0.0.1", 27017}

  This can be overwritten by the environment variable `:host`, ie:

  ```erlang
  [
    {mongo,
      [
        {host, {"127.0.0.1", 27017}}
      ]}
  ].
  ```
  """
  def connect do
    connect %{}
  end

  @doc """
  connects to a mongodb server
  """
  def connect(host, port) when is_binary(host) and is_integer(port) do
    connect %{host: host, port: port}
  end

  @doc """
  connects to a mongodb server specifying options

  Opts must be a Map
  """
  def connect(opts) when is_map(opts) do
    host = Map.get(opts, :host, @host)
    mongo_server = %Mongo.Server{
      host: case host do
              host when is_binary(host) -> String.to_charlist(host)
              host -> host
            end,
      port: Map.get(opts, :port,    @port),
      mode: Map.get(opts, :mode,    @mode),
      timeout: Map.get(opts, :timeout,    @timeout),
      id_prefix: mongo_prefix()}

    case tcp_connect(mongo_server) do
      {:ok, s} ->
        with {:ok, s1} <- wire_version(s),
             {:ok, s2} <- maybe_auth(opts, s1) do
          {:ok, s2}
        else
          error ->
            close(s)
            error
        end
      error -> error
    end
  end

  @doc false
  def tcp_connect(mongo) do
    case :gen_tcp.connect(mongo.host, mongo.port, tcp_options(mongo), mongo.timeout) do
      {:ok, socket} ->
        {:ok, %Mongo.Server{mongo| socket: socket}}
      error -> error
    end
  end

  @doc false
  defp wire_version(mongo) do
    cmd = %{ismaster: 1}
    case cmd_sync(mongo, cmd) do
      {:ok, resp} ->
        case Mongo.Response.cmd(resp) do
          {:ok, %{maxWireVersion: version}} -> {:ok, %{mongo | wire_version: version}}
          {:ok, %{ok: ok}} when ok == 1 -> {:ok, %{mongo | wire_version: 0}}
          error -> error
        end
      error -> error
    end
  end

  @doc false
  defp maybe_auth(opts, mongo) do
    if opts[:username] != nil and opts[:password] != nil do
      Mongo.Auth.auth(opts, mongo)
    else
      {:ok, mongo}
    end
  end

  @doc false
  defp tcp_recv(mongo) do
    :gen_tcp.recv(mongo.socket, 0, mongo.timeout)
  end

  @doc """
  Retreives a repsonce from the MongoDB server (only for passive mode)
  """
  def response(mongo, req_id) do
    case tcp_recv(mongo) do
      {:ok, <<messageLength::32-signed-little, _::binary>> = message} ->
        case complete(mongo, messageLength, message) |> Mongo.Response.new do
          {:ok, response = %Mongo.Response{requestID: res_id}} when res_id != req_id ->
            Logger.info("#{__MODULE__} receive unknown package from mongo: #{inspect response}")
            response(mongo, req_id)
          res -> res
        end
      {:error, msg} -> %Mongo.Error{msg: msg}
    end
  end

  @doc """
  Sends a message to MongoDB
  """
  def send(mongo, payload, reqid \\ gen_reqid())
  def send(%Mongo.Server{socket: socket, mode: :passive}, payload, reqid) do
     do_send(socket, payload, reqid)
  end
  def send(%Mongo.Server{socket: socket, mode: :active}, payload, reqid) do
    :inet.setopts(socket, active: :once)
    do_send(socket, payload, reqid)
  end
  # sends the message to the socket, returns request {:ok, reqid}
  defp do_send(socket, payload, reqid) do
    case :gen_tcp.send(socket, payload |> message(reqid)) do
      :ok -> {:ok, reqid}
      error -> raise Mongo.Bang, msg: :network_error, acc: error
    end
  end

  @doc false
  # preprares for a one-time async request
  def async(%Mongo.Server{mode: :passive}=mongo) do
    :inet.setopts(mongo.socket, active: :once)
  end

  @doc """
  Sends a command message requesting imediate response
  """
  def cmd_sync(mongo, command) do
    case cmd(mongo, command) do
      {:ok, reqid} ->
        response(mongo, reqid)
      error -> error
    end
  end

  @doc """
  Executes an admin command to the server

    iex> Mongo.connect!  # Returns a exception when connection fails
    iex> case Mongo.connect do
    ...>    {:ok, _mongo } -> :ok
    ...>    error -> error
    ...> end
    :ok

  """
  def cmd(mongo, cmd) do
    send(mongo, Mongo.Request.cmd("admin", cmd))
  end

  @doc """
  Pings the server

    iex> Mongo.connect! |> Mongo.Server.ping
    :ok

  """
  def ping(mongo) do
    case cmd_sync(mongo, %{ping: true}) do
      {:ok, resp} -> Mongo.Response.success(resp)
      error -> error
    end
  end

  @doc """
  Returns true if connection mode is active
  """
  def active?(mongo), do: mongo.mode == :active

  @doc """
  Closes the connection
  """
  def close(mongo) do
    :gen_tcp.close(mongo.socket)
  end

  # makes sure response is complete
  defp complete(_mongo, expected_length, buffer) when byte_size(buffer) == expected_length, do: buffer
  defp complete(mongo, expected_length, buffer) do
    case tcp_recv(mongo) do
      {:ok, mess} -> complete(mongo, expected_length, buffer <> mess)
      {:error, msg} -> %Mongo.Error{msg: msg}
    end
  end

  # Convert TCP options to `:inet.setopts` compatible arguments.
  defp tcp_options(m) do
    args = options(m)

    # default to binary
    args = [:binary | args]

    args
  end
  # default server options
  defp options(mongo) do
    [ active: false,
      nodelay: true,
      send_timeout: mongo.timeout,
      send_timeout_close: true ]
  end

  defp mongo_prefix do
    case :inet.gethostname do
      {:ok, hostname} ->
        <<prefix::16, _::binary>> = :crypto.hash(:md5, (hostname ++ :os.getpid) |> to_string)
        prefix
      _ -> :rand.uniform(65535)
    end
  end
  @doc false
  def prefix(%Mongo.Server{id_prefix: prefix}) do
    for << <<b::4>> <- <<prefix::16>> >>, into: <<>> do
        <<Integer.to_string(b,16)::binary>>
    end |> String.downcase
  end

  @doc """
  Adds options to an existing mongo server connection

  new_opts must be a map with zero or more of the following keys:

  * read: `:awaitdata`, `:nocursortimeout`, `:slaveok`, `:tailablecursor`
  * write concern: `:wc`
  * socket: `:mode`, `:timeout`
  """
  def opts(mongo, new_opts) do
    %Mongo.Server{mongo| opts: Map.merge(mongo.opts, new_opts)}
  end

  @doc """
  Gets mongo connection default options
  """
  def db_opts(mongo) do
    Map.take(mongo.opts, [:awaitdata, :nocursortimeout, :slaveok, :tailablecursor, :wc]) #, :mode, :timeout])
    |> Map.put(:mode, mongo.mode) |> Map.put(:timeout, mongo.timeout)
  end

  use Bitwise, only_operators: true
  @doc """
  Assigns radom ids to a list of documents when `:_id` is missing

      iex> [%{a: 1}] |> Mongo.Server.assign_id |> Enum.at(0) |> Map.keys
      [:_id, :a]

      #a prefix to ids can be set manually like this
      iex> prefix = case [%{a: 1}] |> Mongo.Server.assign_id(256*256-1) |> Enum.at(0) |> Map.get(:_id) do
      ...>   %Bson.ObjectId{oid: <<prefix::16, _::binary>>} -> prefix
      ...>   error -> error
      ...> end
      ...> prefix
      256*256-1

      #by default prefix are set at connection time and remains identical for the entire connection
      iex> mongo = Mongo.connect!
      ...> prefix = case [%{a: 1}] |> Mongo.Server.assign_id(mongo) |> Enum.at(0) |> Map.get(:_id) do
      ...>   %Bson.ObjectId{oid: <<prefix::16, _::binary>>} -> prefix
      ...>   error -> error
      ...> end
      ...> prefix == mongo.id_prefix
      true

  """
  def assign_id(docs, client_prefix \\ gen_client_prefix())
  def assign_id(docs, client_prefix) do
    client_prefix = check_client_prefix(client_prefix)
    Enum.map_reduce(
      docs,
      {client_prefix, gen_trans_prefix(), :rand.uniform(4294967295)},
      fn(doc, id) -> { Map.put(doc, :_id, %Bson.ObjectId{oid: to_oid(id)}), next_id(id) } end)
      |> elem(0)
  end

  # returns a 2 bites prefix integer
  defp check_client_prefix(%Mongo.Server{id_prefix: prefix}) when is_integer(prefix), do: prefix
  defp check_client_prefix(prefix) when is_integer(prefix), do: prefix
  defp check_client_prefix(_), do: gen_client_prefix()
  # generates a 2 bites prefix integer
  defp gen_client_prefix, do: :rand.uniform(65535)
  # returns a 6 bites prefix integer
  defp gen_trans_prefix do
    {gs, s, ms} = :erlang.timestamp()
    (gs * 1000000000000 + s * 1000000 + ms) &&& 281474976710655
  end

  # from a 3 integer tuple to ObjectID
  defp to_oid({client_prefix, trans_prefix, suffix}), do: <<client_prefix::16, trans_prefix::48, suffix::32>>
  # Selects next ID
  defp next_id({client_prefix, trans_prefix, suffix}), do: {client_prefix, trans_prefix, suffix+1}

  # add request ID to a payload message
  defp message(payload, reqid)
  defp message(payload, reqid) do
    [
      <<(:erlang.iolist_size(payload) + 12)::size(32)-little>>, 
      reqid, <<0::32>>, payload
    ]
  end
  # generates a request Id when not provided (makes sure it is a positive integer)
  defp gen_reqid() do
    <<tail::24, _::1, head::7>> = :crypto.strong_rand_bytes(4)
    <<tail::24, 0::1, head::7>>
  end
end
