defmodule Mongo.Db do
  @moduledoc """
    Module holding operations that can be performed on MongoDB databases
  """

  defstruct [
    name: nil,
    mongo: nil,
    auth: nil,
    opts: %{} ]

  use Mongo.Helpers

  alias Mongo.Server

  @doc """
  Creates `%Mongo.Db{}` with default options
  """
  def new(mongo, name), do: %Mongo.Db{mongo: mongo, name: name, opts: Server.db_opts(mongo)}

  @doc """
  Returns a collection struct
  """
  defdelegate collection(db, name), to: Mongo.Collection, as: :new

  @doc """
  Executes a db command requesting imediate response
  """
  def cmd_sync(db, command, cmd_args \\ %{}) do
    case cmd(db, command, cmd_args) do
      {:ok, reqid} -> Server.response(db.mongo, reqid)
      error -> error
    end
  end

  @doc """
  Executes a db command

  Before using this check `Mongo.Collection`, `Mongo.Db` or `Mongo.Server`
  for commands already implemented by these modules
  """
  def cmd(db, cmd, cmd_args \\ %{}) do
    Server.send(db.mongo, Mongo.Request.cmd(db.name, cmd, cmd_args))
  end
  defbang cmd(db, command)

  @doc """
  Returns the error status of the preceding operation.
  """
  def getLastError(db) do
    case cmd_sync(db, %{getlasterror: true}) do
      {:ok, resp} -> resp |> Mongo.Response.error
      error -> error
    end
  end
  defbang getLastError(db)

  @doc """
    drop the database 
  """
  def dropDatabase(db) do
    case cmd_sync(db, %{dropDatabase: 1}) do
      {:ok, resp} -> resp |> Mongo.Response.error
      error -> error
    end
  end
  defbang dropDatabase(db)

  @doc """
  Returns the previous error status of the preceding operation(s).
  """
  def getPrevError(db) do
    case cmd_sync(db, %{getPrevError: true}) do
      {:ok, resp} -> resp |> Mongo.Response.error
      error -> error
    end
  end
  defbang getPrevError(db)

  @doc """
  Resets error
  """
  def resetError(db) do
    case cmd(db, %{resetError: true}) do
      {:ok, _} -> :ok
      error -> error
    end
  end
  defbang resetError(db)

  @doc """
  Kill a cursor of the db
  """
  def kill_cursor(db, cursorID) do
    Mongo.Request.kill_cursor(cursorID) |> Server.send(db.mongo)
  end

  @doc """
  Adds options to the database overwriting mongo server connection options

  new_opts must be a map with zero or more of the following keys:

  * read: `:awaitdata`, `:nocursortimeout`, `:slaveok`, `:tailablecursor`
  * write concern: `:wc`
  * socket: `:mode`, `:timeout`
  """
  def opts(db, new_opts) do
    %Mongo.Db{db| opts: Map.merge(db.opts, new_opts)}
  end

  @doc """
  Gets collection default options
  """
  def coll_opts(db) do
    Map.take(db.opts, [:awaitdata, :nocursortimeout, :slaveok, :tailablecursor, :wc])
  end

end
