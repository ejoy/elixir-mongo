defmodule Mongo.Find do
  @moduledoc """
  Find operation on MongoDB
  """
  use Mongo.Helpers

  defstruct [
    mongo: nil,
    collection: nil,
    selector: %{},
    projector: %{},
    batchSize: 0,
    skip: 0,
    opts: %{},
    mods: %{}]

  @doc """
  Creates a new find operation.

  Not to be used directly, prefer `Mongo.Collection.find/3`
  """
  def new(collection, jsString, projector) when is_binary(jsString), do: new(collection, %{'$where': jsString}, projector)
  def new(collection, selector, projector) do
    %__MODULE__{collection: collection, selector: selector, projector: projector, opts: collection |> Mongo.Collection.read_opts}
  end

  @doc """
  Sets where MongoDB begins returning results

  Must be run before executing the query

      iex> Mongo.connect.%@m{"test"}.collection("anycoll").find.skip(1).toArray |> Enum.count
      5
      iex> Mongo.connect.%@m{"test"}.collection("anycoll").find.skip(2).toArray |> Enum.count
      4

  """
  def skip(find, skip), do: %__MODULE__{find| skip: skip}

  @doc """
  Limits the number of documents to the query.

  Must be run before executing the query
  """
  def limit(find, limit) when is_integer(limit), do: %__MODULE__{find| batchSize: -limit}

  @doc """
  Executes the query and returns a `%Mongo.Cursor{}`
  """
  def exec(find) do
    Mongo.Cursor.exec(find.collection, Mongo.Request.query(find), find.batchSize)
  end

  @doc """
  Runs the explain operator that provides information on the query plan
  """
  def explain(find) do
    find |> addSpecial(:'$explain', 1) |> Enum.at(0)
  end

  @doc """
  Add hint opperator that forces the query optimizer to use a specific index to fulfill the query
  """
  def hint(f, hints)
  def hint(f, indexName) when is_atom(indexName), do: f |> addSpecial(:'$hint', indexName)
  def hint(f, hints) when is_map(hints), do: f |> addSpecial(:'$hint', hints)

  def sort(f, opts) when is_map(opts) or is_list(opts), do: f |> addSpecial(:"$orderby", opts)
  @doc """
  Sets query options

  Defaults option set is equivalent of calling:

      Find.opts(
        awaitdata: false
        nocursortimeout: false
        slaveok: true
        tailablecursor: false)
  """
  def opts(find, options), do: %__MODULE__{find| opts: options}

  def addSpecial(find, k, v) do
    %__MODULE__{find| mods: Map.put(find.mods, k, v)}
  end

  defimpl Enumerable, for: Mongo.Find do

    @doc """
    Executes the query and reduce retrieved documents into a value
    """
    def reduce(find, acc, reducer) do
      case Mongo.Find.exec(find) do
        %Mongo.Cursor{}=cursor ->
          case Enumerable.reduce(cursor, {:cont, acc},
            fn(docs, acc)->
              case Enumerable.reduce(docs, acc, reducer) do
                {:done, acc} -> {:cont, {:cont, acc}}
                {:halted, acc} -> {:halt, acc}
                {:suspended, acc} -> {:suspend, acc}
                error -> {:halt, error}
              end
            end) do
              {:done, {:cont, acc}} -> {:done, acc}
              other -> other
          end
        error ->
          case error do
            {:error, msg} -> raise Mongo.Bang, msg: msg, acc: acc
            %Mongo.Error{msg: msg, acc: acc} -> raise Mongo.Bang, msg: msg, acc: acc
          end
      end
    end

    @doc """
    Counts number of documents to be retreived
    """
    def count(find) do
      case Mongo.Collection.count(find.collection, find.selector, Map.take(find, [:skip, :limit])) do
        %Mongo.Error{} -> -1
        n -> n
      end
    end

    @doc """
    Not implemented
    """
    def member?(_, _), do: :not_implemented

    @doc false
    #Not implemented
    def slice(_), do: {:error, __MODULE__}
  end
end
