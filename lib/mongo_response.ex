defmodule Mongo.Response do
  @moduledoc """
  Receives, decode and parse MongoDB response from the server
  """
  defstruct [
    cursorID: nil,
    startingFrom: nil,
    nbdoc: nil,
    docs: nil,
    requestID: nil]

  @msg         <<1, 0, 0, 0>>    #    1  Opcode OP_REPLY : Reply to a client request

  @doc """
  Parses a response message

  If the message is partial, this method makes shure the response is complete by fetching additional messages
  """
  def new(
    <<_::32,                                           # total message size, including this
      _::32,                                           # identifier for this message
      requestID::size(32)-signed-little,               # requestID from the original request
      @msg::binary,                                    # Opcode OP_REPLY
      _::6, queryFailure::1, cursorNotFound::1, _::24, # bit vector representing response flags
      cursorID::size(64)-signed-little,                # cursor id if client needs to do get more's
      startingFrom::size(32)-signed-little,            # where in the cursor this reply is starting
      numberReturned::size(32)-signed-little,          # number of documents in the reply
      buffer::bitstring>>) do                          # buffer of Bson documents
    cond do
      cursorNotFound > 0 ->
        %Mongo.Error{msg: :"cursor not found"}
      queryFailure > 0 ->
        if numberReturned > 0 do
          case bson_decode_all(buffer) do
            %Mongo.Error{} = _error ->
              %Mongo.Error{msg: :"query failure"}
            docs ->
              %Mongo.Error{ msg: :"query failure", acc: docs}
          end
        else
          %Mongo.Error{msg: :"query failure"}
        end
      true -> 
        case bson_decode_all(buffer) do
            %Mongo.Error{} = error -> error
            docs when length(docs) == numberReturned -> {:ok, %Mongo.Response{
                cursorID: cursorID,
                startingFrom: startingFrom,
                nbdoc: numberReturned,
                docs: docs,
                requestID: requestID}}
            _ -> %Mongo.Error{msg: :"query failure"}
        end
    end
  end
  
  def new(%Mongo.Error{msg: msg}) do
    %Mongo.Error{msg: msg}
  end

  @doc """
  Decodes a command response

  Returns `{:ok, doc}` or transfers the error message
  """
  def cmd(%Mongo.Response{nbdoc: 1, docs: [doc]}) do
    case doc do
      %{ok: ok} = doc when ok > 0 -> {:ok, doc}
      errdoc -> %Mongo.Error{msg: :"cmd error", acc: errdoc}
    end 
  end

  @doc """
  Decodes a count respsonse

  Returns `{:ok, n}` or transfers the error message
  """
  def count(response) do
    case cmd(response) do
      {:ok, doc} -> {:ok, doc[:n]}
      error -> error
    end
  end

  @doc """
  Decodes a success respsonse

  Returns `:ok` or transfers the error message
  """
  def success(response) do
    case cmd(response) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc """
  Decodes a distinct respsonse

  Returns `{:ok, values}` or transfers the error message
  """
  def distinct(response) do
    case cmd(response) do
      {:ok, doc} -> {:ok, doc[:values]}
      error -> error
    end
  end

  @doc """
  Decodes a map-reduce respsonse

  Returns `{:ok, results}` (inline) or `:ok` or transfers the error message
  """
  def mr(response) do
    case cmd(response) do
      {:ok, doc} ->
        case doc[:results] do
          nil -> :ok
          results -> {:ok, results}
        end
      error -> error
    end
  end

  @doc """
  Decodes a group respsonse

  Returns `{:ok, retval}` or transfers the error message
  """
  def group(response) do
    case cmd(response) do
      {:ok, doc} -> {:ok, doc[:retval]}
      error -> error
    end
  end

  @doc """
  Decodes an aggregate respsonse

  Returns `{:ok, result}` or transfers the error message
  """
  def aggregate(response) do
    case cmd(response) do
      {:ok, doc} ->
        doc.cursor.firstBatch #TODO: 这个库.. 包括 getIndexes, 就是这么粗糙的只拿第一批, 等我们壮大到 batch 不够用, 再加上...
      error -> error
    end
  end
  @doc """
  Decodes a getnonce respsonse

  Returns `{:ok, nonce}` or transfers the error message
  """
  def getnonce(response) do
    case cmd(response) do
      {:ok, doc} -> doc[:nonce]
      error -> error
    end
  end
  @doc """
  Decodes an error respsonse

  Returns `{:ok, nonce}` or transfers the error message
  """
  def error(response) do
    case cmd(response) do
      {:ok, doc} ->
        case doc[:err] do
          nil -> {:ok, doc}
          _ -> {:error, doc}
        end
      error -> error
    end
  end

  @doc """
  Helper fuction to decode bson buffer
  """
  def bson_decode_all(<<>>), do: []
  def bson_decode_all(buffer) do
    try do
      bson_decode_all(buffer, [])
    catch
      error -> 
        %Mongo.Error{msg: :bson_decode_error, acc: [error]}
    end
  end
  
  defp bson_decode_all(buffer, acc) do
    case Bson.decode(buffer, [:return_atom, :return_trailer]) do
      {:has_trailer, doc, rest} -> bson_decode_all(rest, [doc|acc])
      doc -> [doc | acc] |> :lists.reverse
    end
  end
end
