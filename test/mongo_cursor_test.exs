Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Cursor.Test do
  use ExUnit.Case, async: false

  # In order to run the tests a mongodb server must be listening locally on the default port
  setup do
    mongo = Mongo.connect!
    db = Mongo.db(mongo, "test")
    anycoll2 = Mongo.Db.collection(db, "coll_cursor")
    Mongo.Collection.drop anycoll2
    [
        %{a: 0, value: 0},
        %{a: 1, value: 1},
        %{a: 2, value: 1},
        %{a: 3, value: 1},
        %{a: 4, value: 1},
        %{a: 5, value: 3} ] |> Mongo.Collection.insert(anycoll2)
    { :ok, mongo: mongo, db: db, anycoll2: anycoll2 }
  end

  test "batchSize", ctx do
    assert ctx[:anycoll2] |> Mongo.Collection.find |> Map.put(:batchSize, 2) |> Enum.to_list |> Enum.count == 6
  end

  test "batchArray", ctx do
    assert ctx[:anycoll2] |> Mongo.Collection.find |> Map.put(:batchSize, 3) |> Mongo.Find.exec |> Enum.to_list |> Enum.count == 3
  end

  test "explain", ctx do
    assert ctx[:anycoll2] |> Mongo.Collection.find |> Mongo.Find.explain 
  end

  test "find hint", ctx do
    ctx[:anycoll2] |> Mongo.Collection.createIndex("tst_value", %{value: true})
    explain = ctx[:anycoll2] |> Mongo.Collection.find |> Mongo.Find.hint(%{value: true}) |> Mongo.Find.explain 
    assert "tst_value" == explain["queryPlanner"]["winningPlan"]["inputStage"][:indexName]
  end

end
