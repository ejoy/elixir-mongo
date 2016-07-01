Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Crud.Test do
  use ExUnit.Case, async: false

  # In order to run the tests a mongodb server must be listening locally on the default port
  setup do
    mongo = Mongo.connect!
    db = Mongo.db(mongo, "test")
    anycoll1 = Mongo.Db.collection(db, "coll_crud")
    Mongo.Collection.drop anycoll1
    [
        %{a: 0, value: 0},
        %{a: 1, value: 1},
        %{a: 2, value: 1},
        %{a: 3, value: 1},
        %{a: 4, value: 1},
        %{a: 5, value: 3} ] |> Mongo.Collection.insert(anycoll1)
    { :ok, mongo: mongo, db: db, anycoll1: anycoll1 }
  end

  test "find", ctx do
    if true do
      anycoll1 = ctx[:anycoll1]
      # count without retreiving
      assert anycoll1 |> Mongo.Collection.find |> Enum.count == 6
      # retreive all docs then count
      assert anycoll1 |> Mongo.Collection.find |> Enum.to_list |> Enum.count == 6
      # retreive all but one doc then count
      assert anycoll1 |> Mongo.Collection.find |> Mongo.Find.skip(1) |> Enum.to_list |> Enum.count == 5
      
      # retreive one doc 
      assert anycoll1 |> Mongo.Collection.find |> Mongo.Find.limit(1) |> Enum.to_list |> Enum.count == 1
    end
  end

  test "find where", ctx do
    if true do
      assert ctx[:anycoll1] |> Mongo.Collection.find("obj.value == 0") |> Enum.count == 1
      assert ctx[:anycoll1] |> Mongo.Collection.find("obj.value == 0") |> Enum.to_list |> Enum.count == 1
    end
  end

  test "insert", ctx do
    anycoll1 = ctx[:anycoll1]
    if true do
      assert %{a: 23} |> Mongo.Collection.insert_one!(anycoll1) == %{a: 23}
      assert [%{a: 23}, %{a: 24, b: 1}] |> Mongo.Collection.insert!(anycoll1) |> is_list
    end
    if true do
      assert %{'_id': 2, a: 456} |> Mongo.Collection.insert_one!(anycoll1) |> is_map
      assert {:ok, _} = ctx[:db] |> Mongo.Db.getLastError 
    end
  end

  test "update", ctx do
    if true do
      ctx[:anycoll1] |> Mongo.Collection.update(%{a: 456}, %{a: 123, b: 789})
      assert {:ok, _} = ctx[:db] |> Mongo.Db.getLastError
    end
  end

  test "delete", ctx do
    if true do
      ctx[:anycoll1] |> Mongo.Collection.delete(%{b: 789})
      assert {:ok, _} = ctx[:db] |> Mongo.Db.getLastError
    end
  end

  test "objid", ctx do
    if true do
      anycoll1 = ctx[:anycoll1]
      assert [%{a: -23}, %{a: -24, b: 1}] |> Mongo.Server.assign_id(ctx[:mongo]) |> Mongo.Collection.insert!(anycoll1) |> is_list
    end
  end

  test "bang find", ctx do
    if true do
      assert %Mongo.Error{} = ctx[:anycoll1] |> Mongo.Collection.find(%{value: %{'$in': 0}}) |> Mongo.Find.exec
    end
  end

  test "insert error", ctx do
    anycoll1 = ctx[:anycoll1]
    if true do
      %{_id: 1, a: 31} |> Mongo.Collection.insert_one!(anycoll1)
      %{_id: 1, a: 32} |> Mongo.Collection.insert_one!(anycoll1)
      assert {:error, _} = ctx[:db] |> Mongo.Db.getLastError
    end
  end

  test "atom collection name", ctx do
      assert ctx.db |> Mongo.Db.collection(:coll_crud) |> Mongo.Collection.find("obj.value == 0") |> Enum.count == 1
  end
end
