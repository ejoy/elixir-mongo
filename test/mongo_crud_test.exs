Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Crud.Test do
  use ExUnit.Case, async: false

  # In order to run the tests a mongodb server must be listening locally on the default port
  setup_all do
    mongo = Mongo.connect!
    db = Mongo.db(mongo, "test")
    anycoll = Mongo.Db.collection(db, "coll_crud")
    modify_coll = Mongo.Db.collection(db, "coll_crud_for_modify")
    Mongo.Collection.delete(anycoll, %{})
    Mongo.Collection.delete(modify_coll, %{})
    Mongo.Collection.drop anycoll
    Mongo.Collection.drop modify_coll
    [
        %{a: 0, value: 0},
        %{a: 1, value: 1},
        %{a: 2, value: 1},
        %{a: 3, value: 1},
        %{a: 4, value: 1},
        %{a: 5, value: 3} ] |> Mongo.Collection.insert(anycoll)
    { :ok, mongo: mongo, db: db, anycoll: anycoll, modify_coll: modify_coll }
  end

  test "find", ctx do
    if true do
      require Logger
      anycoll = ctx[:anycoll]
      # count without retreiving
      assert anycoll |> Mongo.Collection.find |> Enum.count == 6
      # retreive all docs then count
      assert anycoll |> Mongo.Collection.find |> Enum.to_list |> Enum.count == 6
      # retreive all but one doc then count
      assert anycoll |> Mongo.Collection.find |> Mongo.Find.skip(1) |> Enum.to_list |> Enum.count == 5
      
      # retreive one doc 
      assert anycoll |> Mongo.Collection.find |> Mongo.Find.limit(1) |> Enum.to_list |> Enum.count == 1
    end
  end

  test "find where", ctx do
    if true do
      assert ctx[:anycoll] |> Mongo.Collection.find("obj.value == 0") |> Enum.count == 1
      assert ctx[:anycoll] |> Mongo.Collection.find("obj.value == 0") |> Enum.to_list |> Enum.count == 1
    end
  end

  test "insert", ctx do
    anycoll = ctx[:modify_coll]
    if true do
      assert %{a: 23} |> Mongo.Collection.insert_one!(anycoll) == %{a: 23}
      assert [%{a: 23}, %{a: 24, b: 1}] |> Mongo.Collection.insert!(anycoll) |> is_list
    end
    if true do
      assert %{_id: 2, a: 456} |> Mongo.Collection.insert_one!(anycoll) |> is_map
      assert {:ok, _} = ctx[:db] |> Mongo.Db.getLastError 
    end
  end

  test "update", ctx do
    if true do
      ctx[:modify_coll] |> Mongo.Collection.update(%{a: 456}, %{a: 123, b: 789})
      assert {:ok, _} = ctx[:db] |> Mongo.Db.getLastError
    end
  end

  test "delete", ctx do
    if true do
      ctx[:modify_coll] |> Mongo.Collection.delete(%{b: 789})
      assert {:ok, _} = ctx[:db] |> Mongo.Db.getLastError
    end
  end

  test "objid", ctx do
    if true do
      anycoll = ctx[:modify_coll]
      assert [%{a: -23}, %{a: -24, b: 1}] |> Mongo.Server.assign_id(ctx[:mongo]) |> Mongo.Collection.insert!(anycoll) |> is_list
    end
  end

  test "bang find", ctx do
    if true do
      assert %Mongo.Error{} = ctx[:anycoll] |> Mongo.Collection.find(%{value: %{'$in': 0}}) |> Mongo.Find.exec
    end
  end

  test "insert error", ctx do
    anycoll = ctx[:modify_coll]
    if true do
      %{_id: 1, a: 31} |> Mongo.Collection.insert_one!(anycoll)
      %{_id: 1, a: 32} |> Mongo.Collection.insert_one!(anycoll)
      assert {:error, _} = ctx[:db] |> Mongo.Db.getLastError
    end
  end

  test "atom collection name", ctx do
      assert ctx.db |> Mongo.Db.collection(:coll_crud) |> Mongo.Collection.find("obj.value == 0") |> Enum.count == 1
  end
end
