defmodule Mongo.Db.Test do
  use ExUnit.Case, async: false

  test "drop database" do
    mongo = Mongo.connect!
    db = Mongo.db(mongo, "test_drop")
    anycoll = Mongo.Db.collection(db, "coll_db")
    Mongo.Collection.drop anycoll
    [
        %{a: 0, value: 0},
        %{a: 1, value: 1},
        %{a: 2, value: 1},
        %{a: 3, value: 1},
        %{a: 4, value: 1},
        %{a: 5, value: 3} ] |> Mongo.Collection.insert(anycoll)

    assert %{dropped: "test_drop", ok: 1.0} = Mongo.Db.dropDatabase!(db)

    assert anycoll |> Mongo.Collection.find |> Enum.count == 0
  end
end

